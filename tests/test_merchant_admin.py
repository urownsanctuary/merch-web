import io
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ADMIN_LOGIN", "admin")
os.environ.setdefault("ADMIN_PASSWORD", "strong-password")
os.environ.setdefault("ENVIRONMENT", "test")

from fastapi.testclient import TestClient
from openpyxl import Workbook
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import (
    admin_add_merchant,
    app,
    get_admin_cookie_value,
    get_admin_csrf_token,
    get_db,
)
from app.merchant_admin import (
    MerchantInputError,
    create_merchant,
    ensure_merchant_admin_schema,
    get_merchant_for_admin,
    list_merchants,
    set_merchant_active,
    update_merchant,
)
from app.services import fio_norm, hash_last4, import_merchants_xlsx, login_user


def merchant_workbook(rows):
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["ФИО", "Последние 4"])
    for row in rows:
        sheet.append(row)
    output = io.BytesIO()
    workbook.save(output)
    output.seek(0)
    return output


class MerchantAdminTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.factory = sessionmaker(bind=self.engine)
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                """
                CREATE TABLE merchants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fio TEXT NOT NULL,
                    fio_norm TEXT NOT NULL,
                    pass_hash TEXT NOT NULL,
                    telegram_id TEXT,
                    tu TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.exec_driver_sql(
                """
                CREATE TABLE visits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    merchant_id INTEGER NOT NULL,
                    point_code TEXT,
                    visit_date DATE,
                    slot TEXT
                )
                """
            )
        with self.factory() as db:
            ensure_merchant_admin_schema(db)

        def override_db():
            db = self.factory()
            try:
                yield db
            finally:
                db.close()

        app.dependency_overrides[get_db] = override_db
        self.client = TestClient(app)
        self.admin_cookie = get_admin_cookie_value()
        self.csrf = get_admin_csrf_token(self.admin_cookie)
        self.client.cookies.set("admin_auth", self.admin_cookie)

    def tearDown(self):
        self.client.close()
        app.dependency_overrides.clear()
        self.engine.dispose()

    def create(self, fio="Иванов Иван", last4="1234", tu="ТУ-1"):
        with self.factory() as db:
            merchant = create_merchant(
                db,
                fio,
                last4,
                tu,
                actor="admin",
                fio_normalizer=fio_norm,
                last4_hasher=hash_last4,
            )
            db.commit()
            return merchant

    def post_create(self, **overrides):
        data = {
            "fio": "  Ёлкин   Семён  ",
            "last4": "4321",
            "tu": "ТУ-1",
            "csrf_token": self.csrf,
        }
        data.update(overrides)
        return self.client.post(
            "/admin-add-merchant", data=data, follow_redirects=False
        )

    def test_schema_is_additive_and_idempotent(self):
        with self.factory() as db:
            ensure_merchant_admin_schema(db)
            ensure_merchant_admin_schema(db)
            columns = {
                row[1]
                for row in db.execute(text("PRAGMA table_info(merchants)")).all()
            }
            self.assertTrue(
                {"last4", "is_active", "created_at", "updated_at"}.issubset(columns)
            )
            self.assertEqual(
                db.execute(
                    text(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='merchant_audit_log'"
                    )
                ).scalar(),
                1,
            )
            audit_columns = {
                row[1]
                for row in db.execute(
                    text("PRAGMA table_info(merchant_audit_log)")
                ).all()
            }
            self.assertTrue(
                {
                    "id",
                    "merchant_id",
                    "action",
                    "actor",
                    "changed_fields",
                    "created_at",
                }.issubset(audit_columns)
            )
            indexes = {
                row[1]
                for row in db.execute(text("PRAGMA index_list(merchants)")).all()
            }
            self.assertIn("uq_merchants_fio_norm_last4", indexes)

    def test_schema_adds_created_at_to_minimal_legacy_table(self):
        legacy_engine = create_engine("sqlite+pysqlite:///:memory:")
        legacy_factory = sessionmaker(bind=legacy_engine)
        try:
            with legacy_engine.begin() as connection:
                connection.exec_driver_sql(
                    """
                    CREATE TABLE merchants (
                        id INTEGER PRIMARY KEY,
                        fio TEXT NOT NULL,
                        fio_norm TEXT NOT NULL,
                        pass_hash TEXT NOT NULL,
                        tu TEXT
                    )
                    """
                )
                connection.exec_driver_sql(
                    """
                    INSERT INTO merchants (id, fio, fio_norm, pass_hash, tu)
                    VALUES (1, 'Legacy User', 'legacy user', 'hash', 'TU-1')
                    """
                )
            with legacy_factory() as db:
                ensure_merchant_admin_schema(db)
                row = db.execute(
                    text(
                        """
                        SELECT last4, is_active, created_at, updated_at
                        FROM merchants WHERE id=1
                        """
                    )
                ).mappings().one()
                self.assertIsNone(row["last4"])
                self.assertTrue(row["is_active"])
                self.assertIsNotNone(row["created_at"])
                self.assertIsNotNone(row["updated_at"])
        finally:
            legacy_engine.dispose()

    def test_postgresql_migration_matches_production_legacy_schema(self):
        class FakeDialect:
            name = "postgresql"

        class FakeBind:
            dialect = FakeDialect()

        class FakeDb:
            def __init__(self):
                self.statements = []
                self.committed = False

            def get_bind(self):
                return FakeBind()

            def execute(self, statement, _params=None):
                self.statements.append(str(statement))
                return None

            def commit(self):
                self.committed = True

        fake_db = FakeDb()
        legacy_columns = {
            "id",
            "fio",
            "fio_norm",
            "pass_hash",
            "telegram_id",
            "tu",
            "created_at",
        }
        with patch(
            "app.merchant_admin._merchant_columns", return_value=legacy_columns
        ):
            ensure_merchant_admin_schema(fake_db)

        ddl = "\n".join(fake_db.statements)
        self.assertIn("ADD COLUMN IF NOT EXISTS last4 TEXT", ddl)
        self.assertIn("ADD COLUMN IF NOT EXISTS is_active BOOLEAN", ddl)
        self.assertIn("ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP", ddl)
        self.assertIn("CREATE TABLE IF NOT EXISTS merchant_audit_log", ddl)
        self.assertIn(
            "DROP CONSTRAINT IF EXISTS merchants_fio_norm_uq", ddl
        )
        self.assertIn("DROP CONSTRAINT IF EXISTS merchants_fio_key", ddl)
        self.assertIn("ON merchants (fio_norm, last4)", ddl)
        self.assertTrue(fake_db.committed)

    def test_successful_add_normalizes_name_and_allows_login(self):
        response = self.post_create()
        self.assertEqual(response.status_code, 303)
        with self.factory() as db:
            row = db.execute(
                text("SELECT * FROM merchants WHERE last4='4321'")
            ).mappings().one()
            self.assertEqual(row["fio"], "Ёлкин Семён")
            self.assertEqual(row["fio_norm"], fio_norm("елкин семен"))
            self.assertTrue(row["is_active"])
            self.assertIsNotNone(login_user(db, "  ЕЛКИН СЕМЕН ", "4321"))

    def test_add_route_does_not_depend_on_missing_main_re_import(self):
        self.assertNotIn("re", admin_add_merchant.__globals__)
        response = self.post_create()
        self.assertEqual(response.status_code, 303)
        self.assertNotEqual(response.status_code, 500)

    def test_empty_name_and_invalid_phone_are_inline_and_preserve_values(self):
        response = self.post_create(fio=" ", last4="12 3", tu="ТУ-Север")
        self.assertEqual(response.status_code, 422)
        self.assertIn("Укажите ФИО", response.text)
        self.assertIn("ровно 4 цифры", response.text)
        self.assertIn('value="ТУ-Север"', response.text)
        with self.factory() as db:
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM merchants")).scalar(), 0)

    def test_database_error_rolls_back_and_returns_form_without_500(self):
        def insert_then_fail(db, *_args, **_kwargs):
            sensitive_error_detail = "synthetic database failure with secret-token"
            db.execute(
                text(
                    """
                    INSERT INTO merchants
                        (fio, fio_norm, pass_hash, last4, tu, is_active)
                    VALUES
                        ('Partial User', 'partial user', 'redacted', '4321', 'TU-1', TRUE)
                    """
                )
            )
            raise RuntimeError(sensitive_error_detail)

        with patch("app.main.create_merchant", side_effect=insert_then_fail):
            with self.assertLogs("app.main", level="ERROR") as captured:
                response = self.post_create(
                    fio="Preserved User", last4="4321", tu="TU-Preserved"
                )

        self.assertEqual(response.status_code, 503)
        self.assertNotEqual(response.status_code, 500)
        self.assertIn('value="Preserved User"', response.text)
        self.assertIn('value="4321"', response.text)
        self.assertIn('value="TU-Preserved"', response.text)
        self.assertIn("Данные не сохранены", response.text)
        serialized_logs = "\n".join(captured.output)
        self.assertIn("RuntimeError", serialized_logs)
        self.assertNotIn("secret-token", serialized_logs)
        self.assertNotIn("Preserved User", serialized_logs)
        with self.factory() as db:
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM merchants")).scalar(), 0)

    def test_exact_duplicate_is_blocked_and_links_existing(self):
        existing = self.create("Ёлкин Семён", "1234")
        response = self.post_create(fio=" елкин  семен ", last4="1234")
        self.assertEqual(response.status_code, 422)
        self.assertIn("уже существует", response.text)
        self.assertIn(
            f"/admin-merchants/{existing['id']}/edit", response.text
        )
        with self.factory() as db:
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM merchants")).scalar(), 1)

    def test_same_name_different_phone_requires_conscious_confirmation(self):
        self.create("Иванов Иван", "1234")
        warning = self.post_create(fio="ИВАНОВ ИВАН", last4="5678")
        self.assertEqual(warning.status_code, 422)
        self.assertIn("Возможное совпадение ФИО", warning.text)
        confirmed = self.post_create(
            fio="ИВАНОВ ИВАН", last4="5678", confirm_same_name="1"
        )
        self.assertEqual(confirmed.status_code, 303)
        with self.factory() as db:
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM merchants")).scalar(), 2)
            self.assertIsNotNone(login_user(db, "Иванов Иван", "1234"))
            self.assertIsNotNone(login_user(db, "Иванов Иван", "5678"))

    def test_edit_keeps_id_and_historical_visit(self):
        merchant = self.create()
        with self.factory() as db:
            db.execute(
                text(
                    """
                    INSERT INTO visits (merchant_id, point_code, visit_date, slot)
                    VALUES (:merchant_id, 'P1', '2026-07-01', 'MORNING')
                    """
                ),
                {"merchant_id": merchant["id"]},
            )
            db.commit()
            update_merchant(
                db,
                merchant["id"],
                "Петров Пётр",
                "9876",
                "ТУ-2",
                "active",
                actor="admin",
                fio_normalizer=fio_norm,
                last4_hasher=hash_last4,
            )
            db.commit()
            updated = get_merchant_for_admin(db, merchant["id"])
            self.assertEqual(updated["fio"], "Петров Пётр")
            self.assertEqual(updated["last4"], "9876")
            self.assertEqual(updated["tu"], "ТУ-2")
            self.assertEqual(
                db.execute(
                    text("SELECT merchant_id FROM visits")
                ).scalar(),
                merchant["id"],
            )
            self.assertIsNotNone(login_user(db, "ПЕТРОВ ПЕТР", "9876"))
            self.assertIsNone(login_user(db, "Иванов Иван", "1234"))

    def test_deactivation_blocks_login_preserves_history_and_restore_works(self):
        merchant = self.create()
        with self.factory() as db:
            db.execute(
                text(
                    "INSERT INTO visits (merchant_id, point_code, visit_date, slot) VALUES (:id,'P1','2026-07-01','MORNING')"
                ),
                {"id": merchant["id"]},
            )
            set_merchant_active(db, merchant["id"], False, actor="admin")
            db.commit()
            self.assertIsNone(login_user(db, "Иванов Иван", "1234"))
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM visits")).scalar(), 1)
            set_merchant_active(db, merchant["id"], True, actor="admin")
            db.commit()
            self.assertIsNotNone(login_user(db, "Иванов Иван", "1234"))

    def test_status_route_requires_csrf(self):
        merchant = self.create()
        response = self.client.post(
            f"/admin-merchants/{merchant['id']}/status",
            data={"active": "0", "csrf_token": ""},
        )
        self.assertEqual(response.status_code, 403)

    def test_unauthorized_access_redirects(self):
        self.client.cookies.clear()
        response = self.client.get("/admin-merchants", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/admin-login")

    def test_xss_values_are_escaped_in_list(self):
        self.create("Иванов <script>alert(1)</script>", "1234", '<img src=x onerror="x">')
        response = self.client.get("/admin-merchants")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("<script>alert(1)</script>", response.text)
        self.assertNotIn('<img src=x onerror="x">', response.text)
        self.assertIn("&lt;script&gt;", response.text)
        self.assertIn("&lt;img", response.text)

    def test_search_filters_and_empty_message(self):
        self.create("Иванов Иван", "1234", "ТУ-1")
        self.create("Петров Пётр", "5678", "ТУ-2")
        with self.factory() as db:
            rows = list_merchants(
                db,
                fio_query="петров",
                last4_query="78",
                tu="ТУ-2",
                status="active",
                sort="fio_desc",
            )
            self.assertEqual([row["fio"] for row in rows], ["Петров Пётр"])
        response = self.client.get("/admin-merchants?fio_query=Несуществующий")
        self.assertIn("Сотрудники по выбранным условиям не найдены", response.text)

    def test_audit_records_field_names_but_not_credentials(self):
        merchant = self.create(last4="1234")
        with self.factory() as db:
            update_merchant(
                db,
                merchant["id"],
                "Иванов Иван",
                "9876",
                "ТУ-2",
                "active",
                actor="admin",
                fio_normalizer=fio_norm,
                last4_hasher=hash_last4,
            )
            db.commit()
            serialized = "\n".join(
                str(value)
                for row in db.execute(
                    text(
                        "SELECT action, actor, changed_fields FROM merchant_audit_log ORDER BY created_at"
                    )
                ).all()
                for value in row
            )
            self.assertIn("last4", serialized)
            self.assertNotIn("1234", serialized)
            self.assertNotIn("9876", serialized)
            self.assertNotIn(hash_last4("9876"), serialized)

    def test_import_uses_same_duplicate_rules_and_rolls_back(self):
        self.create("Иванов Иван", "1234")
        with self.factory() as db:
            with self.assertRaises(MerchantInputError):
                import_merchants_xlsx(
                    db,
                    merchant_workbook(
                        [["Петров Пётр", "5678"], ["ИВАНОВ ИВАН", "1234"]]
                    ),
                    "ТУ-1",
                )
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM merchants")).scalar(), 1)

    def test_import_rejects_phone_symbols_before_any_write(self):
        with self.factory() as db:
            with self.assertRaises(ValueError):
                import_merchants_xlsx(
                    db,
                    merchant_workbook(
                        [["Иванов Иван", "1234"], ["Петров Пётр", "56-78"]]
                    ),
                    "ТУ-1",
                )
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM merchants")).scalar(), 0)


if __name__ == "__main__":
    unittest.main()
