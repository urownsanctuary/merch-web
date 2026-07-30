import io
import os
import unittest

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
            self.assertTrue({"last4", "is_active", "updated_at"}.issubset(columns))
            self.assertEqual(
                db.execute(
                    text(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='merchant_audit_log'"
                    )
                ).scalar(),
                1,
            )

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

    def test_empty_name_and_invalid_phone_are_inline_and_preserve_values(self):
        response = self.post_create(fio=" ", last4="12 3", tu="ТУ-Север")
        self.assertEqual(response.status_code, 422)
        self.assertIn("Укажите ФИО", response.text)
        self.assertIn("ровно 4 цифры", response.text)
        self.assertIn('value="ТУ-Север"', response.text)
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
