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
    count_merchant_owned_rows,
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

    def test_admin_data_shows_protected_delete_all_button_and_counts(self):
        with patch("app.main.get_active_period", return_value={"year": 2026, "month": 7}), patch(
            "app.main.get_all_tu_values", return_value=["ТУ-1"]
        ), patch("app.main.get_special_inventory_days", return_value=[]), patch(
            "app.main.get_calendar_status", return_value=[]
        ):
            response = self.client.get("/admin-data")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Удалить всех мерчендайзеров и их данные", response.text)
        self.assertIn('action="/admin-delete-all-merchants"', response.text)
        self.assertIn("УДАЛИТЬ ВСЕХ МЕРЧЕНДАЙЗЕРОВ", response.text)
        self.assertIn(
            "Будут безвозвратно удалены все мерчендайзеры и все связанные с ними "
            "сверки, выходы, примечания, возмещения и чеки. Продолжить?",
            response.text,
        )

    def test_delete_preview_accepts_legacy_point_submissions(self):
        merchant = self.create()
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE point_submissions "
                "(id INTEGER PRIMARY KEY, merchant_id INTEGER NOT NULL, receipt_path TEXT)"
            )
            connection.execute(
                text(
                    "INSERT INTO point_submissions "
                    "(id, merchant_id, receipt_path) "
                    "VALUES (1, :merchant_id, 'receipts/legacy-file/receipt.pdf')"
                ),
                {"merchant_id": merchant["id"]},
            )
            connection.exec_driver_sql(
                "CREATE TABLE receipt_files "
                "(file_id TEXT PRIMARY KEY, merchant_id INTEGER, data BLOB)"
            )
            connection.exec_driver_sql(
                "INSERT INTO receipt_files VALUES "
                "('legacy-file', NULL, X'25504446')"
            )
        with self.factory() as db:
            counts = count_merchant_owned_rows(db)
        self.assertEqual(counts["point_submissions"], 1)
        self.assertEqual(counts["receipt_files"], 1)

    def test_delete_all_requires_admin_csrf_and_exact_confirmation(self):
        merchant = self.create()
        self.client.cookies.clear()
        unauthorized = self.client.post(
            "/admin-delete-all-merchants",
            data={
                "csrf_token": self.csrf,
                "confirmation": "УДАЛИТЬ ВСЕХ МЕРЧЕНДАЙЗЕРОВ",
            },
            follow_redirects=False,
        )
        self.assertEqual(unauthorized.status_code, 303)
        self.assertEqual(unauthorized.headers["location"], "/admin-login")

        self.client.cookies.set("admin_auth", self.admin_cookie)
        forbidden = self.client.post(
            "/admin-delete-all-merchants",
            data={
                "csrf_token": "",
                "confirmation": "УДАЛИТЬ ВСЕХ МЕРЧЕНДАЙЗЕРОВ",
            },
            follow_redirects=False,
        )
        self.assertEqual(forbidden.status_code, 403)
        rejected = self.client.post(
            "/admin-delete-all-merchants",
            data={"csrf_token": self.csrf, "confirmation": "удалить"},
            follow_redirects=False,
        )
        self.assertEqual(rejected.status_code, 303)
        self.assertIn("error=", rejected.headers["location"])
        with self.factory() as db:
            self.assertEqual(
                db.execute(
                    text("SELECT COUNT(*) FROM merchants WHERE id=:id"),
                    {"id": merchant["id"]},
                ).scalar(),
                1,
            )

    def test_delete_all_removes_only_merchant_owned_data(self):
        first = self.create("Иванов Иван", "1234")
        second = self.create("Петров Пётр", "5678")
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE monthly_submissions "
                "(id INTEGER PRIMARY KEY, merchant_id INTEGER, receipt_path TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE point_adjustments "
                "(id INTEGER PRIMARY KEY, merchant_id INTEGER, reimb_receipt TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE point_notes "
                "(id TEXT PRIMARY KEY, merchant_id INTEGER)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE point_reimbursements "
                "(id TEXT PRIMARY KEY, merchant_id INTEGER)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE reimbursement_receipts "
                "(id TEXT PRIMARY KEY, reimbursement_id TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE receipt_files "
                "(file_id TEXT PRIMARY KEY, merchant_id INTEGER, data BLOB)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE points (point_code TEXT PRIMARY KEY)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE supplies "
                "(id INTEGER PRIMARY KEY, point_code TEXT)"
            )
            connection.execute(
                text(
                    "INSERT INTO visits (merchant_id, point_code) VALUES "
                    "(:first, 'P1'), (:second, 'P2')"
                ),
                {"first": first["id"], "second": second["id"]},
            )
            connection.execute(
                text(
                    "INSERT INTO monthly_submissions VALUES "
                    "(1, :first, 'receipts/file-a/receipt.pdf')"
                ),
                {"first": first["id"]},
            )
            connection.execute(
                text(
                    "INSERT INTO point_adjustments VALUES "
                    "(1, :first, 'receipts/file-b/receipt.png')"
                ),
                {"first": first["id"]},
            )
            connection.execute(
                text("INSERT INTO point_notes VALUES ('n1', :first)"),
                {"first": first["id"]},
            )
            connection.execute(
                text("INSERT INTO point_reimbursements VALUES ('r1', :first)"),
                {"first": first["id"]},
            )
            connection.exec_driver_sql(
                "INSERT INTO reimbursement_receipts VALUES ('rr1', 'r1')"
            )
            connection.execute(
                text(
                    "INSERT INTO receipt_files VALUES "
                    "('file-a', NULL, X'25504446'), "
                    "('file-b', :first, X'89504E47')"
                ),
                {"first": first["id"]},
            )
            connection.exec_driver_sql("INSERT INTO points VALUES ('P1')")
            connection.exec_driver_sql("INSERT INTO supplies VALUES (1, 'P1')")

        response = self.client.post(
            "/admin-delete-all-merchants",
            data={
                "csrf_token": self.csrf,
                "confirmation": "УДАЛИТЬ ВСЕХ МЕРЧЕНДАЙЗЕРОВ",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("success=", response.headers["location"])
        with self.factory() as db:
            for table_name in (
                "merchants",
                "visits",
                "monthly_submissions",
                "point_adjustments",
                "point_notes",
                "point_reimbursements",
                "reimbursement_receipts",
                "receipt_files",
                "merchant_audit_log",
            ):
                self.assertEqual(
                    db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar(),
                    0,
                    table_name,
                )
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM points")).scalar(), 1)
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM supplies")).scalar(), 1)

    def test_delete_all_error_rolls_back_without_500(self):
        merchant = self.create()

        def delete_then_fail(db):
            db.execute(text("DELETE FROM merchants"))
            raise RuntimeError("synthetic secret database detail")

        with patch(
            "app.main.delete_all_merchants_and_data",
            side_effect=delete_then_fail,
        ):
            response = self.client.post(
                "/admin-delete-all-merchants",
                data={
                    "csrf_token": self.csrf,
                    "confirmation": "УДАЛИТЬ ВСЕХ МЕРЧЕНДАЙЗЕРОВ",
                },
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303)
        self.assertNotEqual(response.status_code, 500)
        self.assertNotIn("secret", response.headers["location"])
        with self.factory() as db:
            self.assertEqual(
                db.execute(
                    text("SELECT COUNT(*) FROM merchants WHERE id=:id"),
                    {"id": merchant["id"]},
                ).scalar(),
                1,
            )

    def test_deactivate_all_requires_admin_and_csrf(self):
        merchant = self.create()
        self.client.cookies.clear()
        unauthorized = self.client.post(
            "/admin-clear-merchants",
            data={"csrf_token": self.csrf},
            follow_redirects=False,
        )
        self.assertEqual(unauthorized.status_code, 303)
        self.assertEqual(unauthorized.headers["location"], "/admin-login")

        self.client.cookies.set("admin_auth", self.admin_cookie)
        forbidden = self.client.post(
            "/admin-clear-merchants",
            data={"csrf_token": ""},
            follow_redirects=False,
        )
        self.assertEqual(forbidden.status_code, 403)
        with self.factory() as db:
            self.assertTrue(
                db.execute(
                    text("SELECT is_active FROM merchants WHERE id=:id"),
                    {"id": merchant["id"]},
                ).scalar()
            )

    def test_deactivate_all_preserves_history_and_is_idempotent(self):
        first = self.create("Иванов Иван", "1234")
        second = self.create("Петров Пётр", "5678")
        history_tables = (
            "monthly_submissions",
            "point_adjustments",
            "point_notes",
            "point_reimbursements",
            "reimbursement_receipts",
            "receipt_files",
        )
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO visits (merchant_id, point_code, visit_date, slot) "
                    "VALUES (:id, 'P1', '2026-07-01', 'DAY')"
                ),
                {"id": first["id"]},
            )
            for table_name in history_tables:
                connection.exec_driver_sql(
                    f"CREATE TABLE {table_name} "
                    "(id INTEGER PRIMARY KEY AUTOINCREMENT, merchant_id INTEGER NOT NULL)"
                )
                connection.execute(
                    text(f"INSERT INTO {table_name} (merchant_id) VALUES (:id)"),
                    {"id": first["id"]},
                )

        first_run = self.client.post(
            "/admin-clear-merchants",
            data={"csrf_token": self.csrf},
            follow_redirects=False,
        )
        self.assertEqual(first_run.status_code, 303)
        self.assertIn("2", first_run.headers["location"])
        second_run = self.client.post(
            "/admin-clear-merchants",
            data={"csrf_token": self.csrf},
            follow_redirects=False,
        )
        self.assertEqual(second_run.status_code, 303)
        self.assertIn("0", second_run.headers["location"])

        with self.factory() as db:
            statuses = db.execute(
                text("SELECT id, is_active FROM merchants ORDER BY id")
            ).all()
            self.assertEqual(statuses, [(first["id"], 0), (second["id"], 0)])
            self.assertEqual(
                db.execute(text("SELECT merchant_id FROM visits")).scalar(),
                first["id"],
            )
            for table_name in history_tables:
                self.assertEqual(
                    db.execute(
                        text(f"SELECT merchant_id FROM {table_name}")
                    ).scalar(),
                    first["id"],
                )

    def test_deactivate_all_database_error_rolls_back_without_500(self):
        merchant = self.create()

        def deactivate_then_fail(db, **_kwargs):
            db.execute(
                text("UPDATE merchants SET is_active=FALSE WHERE id=:id"),
                {"id": merchant["id"]},
            )
            sensitive_error_detail = "synthetic secret database detail"
            raise RuntimeError(sensitive_error_detail)

        with patch("app.main.clear_all_merchants", side_effect=deactivate_then_fail):
            with self.assertLogs("app.main", level="ERROR") as captured:
                response = self.client.post(
                    "/admin-clear-merchants",
                    data={"csrf_token": self.csrf},
                    follow_redirects=False,
                )
        self.assertEqual(response.status_code, 303)
        self.assertNotEqual(response.status_code, 500)
        self.assertIn("error=", response.headers["location"])
        self.assertNotIn("secret", response.headers["location"])
        self.assertNotIn("secret", "\n".join(captured.output))
        with self.factory() as db:
            self.assertTrue(
                db.execute(
                    text("SELECT is_active FROM merchants WHERE id=:id"),
                    {"id": merchant["id"]},
                ).scalar()
            )

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

    def test_import_reuses_exact_identity_and_creates_new(self):
        self.create("Иванов Иван", "1234")
        with self.factory() as db:
            result = import_merchants_xlsx(
                db,
                merchant_workbook(
                    [["Петров Пётр", "5678"], ["ИВАНОВ ИВАН", "1234"]]
                ),
                "ТУ-1",
            )
            self.assertEqual(result["created"], 1)
            self.assertEqual(result["reactivated"], 0)
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM merchants")).scalar(), 2)

    def test_import_reactivates_exact_identity_and_leaves_absent_inactive(self):
        matched = self.create("Иванов Иван", "1234", "Старый ТУ")
        absent = self.create("Отсутствует В Файле", "9999", "Старый ТУ")
        with self.factory() as db:
            db.execute(
                text("UPDATE merchants SET last4=NULL WHERE id=:id"),
                {"id": matched["id"]},
            )
            db.commit()
        deactivated = self.client.post(
            "/admin-clear-merchants",
            data={"csrf_token": self.csrf},
            follow_redirects=False,
        )
        self.assertEqual(deactivated.status_code, 303)

        with self.factory() as db:
            result = import_merchants_xlsx(
                db,
                merchant_workbook(
                    [
                        ["  ИВАНОВ   ИВАН  ", "1234"],
                        ["Иванов Иван", "5678"],
                        ["Новый Сотрудник", "4321"],
                    ]
                ),
                "Новый ТУ",
            )
            self.assertEqual(result, {"loaded_rows": 3, "created": 2, "reactivated": 1})
            matched_after = db.execute(
                text(
                    "SELECT id, is_active, tu FROM merchants "
                    "WHERE fio_norm=:fio_norm AND last4='1234'"
                ),
                {"fio_norm": fio_norm("Иванов Иван")},
            ).one()
            self.assertEqual(matched_after, (matched["id"], 1, "Новый ТУ"))
            self.assertEqual(
                db.execute(
                    text("SELECT is_active FROM merchants WHERE id=:id"),
                    {"id": absent["id"]},
                ).scalar(),
                0,
            )
            self.assertEqual(
                db.execute(
                    text(
                        "SELECT COUNT(*) FROM merchants "
                        "WHERE fio_norm=:fio_norm"
                    ),
                    {"fio_norm": fio_norm("Иванов Иван")},
                ).scalar(),
                2,
            )
            self.assertIsNotNone(login_user(db, "Иванов Иван", "1234"))
            self.assertIsNotNone(login_user(db, "Новый Сотрудник", "4321"))
            self.assertIsNone(login_user(db, "Отсутствует В Файле", "9999"))

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
            with self.assertRaisesRegex(ValueError, "точный дубль строк 2 и 3"):
                import_merchants_xlsx(
                    db,
                    merchant_workbook(
                        [["Иванов Иван", "1234"], [" ИВАНОВ  ИВАН ", "1234"]]
                    ),
                    "ТУ-1",
                )
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM merchants")).scalar(), 0)


if __name__ == "__main__":
    unittest.main()
