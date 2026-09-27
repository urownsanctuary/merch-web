"""Import failure responses must not disclose exception values or lose rollback."""
import os
import unittest
from collections import defaultdict
from contextlib import ExitStack
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ADMIN_LOGIN", "admin")
os.environ.setdefault("ADMIN_PASSWORD", "strong-password")
os.environ.setdefault("ENVIRONMENT", "test")

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool
from app import main


class AdminImportErrorTests(unittest.TestCase):
    cases = (
        ("/admin-upload-supplies", "import_supplies_xlsx", {}, "поставок"),
        ("/admin-upload-rates", "import_rates_xlsx", {"year": "2026", "month": "9"}, "ставок"),
        ("/admin-upload-production-calendar", "import_calendar_xlsx", {}, "производственного календаря"),
    )
    private = "PRIVATE_SENTINEL password=secret SQL=SELECT email@example.invalid"

    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool,
                                    connect_args={"check_same_thread": False})
        with self.engine.begin() as con:
            con.exec_driver_sql("CREATE TABLE rollback_probe (id INTEGER PRIMARY KEY)")
            con.exec_driver_sql("INSERT INTO rollback_probe VALUES (1)")
        self.db = Session(self.engine)
        self.previous_overrides = main.app.dependency_overrides.copy()
        main.app.dependency_overrides[main.get_db] = lambda: self.db
        self.client = TestClient(main.app)
        self.cookie = main.get_admin_cookie_value()
        self.client.cookies.set("admin_auth", self.cookie)
        self.csrf = main.get_admin_csrf_token(self.cookie)

    def tearDown(self):
        self.client.close()
        main.app.dependency_overrides.clear()
        main.app.dependency_overrides.update(self.previous_overrides)
        self.db.close()
        self.engine.dispose()

    def post(self, path, fields, csrf=None):
        return self.client.post(path, data={**fields, "csrf_token": self.csrf if csrf is None else csrf},
                                files={"file": ("synthetic.xlsx", b"not an xlsx", "application/octet-stream")},
                                follow_redirects=False)

    def test_unexpected_failure_redirects_to_safe_styled_message_and_logs_redacted_trace(self):
        for path, importer, fields, title in self.cases:
            with self.subTest(path=path), patch.object(main, importer, side_effect=RuntimeError(self.private)), \
                    self.assertLogs(main.logger, level="ERROR") as logs:
                response = self.post(path, fields)
            self.assertEqual(response.status_code, 303)
            location = urlsplit(response.headers["location"])
            self.assertEqual(location.path, "/admin-data")
            message = parse_qs(location.query)["error"][0]
            self.assertIn("Не удалось загрузить файл " + title, message)
            self.assertIn("Проверьте формат файла", message)
            self.assertNotIn(self.private, response.headers["location"])
            self.assertNotIn("PRIVATE_SENTINEL", "\n".join(logs.output))
            self.assertIn("technical details redacted", "\n".join(logs.output))
            self.assertIn("error_type=RuntimeError", "\n".join(logs.output))
            with ExitStack() as stack:
                for name, value in (("get_active_period", {"year": 2026, "month": 9}),
                                    ("count_merchant_owned_rows", defaultdict(int)),
                                    ("get_special_inventory_days", []), ("get_calendar_status", []),
                                    ("get_all_tu_values", [])):
                    stack.enter_context(patch.object(main, name, return_value=value))
                page = self.client.get(response.headers["location"])
            self.assertEqual(page.status_code, 200)
            self.assertIn("class='error-box'", page.text)
            self.assertIn(message, page.text)
            self.assertNotIn("PRIVATE_SENTINEL", page.text)

    def test_partial_transaction_is_rolled_back_and_existing_data_remains(self):
        def failing_import(db, *args, **kwargs):
            db.execute(text("DELETE FROM rollback_probe WHERE id=1"))
            db.execute(text("INSERT INTO rollback_probe VALUES (2)"))
            raise RuntimeError(self.private)
        for path, importer, fields, _ in self.cases:
            with self.subTest(path=path), patch.object(main, importer, side_effect=failing_import), \
                    patch.object(main, "log_redacted_exception"):
                response = self.post(path, fields)
            self.assertEqual(response.status_code, 303)
            self.assertEqual(self.db.execute(text("SELECT id FROM rollback_probe")).scalars().all(), [1])
            self.db.rollback()

    def test_success_response_and_import_arguments_unchanged(self):
        for path, importer, fields, _ in self.cases:
            with self.subTest(path=path), patch.object(main, importer, return_value={"loaded_rows": 2, "loaded_points": 1}) as load, \
                    patch.object(main, "log_redacted_exception") as log:
                response = self.post(path, fields)
            self.assertEqual(response.status_code, 303)
            query = parse_qs(urlsplit(response.headers["location"]).query)
            self.assertIn("строк 2", query["success"][0])
            self.assertNotIn("error", query)
            self.assertIs(load.call_args.args[0], self.db)
            self.assertEqual(load.call_args.args[2:], (2026, 9) if fields else ())
            load.assert_called_once()
            log.assert_not_called()

    def test_missing_admin_session_never_calls_import(self):
        self.client.cookies.clear()
        for path, importer, fields, _ in self.cases:
            with self.subTest(path=path), patch.object(main, importer) as load:
                response = self.post(path, fields)
            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/admin-login")
            load.assert_not_called()

    def test_invalid_csrf_never_calls_import(self):
        for path, importer, fields, _ in self.cases:
            with self.subTest(path=path), patch.object(main, importer) as load:
                response = self.post(path, fields, csrf="invalid")
            self.assertEqual(response.status_code, 403)
            load.assert_not_called()

    def test_actual_malformed_file_returns_redirect_not_500(self):
        for path, _, fields, _ in self.cases:
            with self.subTest(path=path), self.assertLogs(main.logger, level="ERROR"):
                response = self.post(path, fields)
            self.assertEqual(response.status_code, 303)
            self.assertIn("error", parse_qs(urlsplit(response.headers["location"]).query))

    def test_value_error_cannot_leak_untrusted_file_contents(self):
        for path, importer, fields, _ in self.cases:
            with self.subTest(path=path), patch.object(main, importer, side_effect=ValueError(self.private)), \
                    self.assertLogs(main.logger, level="ERROR") as logs:
                response = self.post(path, fields)
            self.assertEqual(response.status_code, 303)
            self.assertNotIn("PRIVATE_SENTINEL", response.headers["location"] + "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
