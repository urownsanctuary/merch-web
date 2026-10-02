"""HTTP startup never writes, even with the legacy automatic-sync flag enabled."""
import io
import os
import subprocess
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import MagicMock, patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ADMIN_LOGIN", "admin")
os.environ.setdefault("ADMIN_PASSWORD", "strong-password")
os.environ.setdefault("ENVIRONMENT", "test")

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app import main, schema_setup


class StartupReadOnlyTests(unittest.TestCase):
    def test_startup_never_opens_session_or_background_worker(self):
        for mode in ("0", "1"):
            for auto_sync in ("0", "1"):
                with self.subTest(mode=mode, auto_sync=auto_sync), patch.dict(
                    os.environ, {"ENVIRONMENT": "production", "MAINTENANCE_MODE": mode,
                                 "AUTO_SYNC_PRODUCTION_CALENDAR": auto_sync}
                ), patch.object(main, "SessionLocal", side_effect=AssertionError("startup DB access")), \
                        patch.object(main.threading, "Thread", side_effect=AssertionError("startup thread")), \
                        patch.object(main, "initialize_application_schema", side_effect=AssertionError("implicit migration")):
                    main.ensure_admin_schema_on_startup()

    def test_real_lifespan_and_health_emit_only_select(self):
        engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool,
                               connect_args={"check_same_thread": False})
        statements = []
        def capture(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement.strip().upper())
            if not statement.strip().upper().startswith("SELECT"):
                raise AssertionError("HTTP lifecycle attempted non-SELECT SQL")
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with patch.dict(os.environ, {"ENVIRONMENT": "production", "MAINTENANCE_MODE": "0",
                                         "AUTO_SYNC_PRODUCTION_CALENDAR": "1"}), \
                    patch.object(main, "engine", engine), \
                    patch.object(main, "SessionLocal", side_effect=AssertionError("startup session")), \
                    patch.object(main, "_sync_calendar_background", side_effect=AssertionError("calendar write")):
                for _ in range(2):
                    with TestClient(main.app) as client:
                        self.assertEqual(client.get("/db-check").status_code, 200)
                        self.assertEqual(client.get("/login-page").status_code, 200)
            self.assertEqual(statements, ["SELECT 1", "SELECT 1"])
        finally:
            engine.dispose()

    def test_cli_requires_explicit_apply(self):
        with patch.object(main, "initialize_application_schema") as setup, redirect_stderr(io.StringIO()), \
                self.assertRaises(SystemExit) as caught:
            schema_setup.main([])
        self.assertEqual(caught.exception.code, 2)
        setup.assert_not_called()

    def test_cli_help_does_not_need_database_configuration(self):
        env = dict(os.environ)
        for key in ("DATABASE_URL", "ADMIN_LOGIN", "ADMIN_PASSWORD", "SECRET_SALT", "SESSION_SECRET"):
            env.pop(key, None)
        result = subprocess.run([sys.executable, "-B", "-m", "app.schema_setup", "--help"],
                                env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0)
        self.assertIn("--apply", result.stdout)

    def test_cli_refuses_maintenance_without_opening_database(self):
        with patch.dict(os.environ, {"MAINTENANCE_MODE": "1"}), \
                patch.object(main, "SessionLocal", side_effect=AssertionError("DB opened")), \
                patch.object(main, "initialize_application_schema") as setup, redirect_stderr(io.StringIO()):
            self.assertEqual(schema_setup.main(["--apply"]), 1)
        setup.assert_not_called()

    def test_cli_apply_calls_existing_setup_not_calendar_sync(self):
        with patch.dict(os.environ, {"MAINTENANCE_MODE": "0"}), \
                patch.object(main, "initialize_application_schema") as setup, \
                patch.object(main, "_sync_calendar_background") as sync, redirect_stdout(io.StringIO()) as output:
            self.assertEqual(schema_setup.main(["--apply"]), 0)
        setup.assert_called_once_with()
        sync.assert_not_called()
        self.assertIn("SCHEMA_SETUP_COMPLETE", output.getvalue())

    def test_cli_failure_does_not_disclose_database_error(self):
        with patch.dict(os.environ, {"MAINTENANCE_MODE": "0"}), \
                patch.object(main, "initialize_application_schema", side_effect=RuntimeError("synthetic-private-DSN")), \
                redirect_stderr(io.StringIO()) as output:
            self.assertEqual(schema_setup.main(["--apply"]), 1)
        self.assertNotIn("synthetic-private-DSN", output.getvalue())

    def test_explicit_setup_rolls_back_and_closes_on_failure(self):
        db = MagicMock()
        with patch.dict(os.environ, {"MAINTENANCE_MODE": "0"}), \
                patch.object(main, "SessionLocal", return_value=db), \
                patch.object(main, "ensure_merchant_admin_schema", side_effect=RuntimeError("synthetic")), \
                self.assertRaises(RuntimeError):
            main.initialize_application_schema()
        db.rollback.assert_called_once()
        db.close.assert_called_once()

    def test_explicit_apply_and_reapply_preserve_existing_migration(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        try:
            with engine.begin() as db:
                db.execute(text("CREATE TABLE merchants (id INTEGER PRIMARY KEY, fio TEXT, fio_norm TEXT)"))
                db.execute(text("INSERT INTO merchants VALUES (1, 'Synthetic Fixture', 'synthetic fixture')"))
            with patch.dict(os.environ, {"MAINTENANCE_MODE": "0"}), \
                    patch.object(main, "SessionLocal", side_effect=lambda: Session(engine)), \
                    redirect_stdout(io.StringIO()):
                self.assertEqual(schema_setup.main(["--apply"]), 0)
                with engine.connect() as db:
                    before = list(db.execute(text("SELECT * FROM merchants")).all())
                self.assertEqual(schema_setup.main(["--apply"]), 0)
            with engine.connect() as db:
                self.assertEqual(list(db.execute(text("SELECT * FROM merchants")).all()), before)
                self.assertEqual(db.execute(text("SELECT count(*) FROM merchants WHERE created_at IS NOT NULL AND updated_at IS NOT NULL AND is_active=1")).scalar(), 1)
            self.assertTrue({"merchant_audit_log", "production_calendar", "production_calendar_sync",
                             "production_calendar_audit", "receipt_files", "coffee_bonus", "coffee_days_audit"}
                            <= set(inspect(engine).get_table_names()))
            self.assertIn("manual_delta", {c["name"] for c in inspect(engine).get_columns("coffee_bonus")})
        finally:
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
