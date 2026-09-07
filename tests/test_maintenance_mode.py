import os
import unittest
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ADMIN_LOGIN", "admin")
os.environ.setdefault("ADMIN_PASSWORD", "strong-password")
os.environ.setdefault("ENVIRONMENT", "test")

from fastapi.testclient import TestClient

from app import main
from app.production_calendar import ensure_production_calendar_table
from app.runtime import maintenance_mode_enabled
from app.services import (
    ensure_monthly_submissions_table,
    ensure_point_adjustments_table,
    ensure_special_inventory_days_table,
)


class ExplodingSession:
    def execute(self, *_args, **_kwargs):
        raise AssertionError("maintenance mode attempted a database write")

    def commit(self):
        raise AssertionError("maintenance mode attempted a database commit")


class MaintenanceModeTests(unittest.TestCase):
    def test_truthy_values_enable_mode(self):
        for value in ("1", "true", "YES", " on "):
            with self.subTest(value=value), patch.dict(os.environ, {"MAINTENANCE_MODE": value}):
                self.assertTrue(maintenance_mode_enabled())

    def test_get_and_healthcheck_remain_available(self):
        with patch.dict(os.environ, {"MAINTENANCE_MODE": "1"}):
            with TestClient(main.app) as client:
                self.assertEqual(client.get("/login-page").status_code, 200)
                self.assertEqual(client.get("/db-check").status_code, 200)

    def test_all_mutation_methods_are_blocked_before_route_handling(self):
        with patch.dict(os.environ, {"MAINTENANCE_MODE": "1"}):
            with TestClient(main.app) as client:
                for method in ("post", "put", "patch", "delete"):
                    with self.subTest(method=method):
                        response = getattr(client, method)("/route-does-not-matter")
                        self.assertEqual(response.status_code, 503)
                        self.assertIn("только для просмотра", response.text)
                        self.assertEqual(response.headers["cache-control"], "no-store")

    def test_startup_skips_schema_and_calendar_writes(self):
        with patch.dict(os.environ, {"MAINTENANCE_MODE": "1"}), patch.object(
            main, "SessionLocal", side_effect=AssertionError("database session opened")
        ):
            main.ensure_admin_schema_on_startup()

    def test_lazy_schema_helpers_are_noops(self):
        db = ExplodingSession()
        with patch.dict(os.environ, {"MAINTENANCE_MODE": "1"}):
            ensure_special_inventory_days_table(db)
            ensure_monthly_submissions_table(db)
            ensure_point_adjustments_table(db)
            ensure_production_calendar_table(db)
            main.ensure_receipt_files_table(db)


if __name__ == "__main__":
    unittest.main()
