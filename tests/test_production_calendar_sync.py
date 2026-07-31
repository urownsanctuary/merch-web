import os
import unittest
from datetime import date
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ADMIN_LOGIN", "admin")
os.environ.setdefault("ADMIN_PASSWORD", "strong-password")
os.environ.setdefault("ENVIRONMENT", "test")

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app, build_calendar_html, get_admin_cookie_value, get_admin_csrf_token
from app.production_calendar import (
    DAY_HOLIDAY,
    DAY_TRANSFERRED_OFF,
    DAY_WORKING_SATURDAY,
    build_official_calendar,
    ensure_production_calendar_table,
    reset_manual_calendar_override,
    set_manual_calendar_override,
    sync_calendar_year,
)


class ProductionCalendarSyncTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.factory = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_2025_has_transferred_days_and_working_saturday(self):
        rows = build_official_calendar(2025)
        by_day = {row["calendar_date"]: row for row in rows}
        self.assertEqual(len(rows), 365)
        self.assertTrue(by_day[date(2025, 5, 2)]["is_day_off"])
        self.assertEqual(by_day[date(2025, 5, 2)]["day_type"], DAY_TRANSFERRED_OFF)
        self.assertFalse(by_day[date(2025, 11, 1)]["is_day_off"])
        self.assertEqual(by_day[date(2025, 11, 1)]["day_type"], DAY_WORKING_SATURDAY)
        self.assertEqual(by_day[date(2025, 11, 4)]["day_type"], DAY_HOLIDAY)

    def test_2026_is_complete_and_uses_only_approved_transfer(self):
        rows = build_official_calendar(2026)
        by_day = {row["calendar_date"]: row for row in rows}
        self.assertEqual(len(rows), 365)
        self.assertEqual(by_day[date(2026, 1, 9)]["day_type"], DAY_TRANSFERRED_OFF)
        self.assertEqual(by_day[date(2026, 3, 9)]["day_type"], DAY_TRANSFERRED_OFF)
        self.assertEqual(by_day[date(2026, 5, 11)]["day_type"], DAY_TRANSFERRED_OFF)
        self.assertTrue(by_day[date(2026, 12, 31)]["is_day_off"])

    def test_unapproved_future_year_is_not_invented(self):
        with self.assertRaisesRegex(ValueError, "ещё не утверждён"):
            build_official_calendar(2027)

    def test_sync_is_idempotent_and_manual_override_survives(self):
        with self.factory() as db:
            sync_calendar_year(db, 2026)
            sync_calendar_year(db, 2026)
            count = db.execute(text(
                "SELECT COUNT(*) FROM production_calendar WHERE year=2026"
            )).scalar()
            self.assertEqual(count, 365)

            set_manual_calendar_override(
                db,
                date(2026, 7, 11),
                False,
                "Локальный рабочий день",
                "Подтверждено администратором",
                actor="admin",
            )
            sync_calendar_year(db, 2026)
            row = db.execute(text("""
                SELECT is_day_off, day_type, is_manual_override
                FROM production_calendar WHERE calendar_date='2026-07-11'
            """)).one()
            self.assertFalse(row[0])
            self.assertEqual(row[1], "manual_override")
            self.assertTrue(row[2])

            reset_manual_calendar_override(db, date(2026, 7, 11), actor="admin")
            restored = db.execute(text("""
                SELECT is_day_off, day_type, is_manual_override
                FROM production_calendar WHERE calendar_date='2026-07-11'
            """)).one()
            self.assertTrue(restored[0])
            self.assertEqual(restored[1], "weekend")
            self.assertFalse(restored[2])

    def test_calendar_html_marks_day_off_but_not_working_saturday(self):
        html = build_calendar_html(
            fio="Тест",
            point_code="1",
            y=2025,
            m=11,
            boxes_map={},
            visits={},
            is_submitted=False,
            special_inventory_days=set(),
            calendar_overrides={
                date(2025, 11, 1): False,
                date(2025, 11, 2): True,
            },
        )
        self.assertIn('class="day day-red"', html)
        self.assertIn(">1</div>", html)
        day_one = html.split(">1</div>", 1)[0].rsplit('<a href=', 1)[-1]
        self.assertNotIn("day-red", day_one)

    def test_schema_setup_is_repeatable(self):
        with self.factory() as db:
            ensure_production_calendar_table(db)
            ensure_production_calendar_table(db)
            tables = {
                row[0]
                for row in db.execute(text(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )).all()
            }
        self.assertIn("production_calendar", tables)
        self.assertIn("production_calendar_sync", tables)
        self.assertIn("production_calendar_audit", tables)


class ProductionCalendarAdminRouteTests(unittest.TestCase):
    def test_sync_requires_csrf(self):
        with TestClient(app) as client:
            client.cookies.set("admin_auth", get_admin_cookie_value())
            response = client.post(
                "/admin-sync-production-calendar",
                data={"year": "2026", "csrf_token": ""},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 403)

    def test_sync_starts_background_worker_and_redirects(self):
        cookie = get_admin_cookie_value()
        token = get_admin_csrf_token(cookie)
        with patch("app.main.threading.Thread") as thread:
            with TestClient(app) as client:
                client.cookies.set("admin_auth", cookie)
                response = client.post(
                    "/admin-sync-production-calendar",
                    data={"year": "2026", "csrf_token": token},
                    follow_redirects=False,
                )
        self.assertEqual(response.status_code, 303)
        thread.assert_called_once()
        thread.return_value.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
