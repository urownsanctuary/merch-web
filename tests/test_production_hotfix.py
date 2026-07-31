import os
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ADMIN_LOGIN", "admin")
os.environ.setdefault("ADMIN_PASSWORD", "strong-password")
os.environ.setdefault("ENVIRONMENT", "test")

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.main import app, build_calendar_html, get_db
from app.security import create_merchant_session, read_merchant_session
from app.services import (
    InventoryWeekLimitError,
    SLOT_DAY,
    SLOT_EVENING,
    SLOT_FULL_INVENT,
    SLOT_MORNING,
    allowed_visit_slots,
    compute_overall_total,
    compute_point_total,
    get_intersections_rows,
    get_point_adjustment,
    no_supply_adjustment_marker,
    toggle_day_visit,
    toggle_inventory_visit,
)


FIXED_PERIOD = {"year": 2026, "month": 7, "editable_until": "2026-08-05"}
MERCHANT = {"id": 1, "fio": "Тестовый Мерч"}


class SlotHotfixTests(unittest.TestCase):
    def test_allowed_slots_for_every_weekday(self):
        expected = {
            13: {SLOT_DAY},       # Monday
            14: {SLOT_DAY},       # Tuesday
            15: {SLOT_DAY},       # Wednesday
            16: {SLOT_DAY},       # Thursday
            17: {SLOT_MORNING, SLOT_EVENING},
            18: {SLOT_MORNING, SLOT_EVENING},
            19: {SLOT_DAY},       # Sunday
        }
        for day, slots in expected.items():
            self.assertEqual(allowed_visit_slots(date(2026, 7, day)), slots)

    def test_special_inventory_is_separate_from_weekday_slots(self):
        self.assertEqual(
            allowed_visit_slots(date(2026, 7, 15), special_inventory=True),
            {SLOT_DAY, SLOT_FULL_INVENT},
        )
        self.assertEqual(
            allowed_visit_slots(date(2026, 7, 17), special_inventory=True),
            {SLOT_MORNING, SLOT_EVENING},
        )

    def test_calendar_uses_only_business_markers(self):
        html = build_calendar_html(
            fio="Тестовый Мерч",
            point_code="2674",
            y=2026,
            m=7,
            boxes_map={},
            visits={
                13: {SLOT_DAY},
                17: {SLOT_MORNING},
                18: {SLOT_EVENING},
                19: {SLOT_MORNING, SLOT_EVENING},
            },
            is_submitted=False,
            special_inventory_days=set(),
            calendar_overrides={},
        )
        self.assertEqual(html.count(">В</span>"), 3)
        self.assertEqual(html.count(">И</span>"), 2)
        self.assertNotIn(">У</span>", html)
        self.assertNotIn(">Вч</span>", html)

    def test_red_day_remains_clickable_and_working_saturday_is_not_red(self):
        html = build_calendar_html(
            fio="Тестовый Мерч",
            point_code="2674",
            y=2025,
            m=11,
            boxes_map={},
            visits={},
            is_submitted=False,
            special_inventory_days=set(),
            calendar_overrides={date(2025, 11, 1): False},
        )
        self.assertIn("day=1", html)
        day_one_href = html.index('href="/day-action-page')
        day_one_anchor = html[html.rfind("<a ", 0, day_one_href):html.index(">", day_one_href)]
        self.assertNotIn("day-red", day_one_anchor)

    def test_day_pages_show_only_allowed_actions(self):
        db = MagicMock()
        app.dependency_overrides[get_db] = lambda: db
        token = create_merchant_session("тестовый мерч")
        try:
            common = {
                "get_active_period": MagicMock(return_value=FIXED_PERIOD),
                "get_merchant_by_fio": MagicMock(return_value=MERCHANT),
                "compute_overall_total": MagicMock(return_value={"submission_status": "draft"}),
                "get_visits_for_month": MagicMock(return_value={}),
                "get_special_inventory_days": MagicMock(return_value=[]),
                "get_calendar_overrides": MagicMock(return_value={}),
            }
            with patch.multiple("app.main", **common):
                with TestClient(app) as client:
                    client.cookies.set("merchant_session", token)
                    wednesday = client.get(
                        "/day-action-page",
                        params={"fio": "Тестовый Мерч", "point_code": "2674", "day": 15},
                    )
                    friday = client.get(
                        "/day-action-page",
                        params={"fio": "Тестовый Мерч", "point_code": "2674", "day": 17},
                    )
                    saturday = client.get(
                        "/day-action-page",
                        params={"fio": "Тестовый Мерч", "point_code": "2674", "day": 18},
                    )
            self.assertIn("Добавить выход", wednesday.text)
            self.assertNotIn("утренний", wednesday.text.lower())
            self.assertNotIn("вечерний", wednesday.text.lower())
            for response in (friday, saturday):
                self.assertIn("Добавить утренний выход", response.text)
                self.assertIn("Добавить вечерний полный инвент", response.text)
        finally:
            app.dependency_overrides.clear()

    def test_non_working_day_requires_explicit_confirmation(self):
        db = MagicMock()
        app.dependency_overrides[get_db] = lambda: db
        token = create_merchant_session("Тестовый Мерч")
        csrf = read_merchant_session(token)["csrf"]
        toggle = MagicMock()
        common = {
            "get_active_period": MagicMock(return_value=FIXED_PERIOD),
            "get_merchant_by_fio": MagicMock(return_value=MERCHANT),
            "compute_overall_total": MagicMock(return_value={"submission_status": "draft"}),
            "get_visits_for_month": MagicMock(return_value={}),
            "get_special_inventory_days": MagicMock(return_value=[]),
            "get_calendar_overrides": MagicMock(
                return_value={date(2026, 7, 19): True}
            ),
            "toggle_day_visit": toggle,
        }
        try:
            with patch.multiple("app.main", **common):
                with TestClient(app) as client:
                    client.cookies.set("merchant_session", token)
                    page = client.get(
                        "/day-action-page",
                        params={
                            "fio": "Тестовый Мерч",
                            "point_code": "2674",
                            "day": 19,
                        },
                    )
                    rejected = client.post(
                        "/toggle-day",
                        data={
                            "fio": "Тестовый Мерч",
                            "point_code": "2674",
                            "day": 19,
                            "slot": SLOT_DAY,
                            "csrf_token": csrf,
                        },
                    )
                    accepted = client.post(
                        "/toggle-day",
                        data={
                            "fio": "Тестовый Мерч",
                            "point_code": "2674",
                            "day": 19,
                            "slot": SLOT_DAY,
                            "csrf_token": csrf,
                            "confirm_non_working": "1",
                        },
                        follow_redirects=False,
                    )
            self.assertEqual(page.status_code, 200)
            self.assertIn('name="confirm_non_working"', page.text)
            self.assertIn("официальный производственный выходной", page.text)
            self.assertEqual(rejected.status_code, 409)
            self.assertEqual(accepted.status_code, 303)
            toggle.assert_called_once()
        finally:
            app.dependency_overrides.clear()

    def test_only_one_full_inventory_per_point_and_iso_week(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE visits (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "merchant_id INTEGER NOT NULL, point_code TEXT NOT NULL, "
                "visit_date DATE NOT NULL, slot TEXT NOT NULL, "
                "UNIQUE (merchant_id, point_code, visit_date, slot))"
            )
        with Session(engine) as db:
            self.assertEqual(
                toggle_day_visit(db, 1, "2674", 2026, 7, 17, SLOT_EVENING),
                "added",
            )
            with self.assertRaises(InventoryWeekLimitError):
                toggle_day_visit(db, 1, "2674", 2026, 7, 18, SLOT_EVENING)
            db.rollback()
            with self.assertRaises(InventoryWeekLimitError):
                toggle_inventory_visit(
                    db, 1, "2674", 2026, 7, 15, special_inventory=True
                )
            db.rollback()
            self.assertEqual(
                toggle_day_visit(db, 1, "2674", 2026, 7, 24, SLOT_EVENING),
                "added",
            )

    def test_structured_no_supply_adjustment_suppresses_overlap(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE merchants (id INTEGER PRIMARY KEY, fio TEXT, tu TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE visits (id INTEGER PRIMARY KEY, merchant_id INTEGER, "
                "point_code TEXT, visit_date DATE, slot TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE point_adjustments (id INTEGER PRIMARY KEY, "
                "merchant_id INTEGER, point_code TEXT, month_key DATE, "
                "note_amount INTEGER DEFAULT 0, note_comment TEXT, "
                "reimb_amount INTEGER DEFAULT 0, reimb_comment TEXT, "
                "reimb_receipt TEXT, UNIQUE (merchant_id, point_code, month_key))"
            )
            connection.exec_driver_sql(
                "CREATE TABLE point_notes (id TEXT PRIMARY KEY, merchant_id INTEGER, "
                "point_code TEXT, month_key DATE, amount INTEGER, comment TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE point_reimbursements (id TEXT PRIMARY KEY, "
                "merchant_id INTEGER, point_code TEXT, month_key DATE)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE reimbursement_receipts (id TEXT PRIMARY KEY, "
                "reimbursement_id TEXT)"
            )
            connection.exec_driver_sql(
                "INSERT INTO merchants VALUES (1, 'Первый', 'ТУ-1'), "
                "(2, 'Второй', 'ТУ-2')"
            )
            connection.exec_driver_sql(
                "INSERT INTO visits VALUES "
                "(1, 1, '2674', '2026-07-17', 'MORNING'), "
                "(2, 2, '2674', '2026-07-17', 'MORNING')"
            )
        with Session(engine) as db:
            self.assertEqual(len(get_intersections_rows(db, 2026, 7)), 1)
            db.execute(
                text(
                    "INSERT INTO point_notes "
                    "(id, merchant_id, point_code, month_key, amount, comment) "
                    "VALUES ('note-1', 1, '2674', '2026-07-01', 0, :comment)"
                ),
                {"comment": no_supply_adjustment_marker(date(2026, 7, 17))},
            )
            db.commit()
            self.assertEqual(get_intersections_rows(db, 2026, 7), [])

    def test_direct_invalid_slots_roll_back_without_write(self):
        db = MagicMock()
        app.dependency_overrides[get_db] = lambda: db
        token = create_merchant_session("тестовый мерч")
        csrf = read_merchant_session(token)["csrf"]
        toggle = MagicMock()
        try:
            with patch.multiple(
                "app.main",
                get_active_period=MagicMock(return_value=FIXED_PERIOD),
                get_merchant_by_fio=MagicMock(return_value=MERCHANT),
                compute_overall_total=MagicMock(return_value={"submission_status": "draft"}),
                get_visits_for_month=MagicMock(return_value={}),
                toggle_day_visit=toggle,
            ):
                with TestClient(app) as client:
                    client.cookies.set("merchant_session", token)
                    morning = client.post(
                        "/toggle-day",
                        data={
                            "fio": "Тестовый Мерч",
                            "point_code": "2674",
                            "day": "15",
                            "slot": SLOT_MORNING,
                            "csrf_token": csrf,
                        },
                    )
                    evening = client.post(
                        "/toggle-day",
                        data={
                            "fio": "Тестовый Мерч",
                            "point_code": "2674",
                            "day": "16",
                            "slot": SLOT_EVENING,
                            "csrf_token": csrf,
                        },
                    )
            self.assertEqual(morning.status_code, 409)
            self.assertEqual(evening.status_code, 409)
            self.assertEqual(db.rollback.call_count, 2)
            toggle.assert_not_called()
        finally:
            app.dependency_overrides.clear()

    def test_morning_and_evening_have_separate_pay_without_double_inventory(self):
        rates = {
            "rate_supply": 800,
            "rate_no_supply": 400,
            "rate_inventory": 500,
            "coffee_enabled": False,
            "coffee_rate": 100,
            "pay_lt5": False,
        }
        with (
            patch("app.services.ensure_point_adjustments_table"),
            patch("app.services.get_supply_boxes_map", return_value={17: 5}),
            patch(
                "app.services.get_visits_for_month",
                return_value={17: {SLOT_MORNING, SLOT_EVENING, SLOT_FULL_INVENT}},
            ),
            patch("app.services.get_point_rates", return_value=rates),
            patch("app.services.get_point_adjustment", return_value=None),
        ):
            result = compute_point_total(MagicMock(), 1, "2674", 2026, 7)
        self.assertEqual(result["cnt_supply"], 1)
        self.assertEqual(result["cnt_full_inv"], 1)
        self.assertEqual(result["sum_supply"], 800)
        self.assertEqual(result["sum_inventory"], 500)
        self.assertEqual(result["total"], 1300)


class MonthlySubmissionHotfixTests(unittest.TestCase):
    def test_normalized_adjustments_replace_legacy_without_double_counting(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE point_adjustments (id INTEGER PRIMARY KEY, merchant_id INTEGER, point_code TEXT, month_key DATE, note_amount INTEGER, note_comment TEXT, reimb_amount INTEGER, reimb_comment TEXT, reimb_receipt TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE point_notes (id TEXT PRIMARY KEY, merchant_id INTEGER, point_code TEXT, month_key DATE, amount INTEGER, comment TEXT, created_at TIMESTAMP)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE point_reimbursements (id TEXT PRIMARY KEY, merchant_id INTEGER, point_code TEXT, month_key DATE, amount INTEGER, comment TEXT, created_at TIMESTAMP)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE reimbursement_receipts (id TEXT PRIMARY KEY, reimbursement_id TEXT, legacy_path TEXT, created_at TIMESTAMP)"
            )
            connection.exec_driver_sql(
                "INSERT INTO point_adjustments VALUES (1,1,'2674','2026-07-01',100,'legacy',50,'legacy','legacy.pdf')"
            )
            connection.exec_driver_sql(
                "INSERT INTO point_notes VALUES ('n1',1,'2674','2026-07-01',100,'normalized note','2026-07-01')"
            )
            connection.exec_driver_sql(
                "INSERT INTO point_reimbursements VALUES ('r1',1,'2674','2026-07-01',50,'normalized reimbursement','2026-07-01')"
            )
            connection.exec_driver_sql(
                "INSERT INTO reimbursement_receipts VALUES ('f1','r1','normalized.pdf','2026-07-01')"
            )
        with Session(engine) as db, patch("app.services.ensure_point_adjustments_table"):
            adjustment = get_point_adjustment(db, 1, "2674", 2026, 7)
        self.assertEqual(adjustment["note_amount"], 100)
        self.assertEqual(adjustment["reimb_amount"], 50)
        self.assertNotIn("legacy", adjustment["note_comment"])
        self.assertEqual(adjustment["reimb_receipt"], "normalized.pdf")

    def test_legacy_adjustments_work_without_normalized_tables(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE visits (id INTEGER PRIMARY KEY, merchant_id INTEGER, point_code TEXT, visit_date DATE, slot TEXT)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE supplies (id INTEGER PRIMARY KEY, point_code TEXT, supply_date DATE, boxes INTEGER)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE point_rates (id INTEGER PRIMARY KEY, point_code TEXT, month_key DATE, rate_supply INTEGER, rate_no_supply INTEGER, rate_inventory INTEGER, coffee_enabled BOOLEAN, coffee_rate INTEGER, pay_lt5 BOOLEAN)"
            )
            connection.exec_driver_sql(
                "CREATE TABLE monthly_submissions (id INTEGER PRIMARY KEY, merchant_id INTEGER, month_key DATE, comment TEXT, extra_amount INTEGER DEFAULT 0, receipt_path TEXT, status TEXT DEFAULT 'draft', created_at TIMESTAMP, updated_at TIMESTAMP, UNIQUE (merchant_id, month_key))"
            )
        with Session(engine) as db:
            db.execute(
                text(
                    """
                    CREATE TABLE point_adjustments (
                        id INTEGER PRIMARY KEY,
                        merchant_id INTEGER NOT NULL,
                        point_code TEXT NOT NULL,
                        month_key DATE NOT NULL,
                        note_amount INTEGER NOT NULL DEFAULT 0,
                        note_comment TEXT,
                        reimb_amount INTEGER NOT NULL DEFAULT 0,
                        reimb_comment TEXT,
                        reimb_receipt TEXT,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        UNIQUE (merchant_id, point_code, month_key)
                    )
                    """
                )
            )
            db.execute(
                text(
                    """
                    INSERT INTO point_adjustments
                        (merchant_id, point_code, month_key, note_amount, note_comment, reimb_amount, reimb_comment)
                    VALUES (1, '2674', '2026-07-01', 100, 'note', 50, 'reimbursement')
                    """
                )
            )
            db.commit()
            with (
                patch("app.services.ensure_monthly_submissions_table"),
                patch("app.services.ensure_point_adjustments_table"),
            ):
                result = compute_overall_total(db, 1, 2026, 7)
        self.assertEqual(result["total"], 150)
        self.assertEqual(result["per_point"], {"2674": 150})
        self.assertEqual(result["per_point_details"]["2674"]["note_amount"], 100)
        self.assertEqual(result["per_point_details"]["2674"]["reimb_amount"], 50)

    def test_monthly_page_has_session_and_does_not_raise_name_error(self):
        db = MagicMock()
        app.dependency_overrides[get_db] = lambda: db
        token = create_merchant_session("тестовый мерч")
        overall = {
            "submission_status": "draft",
            "per_point": {},
            "per_point_details": {},
            "total": 0,
        }
        try:
            with (
                patch("app.main.get_active_period", return_value=FIXED_PERIOD),
                patch("app.main.get_merchant_by_fio", return_value=MERCHANT),
                patch("app.main.compute_overall_total", return_value=overall),
            ):
                with TestClient(app) as client:
                    client.cookies.set("merchant_session", token)
                    response = client.get(
                        "/monthly-submit-page", params={"fio": "Тестовый Мерч"}
                    )
            self.assertEqual(response.status_code, 200)
            self.assertIn("Отправить сверку за месяц", response.text)
        finally:
            app.dependency_overrides.clear()

    def test_submit_is_repeatable_and_rolls_back_on_error(self):
        db = MagicMock()
        app.dependency_overrides[get_db] = lambda: db
        token = create_merchant_session("тестовый мерч")
        csrf = read_merchant_session(token)["csrf"]
        submit = MagicMock()
        try:
            with (
                patch("app.main.get_active_period", return_value=FIXED_PERIOD),
                patch("app.main.get_merchant_by_fio", return_value=MERCHANT),
                patch("app.main.submit_monthly_submission", submit),
            ):
                with TestClient(app) as client:
                    client.cookies.set("merchant_session", token)
                    first = client.post(
                        "/submit-monthly-submission",
                        data={"fio": "Тестовый Мерч", "csrf_token": csrf},
                        follow_redirects=False,
                    )
                    second = client.post(
                        "/submit-monthly-submission",
                        data={"fio": "Тестовый Мерч", "csrf_token": csrf},
                        follow_redirects=False,
                    )
            self.assertEqual(first.status_code, 303)
            self.assertEqual(second.status_code, 303)
            self.assertEqual(submit.call_count, 2)

            with (
                patch("app.main.get_active_period", return_value=FIXED_PERIOD),
                patch("app.main.get_merchant_by_fio", return_value=MERCHANT),
                patch(
                    "app.main.submit_monthly_submission",
                    side_effect=RuntimeError("database unavailable"),
                ),
            ):
                with TestClient(app) as client:
                    client.cookies.set("merchant_session", token)
                    failed = client.post(
                        "/submit-monthly-submission",
                        data={"fio": "Тестовый Мерч", "csrf_token": csrf},
                    )
            self.assertEqual(failed.status_code, 503)
            db.rollback.assert_called_once()
        finally:
            app.dependency_overrides.clear()


if __name__ == "__main__":
    unittest.main()
