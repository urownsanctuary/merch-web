import os
import unittest
from datetime import date

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ENVIRONMENT", "test")

from app.production_calendar import calendar_day_off
from app.security import create_merchant_session, read_merchant_session, verify_csrf
from app.services import (
    SLOT_DAY,
    SLOT_EVENING,
    SLOT_FULL_INVENT,
    SLOT_MORNING,
    effective_has_supply,
    find_visit_intersections,
    filter_unadjusted_supply_days,
    fio_norm,
    no_supply_adjustment_marker,
    normalize_visit_slot,
)


class AuthenticationRulesTests(unittest.TestCase):
    def test_fio_case(self):
        self.assertEqual(fio_norm("ИВАНОВ ИВАН"), fio_norm("иванов иван"))

    def test_fio_spaces(self):
        self.assertEqual(fio_norm(" Иванов\u00a0  Иван "), "иванов иван")

    def test_fio_yo(self):
        self.assertEqual(fio_norm("Ёлкин Семён"), fio_norm("Елкин Семен"))

    def test_signed_session_and_csrf(self):
        token = create_merchant_session("иванов иван")
        session = read_merchant_session(token)
        self.assertEqual(session["sub"], "иванов иван")
        self.assertTrue(verify_csrf(session, session["csrf"]))

    def test_tampered_session(self):
        self.assertIsNone(read_merchant_session(create_merchant_session("a") + "x"))


class SupplyRulesTests(unittest.TestCase):
    def test_one_box(self):
        self.assertFalse(effective_has_supply(1, False))

    def test_four_boxes(self):
        self.assertFalse(effective_has_supply(4, False))

    def test_five_boxes(self):
        self.assertTrue(effective_has_supply(5, False))

    def test_pay_lt5(self):
        self.assertTrue(effective_has_supply(1, True))

    def test_no_supply_adjustment_date_is_offered_once(self):
        first = date(2026, 7, 4)
        second = date(2026, 7, 11)
        comment = f"-450 ₽ — {no_supply_adjustment_marker(first)}. smoke"
        self.assertEqual(filter_unadjusted_supply_days([first, second], comment), [second])


class SlotRulesTests(unittest.TestCase):
    def test_all_slots_are_explicit(self):
        self.assertEqual(
            {SLOT_MORNING, SLOT_EVENING, SLOT_DAY, SLOT_FULL_INVENT},
            {"MORNING", "EVENING", "DAY", "FULL_INVENT"},
        )

    def test_user_selection_rejects_legacy_day(self):
        with self.assertRaises(ValueError):
            normalize_visit_slot("DAY", allow_legacy_day=False)

    def _visits(self, *items):
        return [
            {"merchant_id": merchant, "fio": f"M{merchant}", "point_code": point, "visit_date": day, "slot": slot}
            for merchant, point, day, slot in items
        ]

    def test_morning_evening_no_intersection(self):
        rows = self._visits((1, "P1", date(2026, 7, 1), "MORNING"), (2, "P1", date(2026, 7, 1), "EVENING"))
        self.assertEqual(find_visit_intersections(rows), [])

    def test_morning_morning_intersection(self):
        rows = self._visits((1, "P1", date(2026, 7, 1), "MORNING"), (2, "P1", date(2026, 7, 1), "MORNING"))
        self.assertEqual(len(find_visit_intersections(rows)), 1)

    def test_evening_evening_intersection(self):
        rows = self._visits((1, "P1", date(2026, 7, 1), "EVENING"), (2, "P1", date(2026, 7, 1), "EVENING"))
        self.assertEqual(len(find_visit_intersections(rows)), 1)

    def test_different_points_no_intersection(self):
        rows = self._visits((1, "P1", date(2026, 7, 1), "MORNING"), (2, "P2", date(2026, 7, 1), "MORNING"))
        self.assertEqual(find_visit_intersections(rows), [])

    def test_different_dates_no_intersection(self):
        rows = self._visits((1, "P1", date(2026, 7, 1), "MORNING"), (2, "P1", date(2026, 7, 2), "MORNING"))
        self.assertEqual(find_visit_intersections(rows), [])

    def test_same_merchant_no_intersection(self):
        rows = self._visits((1, "P1", date(2026, 7, 1), "MORNING"), (1, "P1", date(2026, 7, 1), "MORNING"))
        self.assertEqual(find_visit_intersections(rows), [])

    def test_pairs_have_no_ab_ba_duplicates(self):
        rows = self._visits(
            (1, "P1", date(2026, 7, 1), "MORNING"),
            (2, "P1", date(2026, 7, 1), "MORNING"),
            (3, "P1", date(2026, 7, 1), "MORNING"),
        )
        pairs = {(row["merchant_a"], row["merchant_b"]) for row in find_visit_intersections(rows)}
        self.assertEqual(pairs, {(1, 2), (1, 3), (2, 3)})

    def test_day_and_inventory_are_not_intersections(self):
        rows = self._visits(
            (1, "P1", date(2026, 7, 1), "DAY"),
            (2, "P1", date(2026, 7, 1), "DAY"),
            (3, "P1", date(2026, 7, 1), "FULL_INVENT"),
            (4, "P1", date(2026, 7, 1), "FULL_INVENT"),
        )
        self.assertEqual(find_visit_intersections(rows), [])


class ProductionCalendarTests(unittest.TestCase):
    def test_normal_workday(self):
        self.assertFalse(calendar_day_off(date(2026, 7, 29), {}))

    def test_saturday(self):
        self.assertTrue(calendar_day_off(date(2026, 8, 1), {}))

    def test_sunday(self):
        self.assertTrue(calendar_day_off(date(2026, 8, 2), {}))

    def test_official_holiday(self):
        day = date(2026, 6, 12)
        self.assertTrue(calendar_day_off(day, {day: True}))

    def test_transferred_day_off(self):
        day = date(2026, 5, 11)
        self.assertTrue(calendar_day_off(day, {day: True}))

    def test_working_saturday_override(self):
        day = date(2026, 8, 1)
        self.assertFalse(calendar_day_off(day, {day: False}))


if __name__ == "__main__":
    unittest.main()
