"""Receipt exclusions precede pairing and never suppress separate inventory."""
import unittest
from datetime import date
from unittest.mock import patch

from sqlalchemy import event, text

from tests import test_calendar_intersections as fixtures
from app import services


class NoSupplyOverlapTests(unittest.TestCase):
    setUp = fixtures.CalendarIntersectionTests.setUp
    tearDown = fixtures.CalendarIntersectionTests.tearDown
    visit = fixtures.CalendarIntersectionTests.visit
    rows = fixtures.CalendarIntersectionTests.rows

    def note(self, comment, merchant=1, point="3284", month="2026-09-01", amount=-400):
        self.db.execute(text("""
            INSERT INTO point_adjustments (merchant_id, point_code, month_key, note_amount, note_comment)
            VALUES (:merchant, :point, :month, :amount, :comment)
        """), dict(merchant=merchant, point=point, month=month, amount=amount, comment=comment))
        self.db.commit()

    def test_dated_reason_removes_day_before_calendar_pairing_and_report_flag(self):
        for person in (1, 2):
            self.visit(person, 7)
        self.note("-400 ₽ — Не принимал поставку 07.09")
        self.assertEqual(self.rows(), [])
        report = services.get_admin_report_rows(self.db, 2026, 9)
        self.assertEqual(len(report), 2)
        self.assertFalse(any(r["has_overlap"] for r in report))
        self.assertEqual(self.db.execute(text("SELECT COUNT(*) FROM visits")).scalar(), 2)

    def test_all_explicit_dates_and_duplicate_dates_in_one_note(self):
        for day in (7, 14, 21):
            for person in (1, 2):
                self.visit(person, day)
        self.note("Не принимал поставку 07.09, 14.09 и 07.09")
        self.assertEqual([r["visit_date"] for r in self.rows()], ["2026-09-21"])

    def test_amount_with_other_reason_does_not_exclude(self):
        for person in (1, 2):
            self.visit(person, 7)
        self.note("-400 ₽ — Другая причина 07.09")
        self.assertEqual(len(self.rows()), 1)

    def test_reason_ignores_amount_and_preserves_separate_evening(self):
        for person in (1, 2):
            self.visit(person, 4, "MORNING")
            self.visit(person, 4, "EVENING")
        self.note("НЕ ПРИНИМАЛ ПОСТАВКУ 04.09", amount=0)
        self.assertEqual([r["slot1"] for r in self.rows()], ["EVENING"])
        self.assertEqual([r["slot1"] for r in services.get_intersections_rows(self.db, 2026, 9)], ["EVENING"])

    def test_filtered_morning_does_not_hide_valid_day_evening_overlap(self):
        self.visit(1, 4, "MORNING")
        self.visit(1, 4, "EVENING")
        self.visit(2, 4, "MORNING")
        self.visit(2, 4, "DAY")
        self.note("Не принимал поставку 04.09")
        # The removed shared morning must not suppress the remaining DAY/EVENING.
        self.assertEqual([r["slot1"] for r in self.rows()], ["CALENDAR_DAY"])

    def test_scope_is_merchant_point_date_and_month(self):
        for point in ("3284", "OTHER"):
            for day in (7, 14):
                for person in (1, 2):
                    self.visit(person, day, point=point)
        self.note("Не принимал поставку 07.09")
        self.note("Не принимал поставку 14.09", merchant=3, point="OTHER")
        self.note("Не принимал поставку 14.09", point="OTHER", month="2026-08-01")
        self.assertEqual({(r["point_code"], r["visit_date"]) for r in self.rows()},
                         {("3284", "2026-09-14"), ("OTHER", "2026-09-07"), ("OTHER", "2026-09-14")})

    def test_normalized_notes_use_same_rule_without_stale_legacy_notes(self):
        self.db.execute(text("CREATE TABLE point_notes (merchant_id INTEGER, point_code TEXT, month_key DATE, comment TEXT)"))
        for day in (4, 11):
            for person in (1, 2):
                self.visit(person, day, "MORNING")
                self.visit(person, day, "EVENING")
        self.note("Не принимал поставку 11.09")
        self.db.execute(text("INSERT INTO point_notes VALUES (1,'3284','2026-09-01','Не принимал поставку 04.09')"))
        self.db.commit()
        with patch.object(services, "normalized_adjustments_available", return_value=True):
            rows = self.rows()
        self.assertEqual({(r["visit_date"], r["slot1"]) for r in rows},
                         {("2026-09-04", "EVENING"), ("2026-09-11", "MORNING"), ("2026-09-11", "EVENING")})

    def test_undated_reason_is_not_a_month_wide_exclusion_and_reads_only(self):
        for person in (1, 2):
            self.visit(person, 7)
        self.note("Не принимал поставку")
        statements = []
        def guard(conn, cursor, sql, parameters, context, many):
            statements.append(sql.lstrip().split()[0].upper())
        event.listen(self.engine, "before_cursor_execute", guard)
        try:
            self.assertEqual(len(self.rows()), 1)
        finally:
            event.remove(self.engine, "before_cursor_execute", guard)
        self.assertTrue(all(verb in ("SELECT", "WITH", "PRAGMA") for verb in statements))

    def test_parser_does_not_borrow_other_reason_dates_or_invalid_period(self):
        cases = [
            ("Не принимал поставку 01.09. 30.09", {1, 30}),
            ("не\u00a0принимал поставку: 7.9.2026; 14.09.2026", {7, 14}),
            ("Не принимала поставку 07.09\nНе принимал поставку 14.09", {7, 14}),
            ("Не принимал поставку 07.09. Другая причина 14.09", {7}),
            ("Не принимал поставку\n-400 ₽ — Другая причина 14.09", set()),
            ("Не принимал поставку 31.09, 01.10, 07.09.2025, 14.09", {14}),
            ("-400 ₽ — Другая причина 07.09", set()),
        ]
        for comment, expected in cases:
            with self.subTest(comment=comment):
                self.assertEqual(services.no_supply_note_dates(comment, 2026, 9),
                                 {date(2026, 9, day) for day in expected})
