import io
import os
import unittest
from datetime import date

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ADMIN_LOGIN", "admin")
os.environ.setdefault("ADMIN_PASSWORD", "strong-password")
os.environ.setdefault("ENVIRONMENT", "test")

from openpyxl import Workbook

from app.legacy_migration import build_migration_plan
from app.main import validate_receipt
from app.production_calendar import parse_calendar_workbook
from app.services import import_merchants_xlsx, import_rates_xlsx, import_supplies_xlsx


class FakeMappings:
    def __init__(self, rows=None):
        self.rows = rows or []

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return self.rows


class FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self.rows = rows or []
        self.scalar_value = scalar_value

    def scalar(self):
        return self.scalar_value

    def scalar_one(self):
        return self.scalar_value if self.scalar_value is not None else 1

    def fetchall(self):
        return self.rows

    def mappings(self):
        return FakeMappings(self.rows)


class FakeDB:
    def __init__(self):
        self.calls = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, statement, params=None):
        sql = str(statement)
        self.calls.append((sql, params))
        if "information_schema.columns" in sql:
            return FakeResult([("point_code",), ("supply_date",), ("boxes",), ("has_supply",)])
        return FakeResult()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def workbook_bytes(headers, rows):
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(headers)
    for row in rows:
        sheet.append(row)
    output = io.BytesIO()
    workbook.save(output)
    output.seek(0)
    return output


class ImportTests(unittest.TestCase):
    def test_rates_import_all_fields_and_single_commit(self):
        file_obj = workbook_bytes(
            ["Точка", "С поставкой", "Без поставки", "Инвент", "Кофе", "pay_lt5", "Ставка кофе"],
            [["P1", 800, 400, 300, "да", "да", 100], ["P2", 900, 450, 350, "нет", "нет", 0]],
        )
        db = FakeDB()
        result = import_rates_xlsx(db, file_obj, 2026, 7)
        self.assertEqual(result["loaded_rows"], 2)
        self.assertEqual(db.commits, 1)
        inserted = [params for sql, params in db.calls if "INSERT INTO point_rates" in sql]
        self.assertTrue(inserted[0]["coffee_enabled"])
        self.assertTrue(inserted[0]["pay_lt5"])
        self.assertEqual(inserted[0]["coffee_rate"], 100)

    def test_rates_invalid_file_writes_nothing(self):
        file_obj = workbook_bytes(["x"] * 7, [["P1", "bad", 1, 1, "да", "нет", 1]])
        db = FakeDB()
        with self.assertRaises(ValueError):
            import_rates_xlsx(db, file_obj, 2026, 7)
        self.assertEqual(db.calls, [])
        self.assertEqual(db.commits, 0)

    def test_merchants_import_preserves_fio_last4_and_tu(self):
        file_obj = workbook_bytes(["ФИО", "Последние 4"], [["Иванов Иван", "0012"], ["Ёлкин Семён", 9876]])
        db = FakeDB()
        result = import_merchants_xlsx(db, file_obj, "ТУ-1")
        self.assertEqual(result["loaded_rows"], 2)
        self.assertEqual(db.commits, 1)
        inserts = [params for sql, params in db.calls if "INSERT INTO merchants" in sql]
        self.assertEqual(inserts[0]["tu"], "ТУ-1")

    def test_merchants_invalid_row_does_not_write(self):
        file_obj = workbook_bytes(["ФИО", "Последние 4"], [["Иванов", "12"]])
        db = FakeDB()
        with self.assertRaises(ValueError):
            import_merchants_xlsx(db, file_obj, "ТУ")
        self.assertEqual(db.calls, [])

    def test_supply_import_large_file_is_batched(self):
        headers = ["Точка"] + list(range(1, 29))
        rows = [[f"P{index:04d}"] + [1] + [None] * 27 for index in range(900)]
        db = FakeDB()
        result = import_supplies_xlsx(db, workbook_bytes(headers, rows))
        self.assertEqual(result, {"loaded_rows": 900, "loaded_points": 900})
        self.assertEqual(db.commits, 1)
        inserts = [(sql, params) for sql, params in db.calls if "INSERT INTO supplies" in sql]
        self.assertEqual(len(inserts), 1)
        self.assertEqual(len(inserts[0][1]), 900)

    def test_production_calendar_workbook(self):
        file_obj = workbook_bytes(
            ["дата", "выходной день", "название", "источник", "комментарий"],
            [
                [date(2026, 6, 12), "да", "Тестовый праздник", "test", "синтетический пример"],
                [date(2026, 8, 1), "нет", "Рабочая суббота", "test", "синтетический пример"],
            ],
        )
        rows = parse_calendar_workbook(file_obj)
        self.assertTrue(rows[0]["is_day_off"])
        self.assertFalse(rows[1]["is_day_off"])
        self.assertEqual(rows[0]["comment"], "синтетический пример")


class ReceiptValidationTests(unittest.TestCase):
    def test_pdf(self):
        validate_receipt("check.pdf", "application/pdf", b"%PDF-1.7 data")

    def test_png(self):
        validate_receipt("check.png", "image/png", b"\x89PNG\r\n\x1a\nrest")

    def test_html_rejected(self):
        with self.assertRaises(ValueError):
            validate_receipt("check.html", "text/html", b"<html>")

    def test_svg_disguised_as_png_rejected(self):
        with self.assertRaises(ValueError):
            validate_receipt("check.png", "image/png", b"<svg></svg>")

    def test_executable_disguised_as_jpeg_rejected(self):
        with self.assertRaises(ValueError):
            validate_receipt("check.jpg", "image/jpeg", b"MZ executable")

    def test_extension_must_match_mime(self):
        with self.assertRaises(ValueError):
            validate_receipt("check.exe", "image/png", b"\x89PNG\r\n\x1a\nrest")

    def test_size_limit(self):
        with self.assertRaises(ValueError):
            validate_receipt("check.pdf", "application/pdf", b"%PDF-" + b"x" * (5 * 1024 * 1024))


def legacy_row(**overrides):
    row = {
        "id": 1,
        "merchant_id": 10,
        "point_code": "P1",
        "month_key": date(2026, 7, 1),
        "note_amount": 0,
        "note_comment": "",
        "reimb_amount": 0,
        "reimb_comment": "",
        "reimb_receipt": "",
    }
    row.update(overrides)
    return row


class LegacyMigrationTests(unittest.TestCase):
    def test_one_note(self):
        plan = build_migration_plan([legacy_row(note_amount=100, note_comment="100 ₽ — note")])
        self.assertEqual(len(plan.notes), 1)

    def test_multiple_notes(self):
        plan = build_migration_plan([legacy_row(note_amount=300, note_comment="100 ₽ — one\n200 ₽ — two")])
        self.assertEqual(len(plan.notes), 2)

    def test_one_reimbursement_and_multiple_receipts(self):
        plan = build_migration_plan([legacy_row(
            reimb_amount=500, reimb_comment="500 ₽ — transport",
            reimb_receipt="receipts/a.pdf|receipts/b.png",
        )])
        self.assertEqual(len(plan.reimbursements), 1)
        self.assertEqual(len(plan.receipts), 2)

    def test_multiple_reimbursements_without_receipts(self):
        plan = build_migration_plan([legacy_row(
            reimb_amount=500, reimb_comment="200 ₽ — one\n300 ₽ — two",
        )])
        self.assertEqual(len(plan.reimbursements), 2)

    def test_multiple_reimbursements_with_paths_is_ambiguous(self):
        plan = build_migration_plan([legacy_row(
            reimb_amount=500, reimb_comment="200 ₽ — one\n300 ₽ — two", reimb_receipt="a|b",
        )])
        self.assertEqual(len(plan.ambiguous), 1)

    def test_empty_values(self):
        plan = build_migration_plan([legacy_row()])
        self.assertEqual(plan.report()["notes_to_migrate"], 0)

    def test_corrupt_line(self):
        plan = build_migration_plan([legacy_row(note_amount=300, note_comment="broken\n200 ₽ — two")])
        self.assertEqual(len(plan.ambiguous), 1)

    def test_data_without_visits_is_still_migrated(self):
        plan = build_migration_plan([legacy_row(note_amount=50, note_comment="50 ₽ — no visits")])
        self.assertEqual(plan.notes[0]["point_code"], "P1")

    def test_partial_migration_is_skipped_idempotently(self):
        first = build_migration_plan([legacy_row(note_amount=100, note_comment="100 ₽ — note")])
        second = build_migration_plan(
            [legacy_row(note_amount=100, note_comment="100 ₽ — note")],
            {first.notes[0]["legacy_key"]},
        )
        self.assertEqual(second.skipped_existing, 1)
        self.assertEqual(second.notes, [])


if __name__ == "__main__":
    unittest.main()
