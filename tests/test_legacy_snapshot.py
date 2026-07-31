import re
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from app.legacy_snapshot import (
    CONNECTION_RE,
    EMAIL_RE,
    PHONE_RE,
    URL_RE,
    SourceData,
    build_snapshot_sql,
    privacy_findings,
    sanitize_structured_text,
    write_snapshot,
)


class LegacySnapshotTests(unittest.TestCase):
    def source(self):
        now = datetime(2026, 7, 1, tzinfo=timezone.utc)
        return SourceData(
            point_adjustments=[
                {
                    "id": 91,
                    "merchant_id": 8402,
                    "point_code": "ТУ-7 / точка 451",
                    "month_key": date(2026, 7, 1),
                    "note_amount": 300,
                    "note_comment": "100 ₽ — Иван Иванов +7 999 123-45-67\nbroken@example.com | 200 ₽ — note",
                    "reimb_amount": 500,
                    "reimb_comment": "200 ₽ — taxi\n300 ₽ — https://prod.example/secret",
                    "reimb_receipt": "https://prod.example/receipts/real-file/check.pdf|receipts/db-file/photo.jpg",
                }
            ],
            point_notes=[
                {
                    "id": "real-note-id",
                    "merchant_id": 8402,
                    "point_code": "ТУ-7 / точка 451",
                    "month_key": date(2026, 7, 1),
                    "amount": 100,
                    "comment": "Иван Иванов",
                    "legacy_key": "not-a-derived-key",
                    "created_at": now,
                }
            ],
            point_reimbursements=[
                {
                    "id": "real-reimbursement-id",
                    "merchant_id": 8402,
                    "point_code": "ТУ-7 / точка 451",
                    "month_key": date(2026, 7, 1),
                    "amount": 500,
                    "comment": "taxi",
                    "legacy_key": "another-real-key",
                    "created_at": now,
                }
            ],
            reimbursement_receipts=[
                {
                    "id": "real-link-id",
                    "reimbursement_id": "real-reimbursement-id",
                    "legacy_path": "receipts/db-file/photo.jpg",
                    "legacy_key": "real-receipt-key",
                    "created_at": now,
                }
            ],
            receipt_metadata=[
                {
                    "file_id": "db-file",
                    "original_filename": "Иванов-чек.jpg",
                    "content_type": "image/jpeg",
                    "byte_size": 12345,
                    "merchant_id": 8402,
                    "created_at": now,
                }
            ],
            source_columns={"point_adjustments": ["id", "merchant_id"]},
        )

    def test_structured_text_retains_legacy_separators(self):
        value = "100 ₽ — Иванов\nbroken | user@example.com +7 999 123-45-67"
        sanitized = sanitize_structured_text(value)
        self.assertEqual(sanitized.count("\n"), 1)
        self.assertIn("100 ₽ — ", sanitized)
        self.assertIn(" | ", sanitized)
        self.assertNotRegex(sanitized, EMAIL_RE)
        self.assertNotRegex(sanitized, PHONE_RE)
        self.assertNotIn("Иванов", sanitized)

    def test_snapshot_remaps_relations_and_contains_no_private_payload(self):
        snapshot, counts = build_snapshot_sql(self.source())
        self.assertEqual(counts["point_adjustments"], 1)
        self.assertEqual(counts["receipt_metadata"], 1)
        self.assertIn("100001", snapshot)
        self.assertIn("POINT_000001", snapshot)
        self.assertIn("reimbursement_000001", snapshot)
        self.assertIn("receipt_000001", snapshot)
        for private_value in (
            "8402",
            "451",
            "Иван",
            "prod.example",
            "real-file",
            "db-file",
            "real-reimbursement-id",
            "real-note-id",
        ):
            self.assertNotIn(private_value, snapshot)
        self.assertNotRegex(snapshot, CONNECTION_RE)
        self.assertNotRegex(snapshot, EMAIL_RE)
        self.assertNotRegex(snapshot, URL_RE)
        self.assertNotRegex(snapshot, PHONE_RE)
        self.assertNotIn("data BYTEA", snapshot)
        self.assertNotIn("original_filename TEXT", snapshot)

    def test_load_requires_explicit_test_only_psql_gate(self):
        snapshot, _ = build_snapshot_sql(self.source())
        self.assertIn(r"\if :{?LEGACY_SNAPSHOT_TEST_ONLY}", snapshot)
        self.assertIn(r"\if :LEGACY_SNAPSHOT_TEST_ONLY", snapshot)
        self.assertIn("Never load into production", snapshot)

    def test_write_snapshot_reports_sha_counts_and_clean_scan(self):
        output = Path.cwd() / "legacy-migration-snapshot-test.sql"
        try:
            report = write_snapshot(self.source(), output)
            self.assertTrue(output.exists())
            self.assertEqual(len(report["sha256"]), 64)
            self.assertEqual(report["row_counts"]["point_adjustments"], 1)
            self.assertTrue(all(value == 0 for value in report["privacy_scan"].values()))
            self.assertEqual(privacy_findings(output.read_text(encoding="utf-8")), report["privacy_scan"])
        finally:
            output.unlink(missing_ok=True)

    def test_source_module_has_no_mutating_source_sql(self):
        source = Path("app/legacy_snapshot.py").read_text(encoding="utf-8")
        executed_sql = re.findall(r'(?:cursor\.execute|sql\.SQL)\(\s*[rubf]*[\"\']{1,3}(.*?)[\"\']{1,3}', source, re.S)
        forbidden = re.compile(r"\b(?:INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE|CALL)\b", re.I)
        self.assertTrue(executed_sql)
        for statement in executed_sql:
            self.assertNotRegex(statement, forbidden)


if __name__ == "__main__":
    unittest.main()
