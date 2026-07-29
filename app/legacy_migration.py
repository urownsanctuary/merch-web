"""Controlled migration of legacy point_adjustments.

Usage:
    python -m app.legacy_migration --dry-run
    python -m app.legacy_migration --apply
"""

import argparse
import hashlib
import json
import re
from dataclasses import dataclass, field

from sqlalchemy import text

from app.db import SessionLocal


NORMALIZED_DDL = (
    """
    CREATE TABLE IF NOT EXISTS point_notes (
        id TEXT PRIMARY KEY,
        merchant_id INTEGER NOT NULL,
        point_code TEXT NOT NULL,
        month_key DATE NOT NULL,
        amount INTEGER NOT NULL CHECK (amount <> 0),
        comment TEXT NOT NULL CHECK (length(trim(comment)) > 0),
        legacy_key TEXT UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS point_reimbursements (
        id TEXT PRIMARY KEY,
        merchant_id INTEGER NOT NULL,
        point_code TEXT NOT NULL,
        month_key DATE NOT NULL,
        amount INTEGER NOT NULL CHECK (amount > 0),
        comment TEXT NOT NULL CHECK (length(trim(comment)) > 0),
        legacy_key TEXT UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reimbursement_receipts (
        id TEXT PRIMARY KEY,
        reimbursement_id TEXT NOT NULL REFERENCES point_reimbursements(id) ON DELETE CASCADE,
        legacy_path TEXT NOT NULL,
        legacy_key TEXT UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
)


@dataclass
class MigrationPlan:
    found: int = 0
    notes: list[dict] = field(default_factory=list)
    reimbursements: list[dict] = field(default_factory=list)
    receipts: list[dict] = field(default_factory=list)
    ambiguous: list[dict] = field(default_factory=list)
    skipped_existing: int = 0

    def report(self) -> dict:
        return {
            "found_legacy_rows": self.found,
            "notes_to_migrate": len(self.notes),
            "reimbursements_to_migrate": len(self.reimbursements),
            "receipts_to_migrate": len(self.receipts),
            "ambiguous_records": len(self.ambiguous),
            "skipped_existing": self.skipped_existing,
            "ambiguous": self.ambiguous,
        }


LINE_RE = re.compile(r"^\s*([+-]?\d+)\s*(?:₽)?\s*(?:[—–-]\s*)?(.*)$")


def _key(row_id: object, kind: str, index: int, payload: str) -> str:
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"point_adjustments:{row_id}:{kind}:{index}:{digest}"


def _items(row: dict, kind: str, total_field: str, comment_field: str, positive: bool) -> tuple[list[dict], list[str]]:
    raw = str(row.get(comment_field) or "").strip()
    total = int(row.get(total_field) or 0)
    if not raw and total == 0:
        return [], []
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    errors = []
    result = []
    if not lines:
        return [], [f"{kind}: сумма {total} без комментария"]
    for index, line in enumerate(lines):
        match = LINE_RE.match(line)
        if match and match.group(2).strip():
            amount, comment = int(match.group(1)), match.group(2).strip()
        elif len(lines) == 1 and total:
            amount, comment = total, line
        else:
            errors.append(f"{kind} строка {index + 1} не содержит однозначной суммы: {line!r}")
            continue
        if amount == 0 or (positive and amount < 0):
            errors.append(f"{kind} строка {index + 1} содержит недопустимую сумму")
            continue
        legacy_key = _key(row["id"], kind, index, line)
        result.append({
            "id": legacy_key,
            "merchant_id": row["merchant_id"],
            "point_code": row["point_code"],
            "month_key": row["month_key"],
            "amount": amount,
            "comment": comment,
            "legacy_key": legacy_key,
        })
    if result and sum(item["amount"] for item in result) != total:
        errors.append(f"{kind}: сумма строк {sum(item['amount'] for item in result)} не равна итогу {total}")
    return result, errors


def build_migration_plan(rows: list[dict], existing_keys: set[str] | None = None) -> MigrationPlan:
    existing_keys = existing_keys or set()
    plan = MigrationPlan(found=len(rows))
    for row in rows:
        notes, note_errors = _items(row, "note", "note_amount", "note_comment", False)
        reimbursements, reimb_errors = _items(row, "reimbursement", "reimb_amount", "reimb_comment", True)
        paths = [part.strip() for part in str(row.get("reimb_receipt") or "").split("|") if part.strip()]
        errors = note_errors + reimb_errors
        if paths and len(reimbursements) != 1:
            errors.append("receipt paths cannot be assigned unambiguously to a single reimbursement")
        if errors:
            plan.ambiguous.append({"legacy_id": row.get("id"), "errors": errors})
            continue
        for collection, items in ((plan.notes, notes), (plan.reimbursements, reimbursements)):
            for item in items:
                if item["legacy_key"] in existing_keys:
                    plan.skipped_existing += 1
                else:
                    collection.append(item)
        if paths and reimbursements:
            reimbursement_key = reimbursements[0]["legacy_key"]
            for index, path in enumerate(paths):
                receipt_key = _key(row["id"], "receipt", index, path)
                if receipt_key in existing_keys:
                    plan.skipped_existing += 1
                else:
                    plan.receipts.append({
                        "id": receipt_key,
                        "reimbursement_legacy_key": reimbursement_key,
                        "legacy_path": path,
                        "legacy_key": receipt_key,
                    })
    return plan


def run_migration(*, apply: bool) -> dict:
    db = SessionLocal()
    try:
        rows = [dict(row) for row in db.execute(text("SELECT * FROM point_adjustments ORDER BY id")).mappings().all()]
        existing_keys: set[str] = set()
        if apply:
            for ddl in NORMALIZED_DDL:
                db.execute(text(ddl))
            key_rows = db.execute(text("""
                SELECT legacy_key FROM point_notes WHERE legacy_key IS NOT NULL
                UNION SELECT legacy_key FROM point_reimbursements WHERE legacy_key IS NOT NULL
                UNION SELECT legacy_key FROM reimbursement_receipts WHERE legacy_key IS NOT NULL
            """)).all()
            existing_keys = {row[0] for row in key_rows}
        plan = build_migration_plan(rows, existing_keys)
        if apply:
            for note in plan.notes:
                db.execute(text("""
                    INSERT INTO point_notes
                        (id, merchant_id, point_code, month_key, amount, comment, legacy_key)
                    VALUES (:id, :merchant_id, :point_code, :month_key, :amount, :comment, :legacy_key)
                    ON CONFLICT (legacy_key) DO NOTHING
                """), note)
            for reimbursement in plan.reimbursements:
                db.execute(text("""
                    INSERT INTO point_reimbursements
                        (id, merchant_id, point_code, month_key, amount, comment, legacy_key)
                    VALUES (:id, :merchant_id, :point_code, :month_key, :amount, :comment, :legacy_key)
                    ON CONFLICT (legacy_key) DO NOTHING
                """), reimbursement)
            for receipt in plan.receipts:
                db.execute(text("""
                    INSERT INTO reimbursement_receipts (id, reimbursement_id, legacy_path, legacy_key)
                    SELECT :id, id, :legacy_path, :legacy_key FROM point_reimbursements
                    WHERE legacy_key=:reimbursement_legacy_key
                    ON CONFLICT (legacy_key) DO NOTHING
                """), receipt)
            db.commit()
        else:
            db.rollback()
        report = plan.report()
        report["mode"] = "apply" if apply else "dry-run"
        return report
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate legacy point_adjustments without deleting legacy data")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run_migration(apply=args.apply), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
