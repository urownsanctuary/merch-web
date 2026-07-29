"""Additive, idempotent PostgreSQL schema migration.

The legacy ``point_adjustments`` column/table is intentionally retained. New
writes use normalized rows with stable IDs. Backfill is conservative: JSON
arrays are copied only when the legacy table exposes the expected identifying
columns, and malformed legacy values are left untouched for manual review.
"""

import json
from decimal import Decimal, InvalidOperation

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


DDL = (
    """
    CREATE TABLE IF NOT EXISTS reconciliation_submissions (
        id BIGSERIAL PRIMARY KEY,
        merchant_id BIGINT NOT NULL REFERENCES merchants(id),
        year INTEGER NOT NULL,
        month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
        submitted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        reopened_at TIMESTAMPTZ,
        UNIQUE (merchant_id, year, month)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS point_notes (
        id BIGSERIAL PRIMARY KEY,
        merchant_id BIGINT NOT NULL REFERENCES merchants(id),
        point_code TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
        amount NUMERIC(12,2) NOT NULL CHECK (amount <> 0),
        comment TEXT NOT NULL CHECK (length(trim(comment)) > 0),
        kind TEXT NOT NULL DEFAULT 'regular',
        adjustment_date DATE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (merchant_id, point_code, adjustment_date, kind)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS point_reimbursements (
        id BIGSERIAL PRIMARY KEY,
        merchant_id BIGINT NOT NULL REFERENCES merchants(id),
        point_code TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
        amount NUMERIC(12,2) NOT NULL CHECK (amount > 0),
        comment TEXT NOT NULL CHECK (length(trim(comment)) > 0),
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reimbursement_receipts (
        id BIGSERIAL PRIMARY KEY,
        reimbursement_id BIGINT NOT NULL REFERENCES point_reimbursements(id) ON DELETE CASCADE,
        original_name TEXT NOT NULL,
        content_type TEXT NOT NULL,
        byte_size INTEGER NOT NULL CHECK (byte_size > 0),
        sha256 TEXT NOT NULL,
        content BYTEA NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS special_inventory_dates (
        inventory_date DATE PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_visits_merchant_date_point ON visits (merchant_id, visit_date, point_code)",
    "CREATE INDEX IF NOT EXISTS ix_supplies_point_date ON supplies (point_code, supply_date)",
    "CREATE INDEX IF NOT EXISTS ix_notes_owner_period_point ON point_notes (merchant_id, year, month, point_code)",
    "CREATE INDEX IF NOT EXISTS ix_reimbursements_owner_period_point ON point_reimbursements (merchant_id, year, month, point_code)",
    "CREATE INDEX IF NOT EXISTS ix_receipts_reimbursement ON reimbursement_receipts (reimbursement_id)",
)


def migrate(engine: Engine) -> None:
    if engine.dialect.name != "postgresql":
        return
    with engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('merch_web_schema_migration'))"))
        for statement in DDL:
            conn.execute(text(statement))
    _backfill_legacy_adjustments(engine)


def _backfill_legacy_adjustments(engine: Engine) -> None:
    columns = {c["name"] for c in inspect(engine).get_columns("point_adjustments")} if inspect(engine).has_table("point_adjustments") else set()
    required = {"merchant_id", "point_code", "year", "month", "notes", "reimbursements"}
    if not required.issubset(columns):
        return
    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT merchant_id, point_code, year, month, notes, reimbursements FROM point_adjustments"
        )).mappings()
        for row in rows:
            _backfill_notes(conn, row, _json_list(row["notes"]))
            _backfill_reimbursements(conn, row, _json_list(row["reimbursements"]))


def _json_list(value: object) -> list[dict]:
    if not value:
        return []
    try:
        parsed = value if isinstance(value, list) else json.loads(str(value))
        return [item for item in parsed if isinstance(item, dict)]
    except (TypeError, ValueError, json.JSONDecodeError):
        return []


def _amount(value: object, positive: bool) -> Decimal | None:
    try:
        amount = Decimal(str(value))
        return amount if (amount > 0 if positive else amount != 0) else None
    except (InvalidOperation, TypeError):
        return None


def _backfill_notes(conn, owner: dict, items: list[dict]) -> None:
    for item in items:
        amount, comment = _amount(item.get("amount"), False), str(item.get("comment", "")).strip()
        if amount is None or not comment:
            continue
        conn.execute(text("""
            INSERT INTO point_notes (merchant_id, point_code, year, month, amount, comment, kind)
            SELECT :merchant_id, :point_code, :year, :month, :amount, :comment, 'legacy'
            WHERE NOT EXISTS (
                SELECT 1 FROM point_notes WHERE merchant_id=:merchant_id AND point_code=:point_code
                AND year=:year AND month=:month AND amount=:amount AND comment=:comment
            )
        """), {**owner, "amount": amount, "comment": comment})


def _backfill_reimbursements(conn, owner: dict, items: list[dict]) -> None:
    for item in items:
        amount, comment = _amount(item.get("amount"), True), str(item.get("comment", "")).strip()
        if amount is None or not comment:
            continue
        conn.execute(text("""
            INSERT INTO point_reimbursements (merchant_id, point_code, year, month, amount, comment)
            SELECT :merchant_id, :point_code, :year, :month, :amount, :comment
            WHERE NOT EXISTS (
                SELECT 1 FROM point_reimbursements WHERE merchant_id=:merchant_id AND point_code=:point_code
                AND year=:year AND month=:month AND amount=:amount AND comment=:comment
            )
        """), {**owner, "amount": amount, "comment": comment})
