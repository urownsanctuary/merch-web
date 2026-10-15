"""Add delivery roles/audit and phone; dry-run by default, no historical backfill."""
import argparse
import json

from sqlalchemy import inspect, text


TABLES = {
    "visit_payment_roles": """
        CREATE TABLE visit_payment_roles (
            merchant_id INTEGER NOT NULL,
            point_code TEXT NOT NULL,
            visit_date DATE NOT NULL,
            role TEXT NOT NULL CHECK (role IN ('NO_SUPPLY', 'RECEIVER')),
            visit_id INTEGER NOT NULL,
            event_id TEXT NOT NULL,
            PRIMARY KEY (merchant_id, point_code, visit_date)
        )""",
    "delivery_events": """
        CREATE TABLE delivery_events (
            id TEXT PRIMARY KEY,
            request_key TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            point_code TEXT NOT NULL,
            visit_date DATE NOT NULL,
            first_merchant_id INTEGER,
            second_merchant_id INTEGER NOT NULL,
            first_visit_id INTEGER,
            second_visit_id INTEGER,
            first_fio TEXT NOT NULL DEFAULT '',
            first_phone TEXT NOT NULL DEFAULT '',
            second_fio TEXT NOT NULL DEFAULT '',
            second_phone TEXT NOT NULL DEFAULT '',
            first_rate_before INTEGER,
            first_rate_after INTEGER,
            second_rate INTEGER,
            first_type_before TEXT,
            first_type_after TEXT,
            second_type TEXT,
            initiator_id INTEGER NOT NULL,
            source TEXT NOT NULL DEFAULT 'merchant',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""",
}
INDEXES = {
    "uq_delivery_receiver": "CREATE UNIQUE INDEX uq_delivery_receiver ON visit_payment_roles (point_code, visit_date) WHERE role = 'RECEIVER'",
    "uq_delivery_transfer": "CREATE UNIQUE INDEX uq_delivery_transfer ON delivery_events (point_code, visit_date) WHERE event_type = 'TRANSFER'",
    "ix_delivery_events_period": "CREATE INDEX ix_delivery_events_period ON delivery_events (visit_date, event_type)",
}


def migrate(db, *, apply=False):
    """Caller owns the transaction. Inspect using its connection (also on SQLite)."""
    inspector = inspect(db.connection())
    if not inspector.has_table("merchants"):
        raise ValueError("Existing merchants schema is required")
    statements = []
    if "phone" not in {c["name"] for c in inspector.get_columns("merchants")}:
        statements.append("ALTER TABLE merchants ADD COLUMN phone TEXT")
    for name, ddl in TABLES.items():
        if not inspector.has_table(name):
            statements.append(ddl)
    indexes = {idx["name"] for table in TABLES if inspector.has_table(table)
               for idx in inspector.get_indexes(table)}
    statements.extend(ddl for name, ddl in INDEXES.items() if name not in indexes)
    if apply:
        for sql in statements:
            db.execute(text(sql))
    return {"mode": "apply" if apply else "dry-run", "operations": len(statements),
            "historical_rows_changed": 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--apply", action="store_true")
    group.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    from app.db import SessionLocal
    from app.runtime import maintenance_mode_enabled
    if args.apply and maintenance_mode_enabled():
        parser.error("Apply is disabled in read-only maintenance mode")
    try:
        with SessionLocal() as db:
            report = migrate(db, apply=args.apply)
            if args.apply:
                db.commit()
            else:
                db.rollback()
        print(json.dumps(report))
    except Exception:
        print("Delivery migration failed; inspect target schema securely.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
