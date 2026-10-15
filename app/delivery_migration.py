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
            can_transfer BOOLEAN NOT NULL DEFAULT FALSE,
            restore_merchant_id INTEGER,
            restore_visit_id INTEGER,
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
    "ix_delivery_roles_day": "CREATE INDEX ix_delivery_roles_day ON visit_payment_roles (point_code, visit_date)",
    "ix_delivery_events_period": "CREATE INDEX ix_delivery_events_period ON delivery_events (visit_date, event_type)",
}


def migrate(db, *, apply=False):
    """Caller owns the transaction. Inspect using its connection (also on SQLite)."""
    inspector = inspect(db.connection())
    if not all(inspector.has_table(t) for t in ('merchants', 'visits')):
        raise ValueError("Existing merchants and visits schema is required")
    statements = []
    if "phone" not in {c["name"] for c in inspector.get_columns("merchants")}:
        statements.append("ALTER TABLE merchants ADD COLUMN phone TEXT")
    for name, ddl in TABLES.items():
        if not inspector.has_table(name):
            statements.append(ddl)
    if inspector.has_table('visit_payment_roles'):
        columns = {c['name'] for c in inspector.get_columns('visit_payment_roles')}
        if not {'can_transfer', 'restore_merchant_id', 'restore_visit_id'}.issubset(columns):
            if db.execute(text('SELECT COUNT(*) FROM visit_payment_roles')).scalar():
                raise ValueError('Unreleased role data requires explicit review; no automatic state backfill')
        for name, definition in [('can_transfer', 'BOOLEAN NOT NULL DEFAULT FALSE'), ('restore_merchant_id', 'INTEGER'), ('restore_visit_id', 'INTEGER')]:
            if name not in columns:
                statements.append(f'ALTER TABLE visit_payment_roles ADD COLUMN {name} {definition}')
    indexes = {idx["name"] for table in TABLES if inspector.has_table(table)
               for idx in inspector.get_indexes(table)}
    statements.extend(ddl for name, ddl in INDEXES.items() if name not in indexes)
    # Only the superseded, unreleased PR index: audit must allow a new transfer
    # after a complete attendance lifecycle. Request keys remain unique.
    if 'uq_delivery_transfer' in indexes:
        statements.append('DROP INDEX uq_delivery_transfer')
    if db.get_bind().dialect.name == 'postgresql':
        visit_indexes = {i['name'] for i in inspector.get_indexes('visits')}
        if 'uq_delivery_visit_identity' not in visit_indexes:
            statements.append('CREATE UNIQUE INDEX uq_delivery_visit_identity ON visits (id, merchant_id, point_code, visit_date)')
        foreign_keys = inspector.get_foreign_keys('visit_payment_roles') if inspector.has_table('visit_payment_roles') else []
        if not any(fk['name'] == 'fk_delivery_active_visit' for fk in foreign_keys):
            statements.append('''ALTER TABLE visit_payment_roles ADD CONSTRAINT fk_delivery_active_visit
                FOREIGN KEY (visit_id, merchant_id, point_code, visit_date)
                REFERENCES visits (id, merchant_id, point_code, visit_date) ON DELETE CASCADE''')
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
