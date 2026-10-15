"""Add delivery roles/audit and phone; dry-run by default, no historical backfill."""
import argparse
import json
import re

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


class PreflightError(ValueError):
    """Only fixed problem codes and aggregate counts may cross the CLI boundary."""

    def __init__(self, problem, count=1):
        self.problem, self.count = problem, int(count)
        super().__init__(f"Delivery preflight failed: {problem}; conflicts={self.count}")


def _sql_key(value):
    # Ignore formatting/casts outside literals, never whitespace *inside* a
    # literal: role='RE CEIVER' must not match role='RECEIVER'.
    return re.sub(r"'(?:''|[^'])*'|::text|[\s()\[\]\"]",
                  lambda m: m[0] if m[0].startswith("'") else '', str(value).lower())


def _default_key(value):
    value = str(value).strip().lower()
    while value.startswith('(') and value.endswith(')'):
        value = value[1:-1].strip()
    return re.sub(r'::text$', '', value)


def _require_columns(inspector, table, specification):
    columns = {c['name']: c for c in inspector.get_columns(table)}
    for name, (kind, nullable) in specification.items():
        column = columns.get(name)
        if column is None:
            raise PreflightError('missing_required_column')
        if str(column['type']).upper() != kind:
            raise PreflightError('incompatible_column_type')
        # SQLite reports an INTEGER/TEXT PRIMARY KEY as nullable; its key
        # values are checked explicitly below. PostgreSQL enforces NOT NULL.
        pk = inspector.get_pk_constraint(table).get('constrained_columns') or []
        if column['nullable'] != nullable and not (
                inspector.bind.dialect.name == 'sqlite' and name in pk):
            raise PreflightError('incompatible_column_nullability')
    if any(not c['nullable'] and c.get('default') is None and name not in specification
           for name, c in columns.items()):
        raise PreflightError('unexpected_required_column')
    return columns


ROLE_COLUMNS = {
    'merchant_id': ('INTEGER', False), 'point_code': ('TEXT', False),
    'visit_date': ('DATE', False), 'role': ('TEXT', False),
    'visit_id': ('INTEGER', False), 'event_id': ('TEXT', False),
    'can_transfer': ('BOOLEAN', False), 'restore_merchant_id': ('INTEGER', True),
    'restore_visit_id': ('INTEGER', True),
}
EVENT_COLUMNS = {
    **{n: ('TEXT', False) for n in ('id', 'request_key', 'event_type', 'point_code',
        'first_fio', 'first_phone', 'second_fio', 'second_phone', 'source')},
    'visit_date': ('DATE', False), 'created_at': ('TIMESTAMP', False),
    **{n: ('INTEGER', False) for n in ('second_merchant_id', 'initiator_id')},
    **{n: ('INTEGER', True) for n in ('first_merchant_id', 'first_visit_id',
        'second_visit_id', 'first_rate_before', 'first_rate_after', 'second_rate')},
    **{n: ('TEXT', True) for n in ('first_type_before', 'first_type_after', 'second_type')},
}


def _conflicts(db, problem, query):
    count = db.execute(text('SELECT COUNT(*) FROM (' + query + ') conflicts')).scalar()
    if count:
        raise PreflightError(problem, count)


def preflight(db, inspector):
    """Validate before *any* DDL, including reapply. Never repair payment data."""
    for table, required in (
        ('merchants', {'id': 'INTEGER'}),
        ('visits', {'id': 'INTEGER', 'merchant_id': 'INTEGER', 'point_code': 'TEXT',
                    'visit_date': 'DATE', 'slot': 'TEXT'}),
    ):
        columns = {c['name']: c for c in inspector.get_columns(table)}
        if any(n not in columns or str(columns[n]['type']).upper() != kind
               for n, kind in required.items()):
            raise PreflightError('incompatible_legacy_schema')
    phone = {c['name']: c for c in inspector.get_columns('merchants')}.get('phone')
    if phone and (str(phone['type']).upper() != 'TEXT' or not phone['nullable']):
        raise PreflightError('incompatible_phone_type')
    for table, specification, key in (
        ('visit_payment_roles', ROLE_COLUMNS, ['merchant_id', 'point_code', 'visit_date']),
        ('delivery_events', EVENT_COLUMNS, ['id']),
    ):
        if not inspector.has_table(table):
            continue
        columns = _require_columns(inspector, table, specification)
        defaults = ({'can_transfer': {'false', '0'}} if table == 'visit_payment_roles' else {
            **{n: {"''"} for n in ('first_fio', 'first_phone', 'second_fio', 'second_phone')},
            'source': {"'merchant'"}, 'created_at': {'current_timestamp', 'now()'},
        })
        if any(columns[n].get('default') is None or _default_key(columns[n]['default']) not in accepted
               for n, accepted in defaults.items()):
            raise PreflightError('incompatible_column_default')
        # Detect conflicts even when a partially installed schema lacks keys.
        _conflicts(db, 'duplicate_' + table + '_identity',
                   f"SELECT 1 FROM {table} GROUP BY {', '.join(key)} HAVING COUNT(*)>1")
        required = [n for n, (_, nullable) in specification.items() if not nullable]
        _conflicts(db, 'null_' + table + '_value',
                   f"SELECT 1 FROM {table} WHERE " + ' OR '.join(n + ' IS NULL' for n in required))
        if table == 'delivery_events':
            _conflicts(db, 'duplicate_request_key',
                       'SELECT 1 FROM delivery_events GROUP BY request_key HAVING COUNT(*)>1')
        else:
            _conflicts(db, 'duplicate_receiver', """SELECT 1 FROM visit_payment_roles
                WHERE role='RECEIVER' GROUP BY point_code,visit_date HAVING COUNT(*)>1""")
            _conflicts(db, 'invalid_role',
                       "SELECT 1 FROM visit_payment_roles WHERE role NOT IN ('NO_SUPPLY','RECEIVER')")
            _conflicts(db, 'orphan_active_role', """SELECT 1 FROM visit_payment_roles r
                WHERE NOT EXISTS (SELECT 1 FROM visits v WHERE v.id=r.visit_id
                  AND v.merchant_id=r.merchant_id AND v.point_code=r.point_code
                  AND v.visit_date=r.visit_date AND v.slot IN ('DAY','MORNING'))""")
            _conflicts(db, 'invalid_restore_target', """SELECT 1 FROM visit_payment_roles r
                WHERE (restore_visit_id IS NULL AND restore_merchant_id IS NOT NULL)
                   OR (restore_visit_id IS NOT NULL AND (restore_merchant_id IS NULL
                     OR NOT EXISTS (SELECT 1 FROM visits v WHERE v.id=r.restore_visit_id
                       AND v.merchant_id=r.restore_merchant_id AND v.point_code=r.point_code
                       AND v.visit_date=r.visit_date AND v.slot IN ('DAY','MORNING'))))""")
            checks = {_sql_key(c['sqltext']) for c in inspector.get_check_constraints(table)}
            accepted_checks = {"rolein'no_supply','receiver'", "role=anyarray'no_supply','receiver'"}
            if not checks or not checks.issubset(accepted_checks):
                raise PreflightError('missing_or_incompatible_role_check')
        if table == 'delivery_events' and inspector.get_check_constraints(table):
            raise PreflightError('unexpected_event_check_constraint')
        if inspector.get_pk_constraint(table).get('constrained_columns') != key:
            raise PreflightError('missing_or_incompatible_primary_key')
        if table == 'delivery_events':
            unique = inspector.get_unique_constraints(table)
            if not any(c['column_names'] == ['request_key'] for c in unique):
                raise PreflightError('missing_request_key_unique_constraint')
            if any(c['column_names'] != ['request_key'] for c in unique):
                raise PreflightError('unexpected_event_unique_constraint')
        elif inspector.get_unique_constraints(table):
            raise PreflightError('unexpected_role_unique_constraint')
        for index in inspector.get_indexes(table):
            if not index['unique']:
                continue
            if table == 'visit_payment_roles' and index['name'] == 'uq_delivery_receiver':
                continue  # Its exact definition is checked by the planner.
            if table == 'delivery_events' and index.get('duplicates_constraint'):
                continue  # Already checked above.
            if (table == 'delivery_events' and index['name'] == 'uq_delivery_transfer'
                    and index['column_names'] == ['point_code', 'visit_date']):
                _validate_index(inspector, table, index['name'], ['point_code', 'visit_date'],
                                True, "event_type='TRANSFER'")
                continue  # Known superseded unreleased index, removed below.
            raise PreflightError('unexpected_unique_index')


def _validate_index(inspector, table, name, columns, unique=False, predicate=None):
    indexes = {i['name']: i for i in inspector.get_indexes(table)}
    index = indexes.get(name)
    if index is None:
        return False
    if inspector.bind.dialect.name == 'postgresql':
        valid = inspector.bind.execute(text('''SELECT i.indisvalid AND i.indisready
            FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            WHERE i.indrelid=to_regclass(:table) AND c.relname=:name'''),
            {'table': table, 'name': name}).scalar()
        if not valid:
            raise PreflightError('invalid_index_state')
    where = index.get('dialect_options', {}).get(inspector.bind.dialect.name + '_where')
    if (index['column_names'] != columns or bool(index['unique']) != unique
            or _sql_key(where if where is not None else '') != _sql_key(predicate or '')):
        raise PreflightError('incompatible_index_definition')
    return True


def migrate(db, *, apply=False):
    """Caller owns the transaction. Inspect using its connection (also on SQLite)."""
    inspector = inspect(db.connection())
    if not all(inspector.has_table(t) for t in ('merchants', 'visits')):
        raise ValueError("Existing merchants and visits schema is required")
    if db.get_bind().dialect.name == 'postgresql':
        # Hold the checked state through DDL/commit, also on reapply. Fail
        # boundedly rather than race live writers or wait indefinitely.
        db.execute(text("SET LOCAL lock_timeout = '5s'"))
        db.execute(text('LOCK TABLE merchants, visits IN SHARE ROW EXCLUSIVE MODE'))
        inspector = inspect(db.connection())
        tables = [t for t in TABLES if inspector.has_table(t)]
        if tables:
            db.execute(text('LOCK TABLE ' + ', '.join(tables) + ' IN SHARE ROW EXCLUSIVE MODE'))
        inspector = inspect(db.connection())
    preflight(db, inspector)
    statements = []
    if "phone" not in {c["name"] for c in inspector.get_columns("merchants")}:
        statements.append("ALTER TABLE merchants ADD COLUMN phone TEXT")
    for name, ddl in TABLES.items():
        if not inspector.has_table(name):
            statements.append(ddl)
    indexes = {idx["name"] for table in TABLES if inspector.has_table(table)
               for idx in inspector.get_indexes(table)}
    statements.extend(ddl for name, ddl in INDEXES.items() if name not in indexes)
    for table, name, columns, unique, predicate in (
        ('visit_payment_roles', 'uq_delivery_receiver', ['point_code', 'visit_date'], True, "role='RECEIVER'"),
        ('visit_payment_roles', 'ix_delivery_roles_day', ['point_code', 'visit_date'], False, None),
        ('delivery_events', 'ix_delivery_events_period', ['visit_date', 'event_type'], False, None),
    ):
        if inspector.has_table(table):
            _validate_index(inspector, table, name, columns, unique, predicate)
    # Only the superseded, unreleased PR index: audit must allow a new transfer
    # after a complete attendance lifecycle. Request keys remain unique.
    if 'uq_delivery_transfer' in indexes:
        statements.append('DROP INDEX uq_delivery_transfer')
    if db.get_bind().dialect.name == 'postgresql':
        _conflicts(db, 'duplicate_visit_identity', '''SELECT 1 FROM visits
            GROUP BY id,merchant_id,point_code,visit_date HAVING COUNT(*)>1''')
        if not _validate_index(inspector, 'visits', 'uq_delivery_visit_identity',
                               ['id', 'merchant_id', 'point_code', 'visit_date'], True):
            statements.append('CREATE UNIQUE INDEX uq_delivery_visit_identity ON visits (id, merchant_id, point_code, visit_date)')
        foreign_keys = inspector.get_foreign_keys('visit_payment_roles') if inspector.has_table('visit_payment_roles') else []
        for fk in foreign_keys:
            if fk['name'] == 'fk_delivery_active_visit' and (
                fk['constrained_columns'] != ['visit_id', 'merchant_id', 'point_code', 'visit_date']
                or fk['referred_table'] != 'visits'
                or fk['referred_schema'] not in (None, inspector.default_schema_name)
                or fk['referred_columns'] != ['id', 'merchant_id', 'point_code', 'visit_date']
                or fk.get('options', {}).get('ondelete', '').upper() != 'CASCADE'
            ):
                raise PreflightError('incompatible_active_visit_foreign_key')
        if not any(fk['name'] == 'fk_delivery_active_visit' for fk in foreign_keys):
            statements.append('''ALTER TABLE visit_payment_roles ADD CONSTRAINT fk_delivery_active_visit
                FOREIGN KEY (visit_id, merchant_id, point_code, visit_date)
                REFERENCES visits (id, merchant_id, point_code, visit_date) ON DELETE CASCADE''')
    if apply:
        for sql in statements:
            db.execute(text(sql))
    return {"mode": "apply" if apply else "dry-run", "operations": len(statements),
            "historical_rows_changed": 0, "preflight": "PASS", "planned_statements": statements}


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
    except PreflightError as exc:
        print(str(exc))
        return 1
    except Exception:
        print("Delivery migration failed; inspect target schema securely.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
