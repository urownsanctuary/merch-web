"""Preflight must reject invalid state before emitting DDL or changing data."""
import os
import unittest
import uuid
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from app import delivery_migration as migration


class PreflightTests(unittest.TestCase):
    postgres = False

    def setUp(self):
        if self.postgres:
            url = os.environ['TEST_DELIVERY_POSTGRES_URL']
            if make_url(url).host not in {'localhost', '127.0.0.1'}:
                raise ValueError('Disposable localhost PostgreSQL required')
            schema = 'preflight_test_' + uuid.uuid4().hex
            bootstrap = create_engine(url)
            with bootstrap.begin() as conn:
                conn.exec_driver_sql('CREATE SCHEMA ' + schema)
            bootstrap.dispose()
            self.engine = create_engine(url, connect_args={'options': '-csearch_path=' + schema})
        else:
            self.engine = create_engine('sqlite+pysqlite:///:memory:')
        self.factory = sessionmaker(bind=self.engine)
        with self.engine.begin() as conn:
            conn.exec_driver_sql('CREATE TABLE merchants(id INTEGER PRIMARY KEY)')
            conn.exec_driver_sql('''CREATE TABLE visits(id INTEGER PRIMARY KEY,merchant_id INTEGER,
                point_code TEXT,visit_date DATE,slot TEXT)''')
            conn.exec_driver_sql("INSERT INTO visits VALUES(1,1,'SYNTHETIC','2026-07-02','DAY'),(2,2,'SYNTHETIC','2026-07-02','DAY')")

    def tearDown(self):
        self.engine.dispose()

    def execute(self, sql):
        with self.engine.begin() as conn:
            conn.exec_driver_sql(sql)

    def install(self):
        with self.factory() as db:
            migration.migrate(db, apply=True)
            db.commit()

    def snapshot(self):
        def stable(value):
            if isinstance(value, dict):
                return {k: stable(v) for k, v in value.items()}
            if isinstance(value, (tuple, list)):
                return [stable(v) for v in value]
            return value if isinstance(value, (str, int, bool, type(None))) else str(value)
        with self.engine.connect() as conn:
            inspector = inspect(conn)
            return {
                table: (stable(inspector.get_columns(table)),
                        inspector.get_pk_constraint(table),
                        inspector.get_unique_constraints(table),
                        stable(inspector.get_indexes(table)),
                        inspector.get_check_constraints(table),
                        inspector.get_foreign_keys(table),
                        [tuple(row) for row in conn.execute(text('SELECT * FROM ' + table))])
                for table in sorted(inspector.get_table_names())
            }

    def assert_rejected_unchanged(self, problem):
        before = self.snapshot()
        for apply in (False, True):
            statements = []
            def record(conn, cursor, statement, parameters, context, many):
                statements.append(statement.lstrip().split()[0].upper())
            event.listen(self.engine, 'before_cursor_execute', record)
            try:
                with self.factory() as db:
                    with self.assertRaises(migration.PreflightError) as error:
                        migration.migrate(db, apply=apply)
                    self.assertEqual(error.exception.problem, problem)
                    self.assertGreater(error.exception.count, 0)
                    self.assertNotIn('SYNTHETIC', str(error.exception))
                    # Even a caller that commits after catching the error must
                    # find no partial DDL/DML to commit.
                    db.commit()
            finally:
                event.remove(self.engine, 'before_cursor_execute', record)
            self.assertFalse(set(statements) & {'CREATE', 'ALTER', 'DROP', 'INSERT', 'UPDATE', 'DELETE'})
            self.assertEqual(self.snapshot(), before)

    def test_clean_legacy_dry_apply_reapply(self):
        before = self.snapshot()
        with self.factory() as db:
            report = migration.migrate(db)
            self.assertEqual(report['preflight'], 'PASS')
            self.assertEqual(report['operations'], len(report['planned_statements']))
            db.rollback()
        self.assertEqual(before, self.snapshot())
        self.install()
        with self.factory() as db:
            self.assertEqual(migration.migrate(db)['operations'], 0)
            self.assertEqual(migration.migrate(db, apply=True)['operations'], 0)
            db.commit()

    def test_partial_missing_column_fails_before_phone_or_other_ddl(self):
        self.execute(migration.TABLES['delivery_events'].replace('event_type TEXT NOT NULL,', ''))
        self.assert_rejected_unchanged('missing_required_column')

    def test_partial_one_complete_table_can_be_completed(self):
        self.execute(migration.TABLES['delivery_events'])
        self.install()
        with self.factory() as db:
            self.assertEqual(migration.migrate(db)['operations'], 0)

    def test_missing_index_is_completed(self):
        self.install()
        self.execute('DROP INDEX uq_delivery_receiver')
        with self.factory() as db:
            self.assertEqual(migration.migrate(db)['operations'], 1)
        self.install()
        with self.factory() as db:
            self.assertEqual(migration.migrate(db)['operations'], 0)

    def test_wrong_named_index_fails(self):
        self.install()
        self.execute('DROP INDEX uq_delivery_receiver')
        self.execute('CREATE UNIQUE INDEX uq_delivery_receiver ON visit_payment_roles(merchant_id)')
        self.assert_rejected_unchanged('incompatible_index_definition')

    def test_index_literal_whitespace_is_not_ignored(self):
        self.install()
        self.execute('DROP INDEX uq_delivery_receiver')
        self.execute("CREATE UNIQUE INDEX uq_delivery_receiver ON visit_payment_roles(point_code,visit_date) WHERE role='RE CEIVER'")
        self.assert_rejected_unchanged('incompatible_index_definition')

    def test_index_literal_case_is_not_ignored(self):
        self.install()
        self.execute('DROP INDEX uq_delivery_receiver')
        self.execute("CREATE UNIQUE INDEX uq_delivery_receiver ON visit_payment_roles(point_code,visit_date) WHERE role='receiver'")
        self.assert_rejected_unchanged('incompatible_index_definition')

    def test_incompatible_phone_type_fails(self):
        self.execute('ALTER TABLE merchants ADD COLUMN phone INTEGER')
        self.assert_rejected_unchanged('incompatible_phone_type')

    def test_incompatible_role_column_type_fails(self):
        self.execute(migration.TABLES['visit_payment_roles'].replace('event_id TEXT', 'event_id INTEGER'))
        self.assert_rejected_unchanged('incompatible_column_type')

    def test_missing_role_check_fails(self):
        self.execute(migration.TABLES['visit_payment_roles'].replace(" CHECK (role IN ('NO_SUPPLY', 'RECEIVER'))", ''))
        self.assert_rejected_unchanged('missing_or_incompatible_role_check')

    def test_missing_request_unique_fails(self):
        self.execute(migration.TABLES['delivery_events'].replace('request_key TEXT NOT NULL UNIQUE', 'request_key TEXT NOT NULL'))
        self.assert_rejected_unchanged('missing_request_key_unique_constraint')

    def test_duplicate_receivers_fail_before_ddl(self):
        self.install()
        self.execute('DROP INDEX uq_delivery_receiver')
        self.execute("""INSERT INTO visit_payment_roles(merchant_id,point_code,visit_date,role,visit_id,event_id)
            VALUES(1,'SYNTHETIC','2026-07-02','RECEIVER',1,'a'),(2,'SYNTHETIC','2026-07-02','RECEIVER',2,'b')""")
        self.assert_rejected_unchanged('duplicate_receiver')

    def test_duplicate_role_identity_fails(self):
        self.execute(migration.TABLES['visit_payment_roles'].replace(
            ',\n            PRIMARY KEY (merchant_id, point_code, visit_date)', ''))
        self.execute("""INSERT INTO visit_payment_roles(merchant_id,point_code,visit_date,role,visit_id,event_id)
            VALUES(1,'SYNTHETIC','2026-07-02','NO_SUPPLY',1,'a'),(1,'SYNTHETIC','2026-07-02','NO_SUPPLY',1,'b')""")
        self.assert_rejected_unchanged('duplicate_visit_payment_roles_identity')

    def test_unexpected_unique_index_fails(self):
        self.install()
        self.execute('CREATE UNIQUE INDEX accidental_one_event_per_day ON delivery_events(visit_date)')
        self.assert_rejected_unchanged('unexpected_unique_index')

    def test_missing_role_primary_key_fails(self):
        self.execute(migration.TABLES['visit_payment_roles'].replace(
            ',\n            PRIMARY KEY (merchant_id, point_code, visit_date)', ''))
        self.assert_rejected_unchanged('missing_or_incompatible_primary_key')

    def test_invalid_restore_target_fails(self):
        self.install()
        self.execute("""INSERT INTO visit_payment_roles(merchant_id,point_code,visit_date,role,visit_id,event_id,
            restore_merchant_id,restore_visit_id)
            VALUES(1,'SYNTHETIC','2026-07-02','RECEIVER',1,'a',2,999)""")
        self.assert_rejected_unchanged('invalid_restore_target')

    def test_known_old_transfer_index_is_safely_removed(self):
        self.install()
        self.execute("CREATE UNIQUE INDEX uq_delivery_transfer ON delivery_events(point_code,visit_date) WHERE event_type='TRANSFER'")
        self.install()
        with self.factory() as db:
            self.assertEqual(migration.migrate(db)['operations'], 0)

    def test_duplicate_request_keys_fail_before_ddl(self):
        self.execute(migration.TABLES['delivery_events'].replace('request_key TEXT NOT NULL UNIQUE', 'request_key TEXT NOT NULL'))
        self.execute("""INSERT INTO delivery_events(id,request_key,event_type,point_code,visit_date,second_merchant_id,initiator_id)
            VALUES('a','same','VISIT_ADDED','SYNTHETIC','2026-07-02',1,1),('b','same','VISIT_ADDED','SYNTHETIC','2026-07-02',2,2)""")
        self.assert_rejected_unchanged('duplicate_request_key')

    def test_orphan_role_fails(self):
        self.install()
        if self.postgres:
            self.execute('ALTER TABLE visit_payment_roles DROP CONSTRAINT fk_delivery_active_visit')
        self.execute("""INSERT INTO visit_payment_roles(merchant_id,point_code,visit_date,role,visit_id,event_id)
            VALUES(1,'SYNTHETIC','2026-07-02','RECEIVER',999,'a')""")
        self.assert_rejected_unchanged('orphan_active_role')

    def test_inventory_role_is_not_active_regular_visit(self):
        self.install()
        self.execute("UPDATE visits SET slot='EVENING' WHERE id=1")
        self.execute("""INSERT INTO visit_payment_roles(merchant_id,point_code,visit_date,role,visit_id,event_id)
            VALUES(1,'SYNTHETIC','2026-07-02','RECEIVER',1,'a')""")
        self.assert_rejected_unchanged('orphan_active_role')

    def test_cli_redacts_conflict_and_stops(self):
        self.execute(migration.TABLES['delivery_events'].replace('event_type TEXT NOT NULL,', ''))
        before = self.snapshot()
        for args in (['--dry-run'], ['--apply']):
            output = StringIO()
            with patch('app.db.SessionLocal', self.factory), patch.dict(os.environ, {'MAINTENANCE_MODE': '0'}), redirect_stdout(output):
                self.assertEqual(migration.main(args), 1)
            self.assertEqual(output.getvalue().strip(), 'Delivery preflight failed: missing_required_column; conflicts=1')
            self.assertEqual(self.snapshot(), before)


@unittest.skipUnless(os.getenv('TEST_DELIVERY_POSTGRES_URL'), 'Disposable localhost PostgreSQL not configured')
class PostgresPreflightTests(PreflightTests):
    postgres = True

    def test_missing_fk_is_completed(self):
        self.install()
        self.execute('ALTER TABLE visit_payment_roles DROP CONSTRAINT fk_delivery_active_visit')
        self.install()
        with self.factory() as db:
            self.assertEqual(migration.migrate(db)['operations'], 0)

    def test_wrong_named_fk_fails(self):
        self.install()
        self.execute('ALTER TABLE visit_payment_roles DROP CONSTRAINT fk_delivery_active_visit')
        self.execute('''ALTER TABLE visit_payment_roles ADD CONSTRAINT fk_delivery_active_visit
            FOREIGN KEY(visit_id) REFERENCES visits(id)''')
        self.assert_rejected_unchanged('incompatible_active_visit_foreign_key')

    def test_duplicate_visit_identity_fails_before_ddl(self):
        self.execute('ALTER TABLE visits DROP CONSTRAINT visits_pkey')
        self.execute("INSERT INTO visits VALUES(1,1,'SYNTHETIC','2026-07-02','DAY')")
        self.assert_rejected_unchanged('duplicate_visit_identity')

    def test_incompatible_default_fails(self):
        self.install()
        self.execute('ALTER TABLE visit_payment_roles ALTER COLUMN can_transfer SET DEFAULT TRUE')
        self.assert_rejected_unchanged('incompatible_column_default')
