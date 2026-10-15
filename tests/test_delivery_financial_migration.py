"""Legacy money and submitted snapshots must survive additive migration."""
from contextlib import ExitStack
from contextlib import redirect_stdout
from io import StringIO
import os
import sqlite3
import unittest
import uuid
from unittest.mock import patch

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker
from app import services, delivery_migration
from app.coffee_days import ensure_coffee_days_schema


class FinancialMigrationTests(unittest.TestCase):
    postgres = False

    def setUp(self):
        if self.postgres:
            url=os.environ['TEST_DELIVERY_POSTGRES_URL']
            if make_url(url).host not in {'localhost','127.0.0.1'}:
                raise ValueError('Disposable localhost PostgreSQL required')
            schema='financial_test_'+uuid.uuid4().hex
            bootstrap=create_engine(url)
            with bootstrap.begin() as conn:conn.exec_driver_sql('CREATE SCHEMA '+schema)
            bootstrap.dispose()
            self.engine=create_engine(url,connect_args={'options':'-csearch_path='+schema})
        else:
            self.engine=create_engine('sqlite+pysqlite:///:memory:',connect_args={'detect_types':sqlite3.PARSE_DECLTYPES})
        self.factory=sessionmaker(bind=self.engine)
        pk='SERIAL PRIMARY KEY' if self.postgres else 'INTEGER PRIMARY KEY'
        with self.engine.begin() as conn:
            for ddl in (
                f'CREATE TABLE merchants(id {pk},fio TEXT,fio_norm TEXT,pass_hash TEXT,last4 TEXT,tu TEXT,is_active BOOLEAN DEFAULT TRUE,created_at TIMESTAMP,historical_fio TEXT,historical_tu TEXT,credentials_cleared_at TIMESTAMP)',
                f'CREATE TABLE visits(id {pk},merchant_id INTEGER,point_code TEXT,visit_date DATE,slot TEXT)',
                'CREATE TABLE supplies(point_code TEXT,supply_date DATE,boxes INTEGER)',
                'CREATE TABLE point_rates(point_code TEXT,month_key DATE,rate_supply INTEGER,rate_no_supply INTEGER,rate_inventory INTEGER,coffee_enabled BOOLEAN,coffee_rate INTEGER,pay_lt5 BOOLEAN)',
                f'CREATE TABLE monthly_submissions(id {pk},merchant_id INTEGER,month_key DATE,status TEXT,comment TEXT,extra_amount INTEGER,receipt_path TEXT,updated_at TIMESTAMP,UNIQUE(merchant_id,month_key))',
                f'CREATE TABLE point_adjustments(id {pk},merchant_id INTEGER,point_code TEXT,month_key DATE,note_amount INTEGER,note_comment TEXT,reimb_amount INTEGER,reimb_comment TEXT,reimb_receipt TEXT,updated_at TIMESTAMP)',
            ):conn.exec_driver_sql(ddl)
            conn.exec_driver_sql("INSERT INTO merchants(id,fio,fio_norm,last4,tu) VALUES(1,'Synthetic Alpha','synthetic alpha','0001','TEST'),(2,'Synthetic Beta','synthetic beta','0002','TEST')")
            conn.exec_driver_sql("INSERT INTO point_rates VALUES('P1','2026-07-01',800,400,400,TRUE,100,FALSE),('P1','2026-06-01',700,350,350,TRUE,90,TRUE)")
            conn.exec_driver_sql("INSERT INTO visits(merchant_id,point_code,visit_date,slot) VALUES(1,'P1','2026-07-02','DAY'),(1,'P1','2026-07-03','MORNING'),(1,'P1','2026-07-03','EVENING'),(1,'P1','2026-07-08','FULL_INVENT'),(2,'P1','2026-07-02','DAY'),(1,'P1','2026-06-02','DAY')")
            conn.exec_driver_sql("INSERT INTO supplies VALUES('P1','2026-07-02',10),('P1','2026-07-03',3),('P1','2026-06-02',3)")
            conn.exec_driver_sql("INSERT INTO monthly_submissions(merchant_id,month_key,status,comment,extra_amount) VALUES(1,'2026-07-01','submitted','old submitted',75),(2,'2026-07-01','draft','old draft',25),(1,'2026-06-01','submitted','older month',50)")
            conn.exec_driver_sql("INSERT INTO point_adjustments(merchant_id,point_code,month_key,note_amount,note_comment,reimb_amount,reimb_comment) VALUES(1,'P1','2026-07-01',-125,'old note',225,'old reimbursement')")
        with self.factory() as db:
            ensure_coffee_days_schema(db)
            db.execute(text("INSERT INTO coffee_bonus(merchant_id,point_code,month_key,enabled,days_count,manual_delta) VALUES(1,'P1','2026-07-01',TRUE,1,-1),(1,'P1','2026-06-01',TRUE,1,0)"));db.commit()
        self.stack=ExitStack()
        for helper in ('ensure_point_adjustments_table','ensure_monthly_submissions_table'):
            self.stack.enter_context(patch.object(services,helper))
        self.stack.enter_context(patch.object(services,'get_active_period',return_value={'year':2026,'month':7}))

    def tearDown(self):
        self.stack.close();self.engine.dispose()

    def snapshot(self):
        with self.factory() as db:
            tables=('visits','supplies','point_rates','point_adjustments','coffee_bonus','coffee_days_audit','monthly_submissions')
            return {
                'rows':{t:[dict(r) for r in db.execute(text('SELECT * FROM '+t+' ORDER BY 1')).mappings()] for t in tables},
                'identity':[tuple(r) for r in db.execute(text('SELECT id,last4 FROM merchants ORDER BY id'))],
                'totals':{(mid,month):services.compute_overall_total(db,mid,2026,month) for mid,month in ((1,6),(1,7),(2,7))},
                'admin':services.get_admin_report_rows(db,2026,7),
                'payroll':services.get_admin_payroll_rows(db,2026,7),
            }

    def test_financial_snapshot_dry_apply_reapply(self):
        before=self.snapshot()
        self.assertEqual([v['total'] for v in before['totals'].values()],[840,2275,925])
        with self.factory() as db:
            self.assertGreater(delivery_migration.migrate(db)['operations'],0)
            self.assertFalse(inspect(db.connection()).has_table('visit_payment_roles'))
            self.assertNotIn('phone',{c['name'] for c in inspect(db.connection()).get_columns('merchants')})
            db.rollback()
        self.assertEqual(self.snapshot(),before)
        for iteration in range(2):
            with self.factory() as db:
                result=delivery_migration.migrate(db,apply=True);db.commit()
                if iteration:self.assertEqual(result['operations'],0)
                self.assertEqual(db.execute(text('SELECT COUNT(*) FROM merchants WHERE phone IS NOT NULL')).scalar(),0)
                self.assertEqual(db.execute(text('SELECT COUNT(*) FROM visit_payment_roles')).scalar(),0)
            self.assertEqual(self.snapshot(),before)

    def test_cli_default_dry_run_apply_reapply(self):
        before=self.snapshot()
        with patch('app.db.SessionLocal',self.factory), patch.dict(os.environ,{'MAINTENANCE_MODE':'0'}), redirect_stdout(StringIO()):
            self.assertEqual(delivery_migration.main([]),0)
            self.assertEqual(delivery_migration.main(['--dry-run']),0)
            self.assertFalse(inspect(self.engine).has_table('delivery_events'))
            self.assertEqual(delivery_migration.main(['--apply']),0)
            self.assertEqual(delivery_migration.main(['--apply']),0)
            self.assertEqual(delivery_migration.main(['--dry-run']),0)
        self.assertEqual(self.snapshot(),before)

    def test_unreleased_nonempty_roles_fail_before_any_schema_mutation(self):
        with self.factory() as db:
            db.execute(text('''CREATE TABLE visit_payment_roles(merchant_id INTEGER,point_code TEXT,
                visit_date DATE,role TEXT,visit_id INTEGER,event_id TEXT)'''))
            db.execute(text("INSERT INTO visit_payment_roles VALUES(2,'P1','2026-07-02','NO_SUPPLY',5,'synthetic')"));db.commit()
        for apply in (False,True):
            with self.factory() as db:
                with self.assertRaises(ValueError):delivery_migration.migrate(db,apply=apply)
                db.rollback()
            self.assertNotIn('phone',{c['name'] for c in inspect(self.engine).get_columns('merchants')})
            self.assertFalse(inspect(self.engine).has_table('delivery_events'))


@unittest.skipUnless(os.getenv('TEST_DELIVERY_POSTGRES_URL'),'Disposable localhost PostgreSQL not configured')
class PostgresFinancialMigrationTests(FinancialMigrationTests):
    postgres = True

    def test_partial_migration_failure_rolls_back_ddl_and_finances(self):
        before=self.snapshot()
        with patch.dict(delivery_migration.INDEXES,{'synthetic_failure':'CREATE INDEX synthetic_failure ON nonexistent_table(id)'}):
            with self.assertRaises(Exception):
                with self.factory() as db:
                    delivery_migration.migrate(db,apply=True);db.commit()
        self.assertFalse(inspect(self.engine).has_table('delivery_events'))
        self.assertNotIn('phone',{c['name'] for c in inspect(self.engine).get_columns('merchants')})
        self.assertEqual(self.snapshot(),before)
