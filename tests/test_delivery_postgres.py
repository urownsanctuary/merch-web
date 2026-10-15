"""Real PostgreSQL locking regressions, opt-in on an isolated localhost cluster."""
import os
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from unittest.mock import patch

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from app import delivery_roles as roles, services
from app.delivery_migration import migrate

URL=os.getenv('TEST_DELIVERY_POSTGRES_URL')


@unittest.skipUnless(URL, 'Set TEST_DELIVERY_POSTGRES_URL to a disposable local PostgreSQL')
class DeliveryPostgresTests(unittest.TestCase):
    def setUp(self):
        url=make_url(URL)
        if url.host not in {'127.0.0.1','localhost'}:
            raise ValueError('Tests require an isolated localhost PostgreSQL')
        self.schema='delivery_test_'+uuid.uuid4().hex
        engine=create_engine(URL)
        with engine.begin() as con:
            con.exec_driver_sql('CREATE SCHEMA '+self.schema)
        engine.dispose()
        self.engine=create_engine(URL,connect_args={'options':'-csearch_path='+self.schema+' -clock_timeout=5000 -cstatement_timeout=10000'})
        self.factory=sessionmaker(bind=self.engine)
        with self.engine.begin() as con:
            for ddl in (
                'CREATE TABLE merchants (id INTEGER PRIMARY KEY, fio TEXT, historical_fio TEXT, phone TEXT)',
                'CREATE TABLE visits (id SERIAL PRIMARY KEY, merchant_id INTEGER,point_code TEXT,visit_date DATE,slot TEXT, UNIQUE(merchant_id,point_code,visit_date,slot))',
                'CREATE TABLE supplies (point_code TEXT,supply_date DATE,boxes INTEGER)',
                'CREATE TABLE point_rates (point_code TEXT,month_key DATE,rate_supply INTEGER,rate_no_supply INTEGER,rate_inventory INTEGER,coffee_enabled BOOLEAN,coffee_rate INTEGER,pay_lt5 BOOLEAN)',
                'CREATE TABLE monthly_submissions (merchant_id INTEGER,month_key DATE,status TEXT,comment TEXT,extra_amount INTEGER,receipt_path TEXT,updated_at TIMESTAMP,UNIQUE(merchant_id,month_key))',
                'CREATE TABLE point_adjustments (merchant_id INTEGER,point_code TEXT,month_key DATE,note_amount INTEGER,note_comment TEXT,reimb_amount INTEGER,reimb_comment TEXT,reimb_receipt TEXT)',
            ):con.exec_driver_sql(ddl)
            con.exec_driver_sql("INSERT INTO merchants VALUES (1,'Synthetic A',NULL,NULL),(2,'Synthetic B',NULL,NULL),(3,'Synthetic C',NULL,NULL)")
            con.exec_driver_sql("INSERT INTO visits (merchant_id,point_code,visit_date,slot) VALUES (1,'P1','2026-07-02','DAY')")
            con.exec_driver_sql("INSERT INTO supplies VALUES ('P1','2026-07-02',10)")
            con.exec_driver_sql("INSERT INTO point_rates VALUES ('P1','2026-07-01',800,400,400,FALSE,100,FALSE)")
        with self.factory() as db:
            self.assertGreater(migrate(db)['operations'],0)
            migrate(db,apply=True);db.commit()
            self.assertEqual(migrate(db,apply=True)['operations'],0)
        self.day=date(2026,7,2)

    def tearDown(self):
        # Keep isolated schemas for diagnostic inspection; never drop shared data.
        self.engine.dispose()

    def receive(self, mid, barrier=None):
        with self.factory() as db:
            if barrier:barrier.wait(timeout=5)
            roles.lock_day(db,'P1',self.day)
            key=uuid.uuid4().hex
            result=roles.decide(db,merchant_id=mid,point='P1',day=self.day,slot='DAY',choice='receiver',key=key)
            if result['stage']=='ready':
                services.toggle_day_visit(db,mid,'P1',2026,7,2,'DAY',commit=False)
                roles.complete(db,decision=result,merchant_id=mid,point='P1',day=self.day,slot='DAY',key=key)
            db.commit()
            return result['stage']

    def test_two_concurrent_receivers_exactly_one_wins(self):
        barrier=threading.Barrier(2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(self.receive,mid,barrier) for mid in (2,3)]
            results=[f.result(timeout=15) for f in futures]
        self.assertCountEqual(results,['ready','exception'])
        with self.factory() as db:
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM visit_payment_roles WHERE role='RECEIVER'")).scalar(),1)
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM delivery_events WHERE event_type='TRANSFER'")).scalar(),1)
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM visits')).scalar(),2)

    def test_submit_wins_race_no_change_to_submitted_first(self):
        started=threading.Event()
        with self.factory() as submission,ThreadPoolExecutor(max_workers=1) as pool:
            roles.lock_merchants(submission,[1])
            def contender():
                started.set()
                return self.receive(2)
            future=pool.submit(contender)
            self.assertTrue(started.wait(5))
            # Use the actual submit path; target outside active month avoids
            # unrelated coffee fixture dependencies, but still locks identity.
            with patch('app.services.ensure_monthly_submissions_table'),patch('app.services.snapshot_coffee_for_submit'):
                services.submit_monthly_submission(submission,1,2026,7)
            self.assertEqual(future.result(timeout=15),'exception')
        with self.factory() as db:
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM visit_payment_roles')).scalar(),0)
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM visits')).scalar(),1)
            self.assertEqual(db.execute(text('SELECT event_type FROM delivery_events')).scalar(),'EXCEPTION_SUBMITTED')

    def test_failure_after_first_role_rolls_back_both_and_event(self):
        original=roles.set_role
        calls=[]
        def fail_second(*args,**kwargs):
            calls.append(True)
            if len(calls)==2:raise RuntimeError('synthetic second role failure')
            return original(*args,**kwargs)
        with patch.object(roles,'set_role',side_effect=fail_second),self.assertRaises(RuntimeError):
            self.receive(2)
        with self.factory() as db:
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM visit_payment_roles')).scalar(),0)
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM delivery_events')).scalar(),0)
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM visits')).scalar(),1)
