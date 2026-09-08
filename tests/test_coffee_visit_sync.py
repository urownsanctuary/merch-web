"""Coffee synchronization only: real POST routes and isolated synthetic history."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
from tests import test_four_production_fixes as fixtures
from app import services
from app.coffee_days import ensure_coffee_days_schema


class CoffeeVisitSyncTests(unittest.TestCase):
    setUp = fixtures.FourFixTests.setUp
    tearDown = fixtures.FourFixTests.tearDown

    def total(self):
        with self.factory() as db:
            return services.compute_point_total(db,1,'P1',2026,7)

    def visit(self,day,slot='DAY',status=303):
        result=self.client.post('/toggle-day',data={'fio':'Тестовый Мерч','point_code':'P1',
            'day':day,'slot':slot,'csrf_token':self.csrf,'confirm_non_working':'1'},follow_redirects=False)
        self.assertEqual(result.status_code,status)
        return result

    def manual(self,delta):
        result=self.client.post('/coffee-days',data={'fio':'Тестовый Мерч','point_code':'P1',
            'delta':delta,'csrf_token':self.csrf},follow_redirects=False)
        self.assertEqual(result.status_code,303)
        return result

    def test_zero_add_second_remove(self):
        self.assertEqual(self.total()['coffee_cnt'],0)
        for day,expected in [(1,1),(2,2),(1,1)]:
            self.visit(day);self.assertEqual(self.total()['coffee_cnt'],expected)

    def test_friday_morning_evening_one_calendar_day(self):
        self.visit(3,'MORNING');self.assertEqual(self.total()['coffee_cnt'],1)
        self.visit(3,'EVENING');self.assertEqual(self.total()['coffee_cnt'],1)
        self.visit(3,'MORNING');self.assertEqual(self.total()['coffee_cnt'],0)

    def test_saturday_morning(self):
        self.visit(4,'MORNING');self.assertEqual(self.total()['coffee_cnt'],1)

    def test_legacy_slots_on_same_date_not_double_counted(self):
        with self.factory() as db:
            db.execute(text("INSERT INTO visits VALUES (1,1,'P1','2026-07-03','DAY')"));db.commit()
        self.visit(3,'MORNING');self.assertEqual(self.total()['coffee_cnt'],1)

    def test_manual_four_to_three_then_new_visit(self):
        for day in (1,2,5,6):self.visit(day)
        self.manual(-1);self.assertEqual(self.total()['coffee_cnt'],3)
        self.visit(7);self.assertEqual(self.total()['coffee_cnt'],4)
        self.visit(2);self.assertEqual(self.total()['coffee_cnt'],3)
        self.visit(8);self.assertEqual(self.total()['coffee_cnt'],4)

    def test_manual_delta_survives_zero_floor(self):
        for day in (1,2,5):self.visit(day)
        self.manual(-1);self.manual(-1)
        for day in (1,2,5):self.visit(day)
        self.assertEqual(self.total()['coffee_cnt'],0)
        for day,expected in [(6,0),(7,0),(8,1)]:
            self.visit(day);self.assertEqual(self.total()['coffee_cnt'],expected)

    def test_manual_plus_and_max(self):
        self.visit(1);self.manual(-1)
        self.assertIn('saved=1',self.manual(1).headers['location'])
        self.assertIn('coffee_error=1',self.manual(1).headers['location'])
        self.visit(2);self.assertEqual(self.total()['coffee_cnt'],2)

    def test_coffee_point_overall_and_unchanged_report_calculations(self):
        self.visit(1);self.visit(2)
        total=self.total();self.assertEqual(total['coffee_sum'],200);self.assertEqual(total['total'],1000)
        self.manual(-1);self.visit(5)
        with self.factory() as db:
            self.assertEqual(services.compute_overall_total(db,1,2026,7)['total'],1400)
            self.assertEqual(services.get_admin_report_rows(db,2026,7)[0]['point_total'],1400)

    def test_submit_blocks_visits_and_manual_change(self):
        self.visit(1)
        with self.engine.begin() as connection:
            connection.exec_driver_sql('CREATE UNIQUE INDEX monthly_identity ON monthly_submissions (merchant_id,month_key)')
        result=self.client.post('/submit-monthly-submission',data={'fio':'Тестовый Мерч','csrf_token':self.csrf},follow_redirects=False)
        self.assertEqual(result.status_code,303)
        self.visit(2,status=409)
        self.assertIn('coffee_error=1',self.manual(-1).headers['location'])
        self.assertEqual(self.total()['coffee_cnt'],1)

    def test_disabled_point_has_no_coffee_writes(self):
        with self.factory() as db:
            db.execute(text('UPDATE point_rates SET coffee_enabled=FALSE'));db.commit()
        self.visit(1);self.visit(2);self.visit(1)
        self.assertEqual(self.total()['coffee_cnt'],0)
        with self.factory() as db:
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM coffee_bonus')).scalar(),0)

    def test_existing_absolute_count_is_lazily_preserved_and_old_month_untouched(self):
        with self.factory() as db:
            for day in (1,2,5,6):
                db.execute(text("INSERT INTO visits (merchant_id,point_code,visit_date,slot) VALUES (1,'P1',:day,'DAY')"),{'day':f'2026-07-{day:02d}'})
            db.execute(text("INSERT INTO coffee_bonus (merchant_id,point_code,month_key,enabled,days_count) VALUES (1,'P1','2026-07-01',TRUE,3),(1,'P1','2026-06-01',TRUE,4)"));db.commit()
            ensure_coffee_days_schema(db);ensure_coffee_days_schema(db)
        self.assertEqual(self.total()['coffee_cnt'],3)
        self.visit(7);self.assertEqual(self.total()['coffee_cnt'],4)
        with self.factory() as db:
            row=db.execute(text("SELECT days_count,manual_delta FROM coffee_bonus WHERE month_key='2026-06-01'")).one()
            self.assertEqual(tuple(row),(4,None))

    def test_sync_failure_rolls_back_visit_and_coffee(self):
        self.visit(1)
        with patch('app.main.sync_coffee_after_visit',side_effect=RuntimeError('synthetic failure')), patch('app.main.log_redacted_exception'):
            self.visit(2,status=503)
        self.assertEqual(self.total()['cnt_day_total'],1)
        self.assertEqual(self.total()['coffee_cnt'],1)
