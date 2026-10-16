"""Sunday selection and direct POST validation on isolated attendance data."""
import os
from datetime import date
from unittest.mock import patch

from app import main
from tests.test_delivery_lifecycle import LifecycleFixture
from tests.test_visit_choice_ux import Forms


class SundayVisitTests(LifecycleFixture):
    def state(self):
        return {table: self.sql('SELECT * FROM ' + table + ' ORDER BY 1')
                for table in ('visits', 'visit_payment_roles', 'delivery_events', 'coffee_bonus')}

    def test_ordinary_sunday_direct_day_add_remove_in_both_feature_modes(self):
        for flag in ('0', '1'):
            with self.subTest(flag=flag), patch.dict(os.environ, {'DELIVERY_ROLE_FLOW_ENABLED': flag}):
                html = main.build_calendar_html(self.names[1], 'P1', 2026, 7, {}, {}, False,
                                               set(), {}, csrf_token=self.csrf)
                form = next(f for f in Forms(html).forms if f['fields']['day'] == '5')
                self.assertEqual(form['fields']['slot'], 'DAY')
                self.assertEqual(form['action'], '/toggle-day')
                self.assertEqual(self.post(form['fields']).status_code, 303)
                self.assertEqual([r['slot'] for r in self.sql('SELECT slot FROM visits')], ['DAY'])
                self.assertEqual(self.post(self.form(5, 'DAY', adding=False)).status_code, 303)
                self.assertEqual(self.sql('SELECT * FROM visits'), [])
                self.assertEqual(self.sql('SELECT * FROM visit_payment_roles'), [])

    def test_ordinary_sunday_rejects_direct_evening_inventory_and_new_morning(self):
        for flag in ('0', '1'):
            with patch.dict(os.environ, {'DELIVERY_ROLE_FLOW_ENABLED': flag}):
                for slot in ('MORNING', 'EVENING', 'FULL_INVENT'):
                    with self.subTest(flag=flag, slot=slot):
                        before = self.state()
                        response = self.post(self.form(5, slot))
                        self.assertEqual(response.status_code, 409)
                        self.assertEqual(self.state(), before)

    def test_special_sunday_exact_choices_and_day_full_inventory_storage(self):
        self.stack.enter_context(patch('app.main.get_special_inventory_days', return_value=[date(2026, 7, 5)]))
        for flag in ('0', '1'):
            with self.subTest(flag=flag), patch.dict(os.environ, {'DELIVERY_ROLE_FLOW_ENABLED': flag}):
                page = self.client.get('/day-action-page', params=dict(fio=self.names[1], point_code='P1', day=5))
                forms = Forms(page.text).forms
                self.assertEqual([f['button'] for f in forms], ['Добавить выход', 'Добавить полный инвент'])
                self.assertEqual([f['action'] for f in forms], ['/toggle-day', '/toggle-inventory'])
                self.assertEqual(self.post(forms[0]['fields']).status_code, 303)
                self.assertEqual(self.post(self.form(5, 'FULL_INVENT')).status_code, 303)
                self.assertEqual({r['slot'] for r in self.sql('SELECT slot FROM visits')}, {'DAY', 'FULL_INVENT'})
                before = self.state()
                self.assertEqual(self.post(self.form(5, 'EVENING')).status_code, 409)
                self.assertEqual(self.state(), before)
                self.assertEqual(self.post(self.form(5, 'FULL_INVENT', adding=False)).status_code, 303)
                self.assertEqual(self.post(self.form(5, 'DAY', adding=False)).status_code, 303)

    def test_historical_sunday_slots_preserved_without_duplicate_regular_payment(self):
        self.sql("INSERT INTO visits(merchant_id,point_code,visit_date,slot) VALUES(1,'P1','2026-07-05','MORNING'),(1,'P1','2026-07-05','EVENING')")
        before = self.state()
        for flag in ('0', '1'):
            with patch.dict(os.environ, {'DELIVERY_ROLE_FLOW_ENABLED': flag}):
                self.assertEqual(self.post(self.form(5, 'DAY')).status_code, 409)
                self.assertEqual(self.state(), before)
        response = self.client.get('/calendar-page', params=dict(fio=self.names[1], point_code='P1'))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.state(), before)
        self.assertIn('badge-day">В', response.text)
        self.assertIn('badge-inv">И', response.text)
        form = next(f for f in Forms(response.text).forms if f['fields'].get('day') == '5')
        self.assertEqual(form['fields']['slot'], 'MORNING')
        self.assertEqual(self.post(form['fields']).status_code, 303)
        self.assertEqual([r['slot'] for r in self.sql('SELECT slot FROM visits')], ['EVENING'])
        self.assertEqual(self.post(self.form(5, 'DAY')).status_code, 303)
        self.assertEqual({r['slot'] for r in self.sql('SELECT slot FROM visits')}, {'DAY', 'EVENING'})
        before = self.state()
        self.assertEqual(self.post(self.form(5, 'EVENING', adding=False)).status_code, 409)
        self.assertEqual(self.state(), before)

    def test_special_sunday_legacy_evening_is_removal_only(self):
        self.stack.enter_context(patch('app.main.get_special_inventory_days', return_value=[date(2026, 7, 5)]))
        self.sql("INSERT INTO visits(merchant_id,point_code,visit_date,slot) VALUES(1,'P1','2026-07-05','EVENING')")
        self.assertEqual(self.post(self.form(5, 'EVENING', adding=False)).status_code, 303)
        self.assertEqual(self.post(self.form(5, 'EVENING')).status_code, 409)
        self.assertEqual(self.post(self.form(5, 'FULL_INVENT')).status_code, 303)
        self.assertEqual([r['slot'] for r in self.sql('SELECT slot FROM visits')], ['FULL_INVENT'])
