"""Current payment state must survive neither deleted attendance nor old cookies."""
import base64
import hashlib
import hmac
import json
import os
import time
import unittest
from unittest.mock import patch

from app import main, services, delivery_roles
from app.security import create_merchant_session, read_merchant_session, set_request_merchant, reset_request_merchant
from tests import test_delivery_roles as fixtures


class LifecycleFixture(unittest.TestCase):
    setUp = fixtures.DeliveryTests.setUp
    tearDown = fixtures.DeliveryTests.tearDown
    login = fixtures.DeliveryTests.login
    sql = fixtures.DeliveryTests.sql
    supply = fixtures.DeliveryTests.supply
    form = fixtures.DeliveryTests.form
    post = fixtures.DeliveryTests.post
    question_form = fixtures.DeliveryTests.question_form
    add_first = fixtures.DeliveryTests.add_first
    double = fixtures.DeliveryTests.double
    totals = fixtures.DeliveryTests.totals

    def assert_one_owner(self, expected):
        self.assertEqual([mid for mid in (1,2,3) if self.totals(mid)['cnt_supply']], expected)
        with self.factory() as db:
            report=services.get_admin_report_rows(db,2026,7)
        self.assertEqual(sorted(r['merchant_id'] for r in report if r['cnt_supply']), expected)
        self.assertLessEqual(len(self.sql("SELECT * FROM visit_payment_roles WHERE role='RECEIVER'")),1)

    def remove(self, mid):
        self.login(mid)
        return self.post(self.form(adding=False))

class LifecycleTests(LifecycleFixture):
    def test_original_high_delete_all_new_owner_restore_never_resurrects(self):
        self.supply();self.add_first();self.assert_one_owner([1])
        self.assertEqual(self.double()[0].status_code,303);self.assert_one_owner([2])
        audit=self.sql("SELECT * FROM delivery_events WHERE event_type='TRANSFER'")
        self.assertEqual(self.remove(1).status_code,303);self.assert_one_owner([2])
        self.assertEqual(self.remove(2).status_code,303);self.assert_one_owner([])
        self.assertEqual(self.sql('SELECT * FROM visit_payment_roles'),[])
        self.assertEqual(self.sql("SELECT * FROM delivery_events WHERE event_type='TRANSFER'"),audit)
        self.login(3);self.assertEqual(self.post(self.form()).status_code,303);self.assert_one_owner([3])
        self.login(2)
        prompt=self.question_form(self.post(self.form()))
        choice=self.post(prompt,delivery_choice='yes')
        self.assertIn('Как проходил ваш выход?',choice.text)
        self.assertEqual(self.sql('SELECT * FROM visits WHERE merchant_id=2'),[])
        self.assertEqual(self.post(self.question_form(choice),delivery_choice='no_supply').status_code,303)
        self.assert_one_owner([3])

    def test_receiver_delete_restores_first_and_is_idempotent(self):
        self.supply();self.add_first();self.double()
        for _ in range(3):
            self.login(2);form=self.form(adding=False)
            self.assertEqual(self.post(form).status_code,303)
            self.assertEqual(self.post(form).status_code,303)
            self.assert_one_owner([1])
            self.assertEqual(self.sql('SELECT * FROM visit_payment_roles WHERE merchant_id=2'),[])
            self.assertEqual(self.double()[0].status_code,303)
            self.assert_one_owner([2])
        self.assertEqual(len(self.sql("SELECT * FROM delivery_events WHERE event_type='RECEIVER_RESTORED'")),3)
        self.assertEqual(len(self.sql("SELECT * FROM delivery_events WHERE event_type='TRANSFER'")),4)

    def test_submitted_first_blocks_receiver_deletion_without_financial_change(self):
        self.supply();self.add_first();self.double()
        self.sql("INSERT INTO monthly_submissions(merchant_id,month_key,status) VALUES(1,'2026-07-01','submitted')")
        before=[self.totals(mid) for mid in (1,2)]
        self.assertEqual(self.remove(2).status_code,409)
        self.assertEqual([self.totals(mid) for mid in (1,2)],before)
        self.assertEqual(len(self.sql('SELECT * FROM visits')),2)
        self.assertEqual(len(self.sql("SELECT * FROM delivery_events WHERE event_type='EXCEPTION_DELETE_RECEIVER'")),1)

    def test_both_removed_in_reverse_order_leaves_no_state(self):
        self.supply();self.add_first();self.double()
        self.assertEqual(self.remove(2).status_code,303);self.assert_one_owner([1])
        self.assertEqual(self.remove(1).status_code,303);self.assert_one_owner([])
        self.assertEqual(self.sql('SELECT * FROM visit_payment_roles'),[])

    def test_deleted_fallback_cannot_resurrect_on_new_visit_same_person(self):
        self.supply();self.add_first();self.double()
        self.assertEqual(self.remove(1).status_code,303)
        receiver=self.sql("SELECT * FROM visit_payment_roles WHERE role='RECEIVER'")[0]
        self.assertIsNone(receiver['restore_visit_id'])
        self.assertEqual(self.double('no_supply',mid=1)[0].status_code,303)
        self.assert_one_owner([2])
        self.assertEqual(self.remove(2).status_code,409)
        self.assert_one_owner([2])

    def test_restoration_snapshot_respects_corrected_unpaid_supply(self):
        self.supply();self.add_first();self.double()
        self.sql('UPDATE supplies SET boxes=3')
        self.assertEqual(self.remove(2).status_code,303)
        row=self.sql("SELECT * FROM delivery_events WHERE event_type='RECEIVER_RESTORED'")[0]
        self.assertEqual(row['first_rate_after'],400)
        self.assertEqual(self.totals(1)['cnt_supply'],0)

    def test_inventory_peer_has_no_supply_role_but_confirmation_is_audited(self):
        self.supply(day=3);self.add_first(day=3,slot='EVENING')
        response,_=self.double(day=3,slot='MORNING')
        self.assertEqual(response.status_code,303)
        self.assertEqual(self.totals(1)['cnt_supply'],0)
        self.assertEqual(self.totals(2)['cnt_supply'],1)
        self.assertEqual(len(self.sql("SELECT * FROM delivery_events WHERE event_type='DOUBLE_REGULAR'")),1)

    def test_managed_receiver_suppresses_unassigned_automatic_payment(self):
        self.supply();self.add_first();self.double()
        # Model an out-of-band legacy insertion: both calculators use one owner.
        self.sql("INSERT INTO visits(merchant_id,point_code,visit_date,slot) VALUES(3,'P1','2026-07-02','DAY')")
        self.assert_one_owner([2])

    def test_single_visit_legacy_correction_remains_available_and_blocks_transfer(self):
        self.supply();self.add_first()
        response=self.client.post('/save-point-note-no-supply',data=dict(
            fio=self.names[1],point_code='P1',supply_day=2,csrf_token=self.csrf),follow_redirects=False)
        self.assertEqual(response.status_code,303,response.text)
        self.assertEqual(self.totals(1)['total'],500)
        self.assertEqual(self.double()[0].status_code,409)
        self.assertEqual(self.totals(1)['total'],500)
        self.assertEqual(self.sql('SELECT * FROM visits WHERE merchant_id=2'),[])

    def test_flag_default_off_creates_no_roles_or_events(self):
        self.supply()
        with patch.dict(os.environ,{},clear=True):
            self.assertFalse(delivery_roles.enabled())
        with patch.dict(os.environ,{'DELIVERY_ROLE_FLOW_ENABLED':'0'}):
            self.add_first();self.login(2)
            self.assertEqual(self.post(self.form()).status_code,303)
            self.assertEqual(self.sql('SELECT * FROM visit_payment_roles'),[])
            self.assertEqual(self.sql('SELECT * FROM delivery_events'),[])
            self.assertEqual([self.totals(m)['total'] for m in (1,2)],[900,900])

    def test_off_after_activation_keeps_totals_and_freezes_managed_day(self):
        self.supply();self.add_first();self.double()
        before=[self.totals(m) for m in (1,2)]
        with patch.dict(os.environ,{'DELIVERY_ROLE_FLOW_ENABLED':'0'}):
            self.assertEqual(self.remove(2).status_code,409)
            self.login(3);self.assertEqual(self.post(self.form()).status_code,409)
            self.assertEqual([self.totals(m) for m in (1,2)],before)


def signed_payload(payload):
    encoded=base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b'=').decode()
    signature=hmac.new(os.environ['SESSION_SECRET'].encode(),encoded.encode(),hashlib.sha256).hexdigest()
    return encoded+'.'+signature


class IdentityTests(LifecycleFixture):
    def legacy_cookie(self, **extra):
        return signed_payload(dict(sub=services.fio_norm(self.names[1]),iat=time.time(),exp=int(time.time())+3600,csrf='synthetic',**extra))

    def test_legacy_cookie_cannot_switch_to_active_namesake(self):
        self.sql('UPDATE merchants SET fio=:fio,fio_norm=:norm WHERE id=2',fio=self.names[1],norm=services.fio_norm(self.names[1]))
        self.sql('UPDATE merchants SET is_active=FALSE WHERE id=1')
        for values in ({},{'merchant_id':None},{'merchant_id':True},{'merchant_id':'2'},{'merchant_id':0}):
            with self.subTest(values=values):
                cookie=self.legacy_cookie(**values)
                self.assertIsNone(read_merchant_session(cookie))
                self.client.cookies.set('merchant_session',cookie)
                response=self.client.get('/calendar-page',params={'fio':self.names[1],'point_code':'P1'},follow_redirects=False)
                self.assertEqual(response.status_code,303)
                self.assertEqual(response.headers['location'],'/login-page')
                self.assertIn('Max-Age=0',response.headers['set-cookie'])

    def test_new_cookie_id_is_identity_and_inactive_missing_id_relogin(self):
        self.sql('UPDATE merchants SET fio=:fio,fio_norm=:norm WHERE id=2',fio=self.names[1],norm=services.fio_norm(self.names[1]))
        for mid in (1,2):
            ctx=set_request_merchant(services.fio_norm(self.names[1]),merchant_id=mid)
            try:
                with self.factory() as db:self.assertEqual(services.get_merchant_by_fio(db,self.names[1])['id'],mid)
            finally:reset_request_merchant(ctx)
        self.login(1)
        self.sql('UPDATE merchants SET is_active=FALSE WHERE id=1')
        for mid in (1,99999):
            self.client.cookies.set('merchant_session',create_merchant_session(services.fio_norm(self.names[1]),mid))
            response=self.client.get('/menu-page',params={'fio':self.names[1]},follow_redirects=False)
            self.assertEqual(response.status_code,303)
            self.assertEqual(response.headers['location'],'/login-page')
            self.assertIn('Max-Age=0',response.headers['set-cookie'])

    def test_legacy_merchant_cookie_does_not_break_admin(self):
        self.client.cookies.set('merchant_session',self.legacy_cookie())
        self.client.cookies.set('admin_auth',main.get_admin_cookie_value())
        self.assertEqual(self.client.get('/admin-export-delivery-changes?year=2026&month=7').status_code,200)

    def test_login_still_fio_last4_and_id_bound(self):
        self.sql('UPDATE merchants SET pass_hash=:hash WHERE id=1',hash=services.hash_last4('0001'))
        response=self.client.post('/login-page',data={'fio':self.names[1],'last4':'0001'},follow_redirects=False)
        self.assertEqual(response.status_code,303)
        self.assertEqual(read_merchant_session(response.cookies['merchant_session'])['merchant_id'],1)

    def test_receipts_require_id_not_namesake_and_admin_remains_allowed(self):
        self.sql('UPDATE merchants SET fio=:fio,fio_norm=:norm WHERE id=2',fio=self.names[1],norm=services.fio_norm(self.names[1]))
        self.names[2]=self.names[1]
        self.sql("INSERT INTO receipt_files(file_id,merchant_id,original_filename,content_type,data) VALUES('synthetic-receipt',1,'receipt.pdf','application/pdf',:data)",data=b'%PDF-synthetic')
        path='/receipts/synthetic-receipt/receipt.pdf'
        self.login(1);self.assertEqual(self.client.get(path).status_code,200)
        self.login(2);self.assertEqual(self.client.get(path).status_code,403)
        self.client.cookies.set('merchant_session',self.legacy_cookie())
        self.assertEqual(self.client.get(path,follow_redirects=False).status_code,303)
        self.client.cookies.set('merchant_session',self.legacy_cookie())
        self.client.cookies.set('admin_auth',main.get_admin_cookie_value())
        self.assertEqual(self.client.get(path).status_code,200)
