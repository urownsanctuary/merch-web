"""Delivery migration must not break the existing protected admin preview."""
import unittest
from unittest.mock import patch

from sqlalchemy import text

from tests import test_delivery_roles as fixtures
from app import main
from app.delivery_migration import migrate
from app.merchant_admin import count_merchant_owned_rows


class DeliveryAdminDataTests(unittest.TestCase):
    setUp = fixtures.DeliveryTests.setUp
    tearDown = fixtures.DeliveryTests.tearDown
    login = fixtures.DeliveryTests.login
    sql = fixtures.DeliveryTests.sql

    def test_preview_after_migration_and_existing_forms(self):
        with self.factory() as db:
            self.assertEqual(migrate(db)['operations'], 0)
            counts = count_merchant_owned_rows(db)
        self.assertEqual(counts['merchants'], 3)
        self.assertNotIn('delivery_events', counts)
        self.assertNotIn('visit_payment_roles', counts)
        self.client.cookies.set('admin_auth', main.get_admin_cookie_value())
        with patch('app.main.log_redacted_exception') as errors:
            page = self.client.get('/admin-data')
        self.assertEqual(page.status_code, 200)
        self.assertIn('Текущее количество:', page.text)
        self.assertNotIn('Не удалось безопасно посчитать', page.text)
        self.assertIn('/admin-upload-merchants', page.text)
        self.assertIn('/admin-upload-supplies', page.text)
        self.assertNotIn('7' + '0' * 9 + '1', page.text)
        self.assertFalse(any(call.args[0] == 'merchant_delete_preview_failed' for call in errors.call_args_list))

    def test_non_admin_is_redirected_without_preview(self):
        self.client.cookies.delete('admin_auth')
        with patch('app.main.count_merchant_owned_rows') as preview:
            page = self.client.get('/admin-data', follow_redirects=False)
        self.assertEqual(page.status_code, 303)
        self.assertEqual(page.headers['location'], '/admin-login')
        preview.assert_not_called()

    def test_unknown_merchant_table_is_still_rejected(self):
        with self.factory() as db:
            db.execute(text('CREATE TABLE unexpected_private_data(merchant_id INTEGER)'))
            db.commit()
            with self.assertRaisesRegex(RuntimeError, 'unexpected_private_data'):
                count_merchant_owned_rows(db)
