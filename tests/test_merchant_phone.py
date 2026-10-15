"""Full-phone import/admin regressions using reserved synthetic phone numbers."""
from io import BytesIO
import unittest
from openpyxl import Workbook
from sqlalchemy import text
from tests import test_merchant_admin as fixtures
from app import services, main
from app.security import read_merchant_session


def workbook(headers, rows):
    wb=Workbook();wb.active.append(headers)
    for row in rows:wb.active.append(row)
    output=BytesIO();wb.save(output);output.seek(0)
    return output


class PhoneTests(unittest.TestCase):
    setUp=fixtures.MerchantAdminTests.setUp
    tearDown=fixtures.MerchantAdminTests.tearDown

    def test_full_phone_variants_and_login(self):
        for raw in ('+7 000 000-00-01','8 000 000 00 01','7'+'0'*9+'1'):
            with self.subTest(raw=raw),self.factory() as db:
                result=services.import_merchants_xlsx(db,workbook(['ФИО','Телефон','ТУ'],[['Тестовый Сотрудник',raw,'ТУ-1']]),'')
                self.assertEqual(result['loaded_rows'],1)
                row=db.execute(text('SELECT id,phone,last4 FROM merchants')).mappings().one()
                self.assertEqual(row['phone'],'7'+'0'*9+'1');self.assertEqual(row['last4'],'0001')
                self.assertEqual(services.login_user(db,'Тестовый Сотрудник','0001')['id'],row['id'])
        response=self.client.post('/login-page',data={'fio':'Тестовый Сотрудник','last4':'0001'},follow_redirects=False)
        self.assertEqual(read_merchant_session(response.cookies['merchant_session'])['merchant_id'],row['id'])
        self.assertNotIn(row['phone'],response.text)
        self.assertNotIn(row['phone'],response.headers['location'])

    def test_legacy_last4_import_and_existing_identity_are_preserved(self):
        with self.factory() as db:
            for _ in range(2):
                services.import_merchants_xlsx(db,workbook(['ФИО','Последние 4'],[['Тестовый Сотрудник','1234']]),'ТУ')
            row=db.execute(text('SELECT id,phone,last4 FROM merchants')).one()
            self.assertEqual(row,(1,None,'1234'))
            services.import_merchants_xlsx(db,workbook(['ФИО','Телефон'],[['Тестовый Сотрудник','7'+'0'*6+'1234']]),'ТУ')
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM merchants')).scalar(),1)
            self.assertEqual(db.execute(text('SELECT id FROM merchants')).scalar(),1)

    def test_mismatch_rejects_entire_file_with_row_number_without_phone(self):
        with self.factory() as db:
            with self.assertRaises(ValueError) as error:
                services.import_merchants_xlsx(db,workbook(['ФИО','Телефон','last4'],[
                    ['Тестовый Первый','7'+'0'*9+'1','0001'],
                    ['Тестовый Второй','7'+'0'*9+'2','9999'],
                ]),'ТУ')
            self.assertIn('3',str(error.exception));self.assertIn('не совпадают',str(error.exception))
            self.assertNotIn('7'+'0'*9+'2',str(error.exception))
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM merchants')).scalar(),0)

    def test_admin_phone_field_and_csrf(self):
        self.client.cookies.set('admin_auth',self.admin_cookie)
        data={'fio':'Тестовый Сотрудник','phone':'+7 000 000-00-01','tu':'ТУ','csrf_token':self.csrf}
        response=self.client.post('/admin-add-merchant',data=data,follow_redirects=False)
        self.assertEqual(response.status_code,303,response.text)
        page=self.client.get('/admin-merchants/1/edit')
        self.assertIn('name="phone"',page.text);self.assertIn('7'+'0'*9+'1',page.text)
        self.assertEqual(self.client.post('/admin-add-merchant',data={**data,'csrf_token':''}).status_code,403)
        legacy_update=self.client.post('/admin-merchants/1',data={
            'fio':'Тестовый Сотрудник','last4':'0001','tu':'ТУ','status':'active','csrf_token':self.csrf,
        },follow_redirects=False)
        self.assertEqual(legacy_update.status_code,303,legacy_update.text)
        with self.factory() as db:
            self.assertEqual(db.execute(text('SELECT phone FROM merchants WHERE id=1')).scalar(),'7'+'0'*9+'1')


    def test_invalid_phone_does_not_leak_value(self):
        with self.factory() as db:
            with self.assertRaises(ValueError) as error:
                services.import_merchants_xlsx(db,workbook(['ФИО','Телефон'],[['Тестовый Сотрудник','private-invalid-input']]),'ТУ')
            self.assertNotIn('private-invalid-input',str(error.exception))
            self.assertEqual(db.execute(text('SELECT COUNT(*) FROM merchants')).scalar(),0)
