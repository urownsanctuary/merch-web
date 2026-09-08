"""Transactional regressions for Android receipts, coffee and preserved history."""
import os
import sqlite3
import unittest
from contextlib import ExitStack
from datetime import date
from io import BytesIO
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ADMIN_LOGIN", "admin")
os.environ.setdefault("ADMIN_PASSWORD", "strong-password")
os.environ.setdefault("ENVIRONMENT", "test")

from fastapi.testclient import TestClient
from openpyxl import load_workbook
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from app import main, services
from app.coffee_days import ensure_coffee_days_schema, coffee_count, change_coffee_days
from app.merchant_admin import ensure_merchant_admin_schema, delete_all_merchants_and_data
from app.security import create_merchant_session, read_merchant_session, set_request_merchant, reset_request_merchant


class FourFixTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool,
            connect_args={"check_same_thread": False, "detect_types": sqlite3.PARSE_DECLTYPES})
        @event.listens_for(self.engine, "connect")
        def functions(connection, _):
            connection.create_function("NOW", 0, lambda: "2026-07-20 12:00:00")
        self.factory = sessionmaker(bind=self.engine)
        schemas = [
            "CREATE TABLE merchants (id INTEGER PRIMARY KEY AUTOINCREMENT, fio TEXT NOT NULL, fio_norm TEXT NOT NULL, pass_hash TEXT NOT NULL, telegram_id INTEGER, tu TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE visits (id INTEGER PRIMARY KEY, merchant_id INTEGER, point_code TEXT, visit_date DATE, slot TEXT)",
            "CREATE TABLE supplies (point_code TEXT, supply_date DATE, boxes INTEGER)",
            "CREATE TABLE point_rates (point_code TEXT, month_key DATE, rate_supply INTEGER, rate_no_supply INTEGER, rate_inventory INTEGER, coffee_enabled BOOLEAN, coffee_rate INTEGER, pay_lt5 BOOLEAN)",
            "CREATE TABLE monthly_submissions (id INTEGER PRIMARY KEY, merchant_id INTEGER, month_key DATE, status TEXT, comment TEXT, extra_amount INTEGER DEFAULT 0, receipt_path TEXT, updated_at TIMESTAMP)",
            "CREATE TABLE point_adjustments (id INTEGER PRIMARY KEY, merchant_id INTEGER, point_code TEXT, month_key DATE, note_amount INTEGER DEFAULT 0, note_comment TEXT, reimb_amount INTEGER DEFAULT 0, reimb_comment TEXT, reimb_receipt TEXT, updated_at TIMESTAMP)",
        ]
        with self.engine.begin() as con:
            for sql in schemas:
                con.exec_driver_sql(sql)
        with self.factory() as db:
            ensure_merchant_admin_schema(db)
            ensure_coffee_days_schema(db)
            main.ensure_receipt_files_table(db)
            db.execute(text("INSERT INTO merchants (fio,fio_norm,pass_hash,tu) VALUES ('Тестовый Мерч','тестовый мерч',:hash,'ТУ-1')"), {"hash": services.hash_last4("1234")})
            db.execute(text("INSERT INTO point_rates VALUES ('P1','2026-07-01',800,400,400,TRUE,100,FALSE)"))
            db.commit()
        self.stack = ExitStack()
        for helper in ("ensure_point_adjustments_table", "ensure_monthly_submissions_table"):
            self.stack.enter_context(patch("app.services." + helper))
        self.stack.enter_context(patch("app.main.get_active_period", return_value={"year":2026,"month":7,"editable_until":"2026-08-05"}))
        self.stack.enter_context(patch("app.main.get_special_inventory_days", return_value=[]))
        self.stack.enter_context(patch("app.main.get_calendar_overrides", return_value={}))
        def dependency():
            with self.factory() as db:
                yield db
        main.app.dependency_overrides[main.get_db] = dependency
        self.client = TestClient(main.app)
        self.token = create_merchant_session("тестовый мерч")
        self.csrf = read_merchant_session(self.token)["csrf"]
        self.client.cookies.set("merchant_session", self.token)

    def tearDown(self):
        self.client.close()
        self.stack.close()
        main.app.dependency_overrides.clear()
        self.engine.dispose()

    def upload(self, files, **extra):
        return self.client.post("/save-point-reimbursement", data={
            "fio":"Тестовый Мерч","point_code":"P1","reimb_amount":"150",
            "reimb_comment":"Тестовый чек","csrf_token":self.csrf, **extra,
        }, files=[("reimb_receipts", f) for f in files], follow_redirects=False)

    def counts(self):
        with self.factory() as db:
            return tuple(db.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar() for t in ("receipt_files","point_adjustments"))

    def test_android_metadata_is_normalized(self):
        for name, mime, content, expected in [
            ("Фото чека.jpg", "image/jpg", b"\xff\xd8\xff\xe1EXIF", "image/jpeg"),
            ("Фото без расширения", "application/octet-stream", b"\xff\xd8\xff\xe1EXIF", "image/jpeg"),
            ("чек.png", "image/png", b"\x89PNG\r\n\x1a\nDATA", "image/png"),
            ("чек.webp", "image/webp", b"RIFFxxxxWEBPdata", "image/webp"),
            ("файл", "application/octet-stream", b"%PDF-1.7 data", "application/pdf"),
        ]:
            with self.subTest(mime=mime, name=name):
                response = self.upload([(name, content, mime)])
                self.assertEqual(response.status_code, 303, response.text)
                with self.factory() as db:
                    row = db.execute(text("SELECT * FROM receipt_files ORDER BY rowid DESC LIMIT 1")).mappings().one()
                    self.assertEqual(row["content_type"], expected)
                    self.assertEqual(row["data"], content)
                    self.assertEqual(len(row["file_id"]), 32)
                    self.assertTrue(row["original_filename"].startswith("receipt."))

    def test_large_android_photo_is_accepted_up_to_limit(self):
        response = self.upload([("photo", b"\xff\xd8\xff" + b"x" * (6 * 1024 * 1024), "application/octet-stream")])
        self.assertEqual(response.status_code, 303)

    def test_empty_heic_and_oversize_are_friendly_and_atomic(self):
        for content in (b"", b"xxxxftypheicDATA", b"xxxxftypheifDATA", b"bad", b"x" * (main.MAX_RECEIPT_BYTES + 1)):
            with self.subTest(size=len(content)):
                response = self.upload([("first.jpg",b"\xff\xd8\xffdata","image/jpeg"),("bad",content,"application/octet-stream")])
                self.assertEqual(response.status_code, 400)
                self.assertIn("Возмещение не сохранено", response.text)
                self.assertEqual(self.counts(), (0, 0))

    def test_multiple_files_commit_once_and_survive_new_session(self):
        commits = []
        with patch.object(main, "ensure_receipt_files_table", side_effect=AssertionError("schema commit during upload")):
            event.listen(self.factory, "after_commit", lambda session: commits.append(True))
            response = self.upload([("one",b"%PDF-1.7 a","application/octet-stream"),("two",b"\xff\xd8\xffdata","image/jpg")])
        self.assertEqual(response.status_code, 303, response.text)
        self.assertEqual(len(commits), 1)
        self.assertEqual(self.counts(), (2, 1))
        with self.factory() as db:
            paths = db.execute(text("SELECT reimb_receipt FROM point_adjustments")).scalar().split("|")
        for path in paths:
            self.assertEqual(self.client.get("/" + path).status_code, 200)

    def test_db_failure_after_first_insert_rolls_back_all(self):
        real = main.save_receipt_file_to_db
        calls = []
        def fail_second(*args):
            calls.append(True)
            if len(calls) == 2:
                raise RuntimeError("synthetic private DB error")
            return real(*args)
        with patch.object(main, "save_receipt_file_to_db", side_effect=fail_second):
            response = self.upload([("one",b"%PDF-a","application/pdf"),("two",b"%PDF-b","application/pdf")])
        self.assertEqual(response.status_code, 400)
        self.assertNotIn("private", response.text)
        self.assertEqual(self.counts(), (0, 0))

    def test_adjustment_failure_rolls_back_receipts(self):
        with patch.object(main, "upsert_point_adjustment", side_effect=RuntimeError("synthetic failure")):
            response = self.upload([("one",b"%PDF-a","application/pdf")])
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.counts(), (0,0))

    def test_upload_requires_csrf(self):
        self.assertEqual(self.upload([("one",b"%PDF-a","application/pdf")],csrf_token="").status_code, 403)
        self.assertEqual(self.counts(), (0,0))

    def test_compatibility_upload_is_atomic_and_submitted_upload_is_blocked(self):
        response = self.client.post("/save-point-adjustment", data={
            "fio":"Тестовый Мерч", "point_code":"P1", "reimb_amount":"150",
            "reimb_comment":"Чек", "csrf_token":self.csrf,
        }, files={"reimb_receipt":("photo",b"ftypheic","image/heic")})
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.counts(),(0,0))
        with self.factory() as db:
            db.execute(text("INSERT INTO monthly_submissions (merchant_id,month_key,status) VALUES (1,'2026-07-01','submitted')"))
            db.commit()
        self.assertEqual(self.upload([("photo",b"%PDF-a","application/pdf")]).status_code,409)
        self.assertEqual(self.counts(),(0,0))

    def test_coffee_default_explicit_zero_bounds_audit_and_lock(self):
        with self.factory() as db:
            ensure_coffee_days_schema(db)
            self.assertEqual(coffee_count(db,1,"P1",date(2026,7,1),2),2)
            with self.assertRaises(ValueError):
                change_coffee_days(db,1,"P1",date(2026,7,1),1,2,True)
            self.assertEqual(change_coffee_days(db,1,"P1",date(2026,7,1),-1,2,True),1)
            self.assertEqual(change_coffee_days(db,1,"P1",date(2026,7,1),-1,2,True),0)
            db.commit()
            self.assertEqual(coffee_count(db,1,"P1",date(2026,7,1),2),0)
            for delta, enabled in [(-1,True),(3,True),(1,False)]:
                with self.assertRaises(ValueError):
                    change_coffee_days(db,1,"P1",date(2026,7,1),delta,2,enabled)
                db.rollback()
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM coffee_days_audit")).scalar(),2)
            db.execute(text("INSERT INTO monthly_submissions (merchant_id,month_key,status) VALUES (1,'2026-07-01','submitted')"))
            db.commit()
            with self.assertRaises(ValueError):
                change_coffee_days(db,1,"P1",date(2026,7,1),1,2,True)
            db.rollback()
            self.assertEqual(coffee_count(db,1,"P1",date(2026,6,1),8),8)

    def test_coffee_route_recalculates_calendar_and_report(self):
        with self.factory() as db:
            db.execute(text("INSERT INTO visits VALUES (1,1,'P1','2026-07-01','DAY')"))
            db.commit()
        response = self.client.post("/coffee-days",data={"fio":"Тестовый Мерч","point_code":"P1","delta":-1,"csrf_token":self.csrf},follow_redirects=False)
        self.assertEqual(response.status_code,303)
        self.assertIn("saved=1",response.headers["location"])
        page = self.client.get(response.headers["location"])
        self.assertEqual(page.status_code,200)
        self.assertIn("Дни с кофемашиной",page.text)
        self.assertNotIn('<div class="detail-title">Кофемашина</div>',page.text)
        self.assertEqual(page.text.count('class="detail-card coffee-days-card"'),1)
        coffee_card = page.text.split('class="detail-card coffee-days-card"',1)[1].split('</form>',1)[0]
        self.assertIn('0 дней × 100 ₽ = 0 ₽',coffee_card)
        self.assertIn('>−1 день</button>',coffee_card)
        self.assertIn('>+1 день</button>',coffee_card)
        with self.factory() as db:
            self.assertEqual(services.compute_point_total(db,1,"P1",2026,7)["total"],400)
            self.assertEqual(services.get_admin_report_rows(db,2026,7)[0]["point_total"],400)
        response = self.client.post("/coffee-days",data={"fio":"Тестовый Мерч","point_code":"P1","delta":1,"csrf_token":self.csrf},follow_redirects=False)
        self.assertEqual(response.status_code,303)
        page = self.client.get(response.headers["location"])
        self.assertIn('1 день × 100 ₽ = 100 ₽',page.text)
        with self.factory() as db:
            self.assertEqual(services.compute_point_total(db,1,"P1",2026,7)["total"],500)

    def test_coffee_ui_hidden_when_disabled_and_locked_after_submit(self):
        with self.factory() as db:
            db.execute(text("UPDATE point_rates SET coffee_enabled=FALSE"))
            db.commit()
        url = "/calendar-page?fio=Тестовый Мерч&point_code=P1"
        self.assertNotIn('action="/coffee-days"',self.client.get(url).text)
        with self.factory() as db:
            db.execute(text("UPDATE point_rates SET coffee_enabled=TRUE"))
            db.execute(text("INSERT INTO monthly_submissions (merchant_id,month_key,status) VALUES (1,'2026-07-01','submitted')"))
            db.commit()
        self.assertNotIn('action="/coffee-days"',self.client.get(url).text)
        result=self.client.post("/coffee-days",data={"fio":"Тестовый Мерч","point_code":"P1","delta":1,"csrf_token":self.csrf},follow_redirects=False)
        self.assertEqual(result.status_code,303)
        self.assertIn("coffee_error=1",result.headers["location"])

    def test_reset_real_database_error_rolls_back_credentials_and_audit(self):
        with self.factory() as db:
            db.execute(text("INSERT INTO merchant_audit_log VALUES ('a',1,'created','admin','[]',CURRENT_TIMESTAMP)"))
            db.execute(text("CREATE TRIGGER reject_reset BEFORE UPDATE ON merchants BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END"))
            db.commit()
        admin = main.get_admin_cookie_value()
        self.client.cookies.set("admin_auth",admin)
        result=self.client.post("/admin-delete-all-merchants",data={"confirmation":"УДАЛИТЬ ВСЕХ МЕРЧЕНДАЙЗЕРОВ","csrf_token":main.get_admin_csrf_token(admin)},follow_redirects=False)
        self.assertEqual(result.status_code,303)
        self.assertIn("error=",result.headers["location"])
        with self.factory() as db:
            self.assertEqual(db.execute(text("SELECT fio FROM merchants WHERE id=1")).scalar(),"Тестовый Мерч")
            self.assertEqual(db.execute(text("SELECT COUNT(*) FROM merchant_audit_log")).scalar(),1)

    def test_notes_and_reimbursements_without_visits_reach_all_totals_and_exports(self):
        for note, reimbursement in ((500,0),(0,150),(500,150),(800,0)):
            with self.subTest(note=note,reimbursement=reimbursement):
                with self.factory() as db:
                    db.execute(text("DELETE FROM point_adjustments"))
                    services.upsert_point_adjustment(db,1,"P1",2026,7,note,"500 ₽ — Первый\n300 ₽ — Второй" if note==800 else "Примечание",reimbursement,"Возмещение",None)
                    self.assertEqual(services.compute_point_total(db,1,"P1",2026,7)["total"],note+reimbursement)
                    self.assertEqual(services.compute_overall_total(db,1,2026,7)["total"],note+reimbursement)
                    self.assertEqual(services.get_admin_report_rows(db,2026,7)[0]["point_total"],note+reimbursement)
                    self.assertEqual(services.get_admin_payroll_rows(db,2026,7)[0]["clean_total"],note+reimbursement)
                self.client.cookies.set("admin_auth",main.get_admin_cookie_value())
                monthly = self.client.get("/monthly-submit-page?fio=Тестовый Мерч")
                self.assertEqual(monthly.status_code,200)
                self.assertIn(str(note+reimbursement),monthly.text)
                for url in ("/admin-export-check","/admin-export-payroll"):
                    response = self.client.get(url+"?year=2026&month=7")
                    self.assertEqual(response.status_code,200)
                    values = list(load_workbook(BytesIO(response.content)).active.values)
                    self.assertIn(note+reimbursement,values[1])

    def test_reset_retains_historical_report_identity_and_rejects_old_session(self):
        with self.factory() as db:
            services.upsert_point_adjustment(db,1,"P1",2026,7,500,"Примечание",0,"",None)
            before = services.get_admin_report_rows(db,2026,7)
            delete_all_merchants_and_data(db)
            db.commit()
            self.assertEqual(services.get_admin_report_rows(db,2026,7),before)
            self.assertEqual(db.execute(text("SELECT merchant_id FROM point_adjustments")).scalar(),1)
            db.execute(text("INSERT INTO merchants (fio,fio_norm,pass_hash,tu) VALUES ('Тестовый Мерч','тестовый мерч',:hash,'ТУ-1')"),{"hash":services.hash_last4("1234")})
            db.commit()
            context = set_request_merchant("тестовый мерч",0)
            try:
                self.assertIsNone(services.get_merchant_by_fio(db,"Тестовый Мерч"))
            finally:
                reset_request_merchant(context)


if __name__ == "__main__":
    unittest.main()
