import os
import io
import unittest
from datetime import date
from decimal import Decimal

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ADMIN_PASSWORD", "test-admin-password")

from fastapi.testclient import TestClient
from openpyxl import Workbook
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.main import app
from app.admin import parse_supply_workbook
from app.security import make_session, read_session
from app.services import (
    SLOT_DAY,
    compute_overall_total,
    compute_point_total,
    effective_has_supply,
    fio_norm,
    is_real_intersection,
    payroll_gross,
)


SCHEMA = """
CREATE TABLE merchants (
 id INTEGER PRIMARY KEY, fio TEXT, fio_norm TEXT, pass_hash TEXT, telegram_id TEXT, tu TEXT, created_at DATE
);
CREATE TABLE supplies (point_code TEXT, supply_date DATE, boxes INTEGER);
CREATE TABLE visits (
 id INTEGER PRIMARY KEY AUTOINCREMENT, merchant_id INTEGER, point_code TEXT, visit_date DATE, slot TEXT,
 UNIQUE(merchant_id, point_code, visit_date, slot)
);
CREATE TABLE point_rates (
 point_code TEXT, month_key DATE, rate_supply INTEGER, rate_no_supply INTEGER, rate_inventory INTEGER,
 coffee_enabled BOOLEAN, coffee_rate INTEGER, pay_lt5 BOOLEAN
);
CREATE TABLE point_notes (
 id INTEGER PRIMARY KEY AUTOINCREMENT, merchant_id INTEGER, point_code TEXT, year INTEGER, month INTEGER,
 amount NUMERIC, comment TEXT, kind TEXT, adjustment_date DATE
);
CREATE TABLE point_reimbursements (
 id INTEGER PRIMARY KEY AUTOINCREMENT, merchant_id INTEGER, point_code TEXT, year INTEGER, month INTEGER,
 amount NUMERIC, comment TEXT
);
CREATE TABLE special_inventory_dates (inventory_date DATE PRIMARY KEY);
CREATE TABLE reconciliation_submissions (
 merchant_id INTEGER, year INTEGER, month INTEGER, reopened_at DATE
);
"""


class ProductionRulesTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        with self.engine.begin() as connection:
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    connection.exec_driver_sql(statement)
            connection.execute(text("""
                INSERT INTO merchants (id, fio, fio_norm, tu) VALUES (1, 'Иванов Иван', 'иванов иван', 'ТУ-1')
            """))
            connection.execute(text("""
                INSERT INTO point_rates
                (point_code, month_key, rate_supply, rate_no_supply, rate_inventory, coffee_enabled, coffee_rate, pay_lt5)
                VALUES ('100', '2026-07-01', 800, 400, 300, 0, 0, 0),
                       ('101', '2026-07-01', 800, 400, 300, 0, 0, 1)
            """))

    def tearDown(self):
        self.engine.dispose()

    def test_fio_normalization_is_case_space_and_yo_insensitive(self):
        self.assertEqual(fio_norm("  ЁЛКИН\u00a0 Иван  "), "елкин иван")

    def test_supply_thresholds_for_one_four_five_and_pay_lt5(self):
        self.assertFalse(effective_has_supply(1, False))
        self.assertFalse(effective_has_supply(4, False))
        self.assertTrue(effective_has_supply(5, False))
        self.assertTrue(effective_has_supply(1, True))

    def test_point_total_and_pay_lt5(self):
        with Session(self.engine) as db:
            db.execute(text("""
                INSERT INTO supplies VALUES
                ('100','2026-07-01',1), ('100','2026-07-02',4), ('100','2026-07-03',5),
                ('101','2026-07-01',1)
            """))
            for point, day in [("100", 1), ("100", 2), ("100", 3), ("101", 1)]:
                db.execute(text("""
                    INSERT INTO visits (merchant_id,point_code,visit_date,slot)
                    VALUES (1,:point,:visit_date,:slot)
                """), {"point": point, "visit_date": date(2026, 7, day), "slot": SLOT_DAY})
            db.commit()
            standard = compute_point_total(db, 1, "100", 2026, 7)
            lt5 = compute_point_total(db, 1, "101", 2026, 7)
        self.assertEqual(standard["cnt_supply"], 1)
        self.assertEqual(standard["cnt_no_supply"], 2)
        self.assertEqual(standard["total"], Decimal("1600"))
        self.assertEqual(lt5["total"], Decimal("800"))

    def test_notes_and_reimbursements_count_without_visits_and_are_not_double_counted(self):
        with Session(self.engine) as db:
            db.execute(text("""
                INSERT INTO point_notes (merchant_id,point_code,year,month,amount,comment)
                VALUES (1,'200',2026,7,125,'note one'), (1,'200',2026,7,75,'note two')
            """))
            db.execute(text("""
                INSERT INTO point_reimbursements (merchant_id,point_code,year,month,amount,comment)
                VALUES (1,'200',2026,7,300,'receipt one'), (1,'200',2026,7,50,'receipt two')
            """))
            db.commit()
            result = compute_overall_total(db, 1, 2026, 7)
            db.execute(text("DELETE FROM point_notes WHERE comment='note one'"))
            db.commit()
            after_delete = compute_overall_total(db, 1, 2026, 7)
        self.assertEqual(result["per_point"]["200"], Decimal("550"))
        self.assertEqual(after_delete["per_point"]["200"], Decimal("425"))

    def test_payroll_rounds_up_and_intersections_require_same_real_slot(self):
        self.assertEqual(payroll_gross(870), 1000)
        self.assertEqual(payroll_gross(871), 1002)
        self.assertTrue(is_real_intersection("MORNING", "MORNING"))
        self.assertFalse(is_real_intersection("MORNING", "EVENING"))
        self.assertFalse(is_real_intersection("DAY", "DAY"))
        self.assertFalse(is_real_intersection("FULL_INVENT", "FULL_INVENT"))

    def test_signed_session_rejects_tampering(self):
        token = make_session("1", "merchant")
        self.assertEqual(read_session(token, "merchant")["sub"], "1")
        self.assertIsNone(read_session(token + "x", "merchant"))
        self.assertIsNone(read_session(token, "admin"))

    def test_large_supply_import_is_parsed_as_one_validated_dataset(self):
        workbook = Workbook(write_only=True)
        sheet = workbook.create_sheet("Поставки")
        sheet.append(["Точка", "Дата поставки", "Количество коробок", "has_supply"])
        for index in range(900):
            sheet.append([f"P{index:04d}", date(2026, 7, (index % 28) + 1), (index % 8) + 1, True])
        output = io.BytesIO()
        workbook.save(output)
        rows = parse_supply_workbook(output.getvalue())
        self.assertEqual(len(rows), 900)
        self.assertEqual(len({row["point_code"] for row in rows}), 900)


class RouteSmokeTests(unittest.TestCase):
    def test_public_login_pages_and_route_inventory(self):
        with TestClient(app) as client:
            self.assertEqual(client.get("/login-page").status_code, 200)
            self.assertEqual(client.get("/admin-login").status_code, 200)
            self.assertEqual(client.get("/menu-page").status_code, 401)
        paths = {route.path for route in app.routes}
        expected = {
            "/login-page", "/admin-login", "/menu-page", "/point-page", "/calendar-page",
            "/day-action-page", "/toggle-day", "/toggle-inventory", "/summary-page",
            "/notes", "/reimbursements", "/submit-reconciliation", "/admin/export.xlsx",
        }
        self.assertTrue(expected.issubset(paths))


if __name__ == "__main__":
    unittest.main()
