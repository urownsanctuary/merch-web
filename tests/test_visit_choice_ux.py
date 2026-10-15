"""Visit labels and Sunday navigation must not change persisted inventory types."""
import os
import unittest
from contextlib import ExitStack
from datetime import date
from html.parser import HTMLParser
from unittest.mock import MagicMock, patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("SECRET_SALT", "test-secret-salt-at-least-24-characters")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-at-least-24-characters")
os.environ.setdefault("ENVIRONMENT", "test")

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from app import main, services
from app.security import create_merchant_session


class Forms(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.forms = []
        self.current = None
        self.in_button = False
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "form":
            self.current = {"action": attrs.get("action"), "fields": {}, "button": ""}
        elif self.current is not None:
            if tag == "input":
                self.current["fields"][attrs.get("name")] = attrs.get("value")
            elif tag == "button":
                self.in_button = True

    def handle_data(self, data):
        if self.in_button and self.current is not None:
            self.current["button"] += data

    def handle_endtag(self, tag):
        if tag == "button":
            self.in_button = False
        elif tag == "form" and self.current is not None:
            self.forms.append(self.current)
            self.current = None


class VisitChoiceUXTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.special = MagicMock(return_value=[])
        self.visits = MagicMock(return_value={})
        main.app.dependency_overrides[main.get_db] = lambda: MagicMock()
        self.stack.enter_context(patch.multiple(
            "app.main",
            get_active_period=MagicMock(return_value={"year": 2026, "month": 7, "editable_until": "2026-08-05"}),
            get_merchant_by_fio=MagicMock(return_value={"id": 1, "fio": "Тестовый Сотрудник"}),
            compute_overall_total=MagicMock(return_value={"submission_status": "draft"}),
            get_special_inventory_days=self.special,
            get_visits_for_month=self.visits,
            get_calendar_overrides=MagicMock(return_value={}),
        ))
        self.client = self.stack.enter_context(TestClient(main.app))
        self.client.cookies.set("merchant_session", create_merchant_session("тестовый сотрудник"))

    def tearDown(self):
        self.stack.close()
        main.app.dependency_overrides.clear()

    def page(self, day):
        return self.client.get("/day-action-page", params={"fio": "Тестовый Сотрудник", "point_code": "P1", "day": day}, follow_redirects=False)

    def test_required_day_matrix_and_persisted_form_values(self):
        for day, special, expected in (
            (16, False, None),  # Thursday
            (17, False, ("MORNING", "EVENING")),
            (18, False, ("MORNING", "EVENING")),
            (19, False, ("MORNING", "EVENING")),
            (13, False, None),  # Monday
            (15, True, ("DAY", "FULL_INVENT")),
        ):
            with self.subTest(day=day, special=special):
                self.special.return_value = [date(2026, 7, day)] if special else []
                response = self.page(day)
                if expected is None:
                    self.assertEqual(response.status_code, 303)
                    self.assertIn("/calendar-page", response.headers["location"])
                    continue
                self.assertEqual(response.status_code, 200)
                forms = Forms(response.text).forms
                self.assertEqual([f["button"] for f in forms], ["Утренний выход", "Вечерний выход"])
                self.assertEqual(forms[0]["fields"]["slot"], expected[0])
                self.assertEqual(forms[0]["action"], "/toggle-day")
                if expected[1] == "FULL_INVENT":
                    self.assertEqual(forms[1]["action"], "/toggle-inventory")
                    self.assertNotIn("slot", forms[1]["fields"])
                else:
                    self.assertEqual(forms[1]["action"], "/toggle-day")
                    self.assertEqual(forms[1]["fields"]["slot"], "EVENING")
                self.assertTrue(all(f["fields"]["csrf_token"] for f in forms))

    def test_every_special_date_opens_both_choices(self):
        for day in range(13, 20):
            with self.subTest(day=day):
                self.special.return_value = [date(2026, 7, day)]
                response = self.page(day)
                self.assertEqual(response.status_code, 200)
                self.assertEqual([f["button"] for f in Forms(response.text).forms],
                                 ["Утренний выход", "Вечерний выход"])

    def test_selected_slots_keep_labels_and_show_removal_hint(self):
        for day, slots, special in ((19, {"MORNING", "EVENING"}, False),
                                    (15, {"DAY", "FULL_INVENT"}, True)):
            with self.subTest(day=day):
                self.special.return_value = [date(2026, 7, day)] if special else []
                self.visits.return_value = {day: slots}
                response = self.page(day)
                self.assertEqual([f["button"] for f in Forms(response.text).forms],
                                 ["Утренний выход", "Вечерний выход"])
                self.assertEqual(response.text.count("Отмечен — нажмите, чтобы убрать"), 2)

    def test_legacy_special_sunday_inventory_retains_its_route(self):
        self.special.return_value = [date(2026, 7, 19)]
        self.visits.return_value = {19: {"DAY", "FULL_INVENT"}}
        forms = Forms(self.page(19).text).forms
        self.assertEqual([f["button"] for f in forms], ["Утренний выход", "Вечерний выход"])
        self.assertEqual(forms[0]["fields"]["slot"], "DAY")
        evening = next(f for f in forms if f["button"] == "Вечерний выход")
        self.assertEqual(evening["action"], "/toggle-inventory")
        self.assertNotIn("slot", evening["fields"])

    def test_existing_sunday_day_is_shown_as_selected_morning(self):
        self.visits.return_value = {19: {"DAY"}}
        response = self.page(19)
        forms = Forms(response.text).forms
        self.assertEqual([f["fields"]["slot"] for f in forms], ["DAY", "EVENING"])
        self.assertEqual(response.text.count("Отмечен — нажмите, чтобы убрать"), 1)

    def test_calendar_links_and_badges(self):
        html = main.build_calendar_html("Тестовый Сотрудник", "P1", 2026, 7, {},
            {15: {"DAY", "FULL_INVENT"}, 19: {"MORNING", "EVENING"}}, False,
            {date(2026, 7, 15)}, {}, csrf_token="test-csrf")
        direct_days = {int(f["fields"]["day"]) for f in Forms(html).forms}
        self.assertTrue({13, 16}.issubset(direct_days))
        for day in (15, 17, 18, 19):
            self.assertNotIn(day, direct_days)
            self.assertIn(f"/day-action-page?fio=Тестовый Сотрудник&point_code=P1&day={day}", html)
        self.assertEqual(html.count(">В</span>"), 2)
        self.assertEqual(html.count(">И</span>"), 2)


class VisitChoiceStorageTests(unittest.TestCase):
    def test_sunday_slots_special_inventory_and_weekly_limit(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        try:
            with engine.begin() as con:
                con.exec_driver_sql("CREATE TABLE visits (id INTEGER PRIMARY KEY, merchant_id INTEGER, point_code TEXT, visit_date DATE, slot TEXT, UNIQUE(merchant_id,point_code,visit_date,slot))")
            with Session(engine) as db:
                services.toggle_day_visit(db, 1, "P1", 2026, 7, 19, "MORNING")
                services.toggle_day_visit(db, 1, "P1", 2026, 7, 19, "EVENING")
                self.assertEqual(set(db.execute(text("SELECT slot FROM visits")).scalars()), {"MORNING", "EVENING"})
                with self.assertRaises(services.InventoryWeekLimitError):
                    services.toggle_inventory_visit(db, 1, "P1", 2026, 7, 15, special_inventory=True)
                db.rollback()
                services.toggle_day_visit(db, 2, "P1", 2026, 7, 15, "DAY")
                services.toggle_inventory_visit(db, 2, "P1", 2026, 7, 15, special_inventory=True)
                self.assertEqual(set(db.execute(text("SELECT slot FROM visits WHERE merchant_id=2")).scalars()), {"DAY", "FULL_INVENT"})
                with self.assertRaises(services.InventoryWeekLimitError):
                    services.toggle_day_visit(db, 2, "P1", 2026, 7, 19, "EVENING")
                db.rollback()
                # The old special-Sunday FULL_INVENT endpoint remains compatible.
                services.toggle_inventory_visit(db, 3, "P1", 2026, 7, 19, special_inventory=True)
                self.assertEqual(db.execute(text("SELECT slot FROM visits WHERE merchant_id=3")).scalar_one(), "FULL_INVENT")
                self.assertEqual(services.toggle_inventory_visit(db, 3, "P1", 2026, 7, 19, special_inventory=True), "removed")
        finally:
            engine.dispose()
