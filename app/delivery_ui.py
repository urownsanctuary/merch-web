"""Server-rendered confirmation flow; no visit is persisted on either question."""
from datetime import date
from html import escape
import uuid
import logging
from urllib.parse import urlencode

from fastapi.responses import HTMLResponse, RedirectResponse

from app import delivery_roles as roles
from app.security import sign_visit_action, read_visit_action


def action_input(csrf, point, day, slot, adding):
    token = sign_visit_action({"csrf": csrf, "point": point, "day": day.isoformat(),
                              "slot": slot, "adding": adding, "key": uuid.uuid4().hex})
    return f'<input type="hidden" name="visit_action" value="{escape(token)}" />'


def _question(*, stage, payload, fio, csrf, base_css, non_working, inventory):
    token = sign_visit_action(payload)
    action = "/toggle-inventory" if inventory else "/toggle-day"
    hidden = {"fio": fio, "point_code": payload["point"], "day": date.fromisoformat(payload["day"]).day,
              "slot": payload["slot"], "csrf_token": csrf, "visit_action": token,
              "confirm_non_working": non_working}
    fields = "".join(f'<input type="hidden" name="{k}" value="{escape(str(v))}" />' for k, v in hidden.items())
    back = "/calendar-page?" + urlencode({"fio": fio, "point_code": payload["point"]})
    if stage == "confirm":
        title = "В этот день другой мерчендайзер уже отметил выход на эту ТТ. Вы уверены, что тоже выходили в этот день?"
        buttons = '<button class="btn" name="delivery_choice" value="yes">Да</button>'
    else:
        title = "Как проходил ваш выход?"
        buttons = '''<button class="btn" name="delivery_choice" value="no_supply">Утренний выход — поставку не принимал</button>
            <p>Вышел с утра, но не дождался водителя / не принимал поставку</p>
            <button class="btn" name="delivery_choice" value="receiver">Дополнительный выход для принятия поставки</button>
            <p>С утра на точку выходил другой мерчендайзер, а я приехал дополнительно для принятия поставки</p>'''
    return HTMLResponse(f'''<!doctype html><html lang="ru"><head><meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1"><title>Подтверждение выхода</title>{base_css()}
        <style>.btn{{white-space:normal;overflow-wrap:anywhere;min-height:48px}}</style></head>
        <body><div class="page"><div class="card"><h1>{title}</h1>
        <form method="post" action="{action}">{fields}{buttons}</form>
        <a class="btn btn-secondary" href="{escape(back)}">Нет — вернуться в календарь</a>
        </div></div></body></html>''', headers={"Cache-Control": "no-store"})


def handle_visit(*, db, merchant, point, day, slot, csrf, action, choice,
                 non_working, base_css, special_inventory=False):
    """Return None only for legacy installations awaiting the explicit migration."""
    from app.services import (toggle_day_visit, toggle_inventory_visit, SLOT_FULL_INVENT,
                              REGULAR_PAY_SLOTS, InventoryWeekLimitError)
    from app.coffee_days import sync_coffee_after_visit
    ready = roles.available(db)
    if not roles.enabled():
        if ready and roles.current_roles(db, point, day):
            db.rollback()
            return HTMLResponse("Изменение подтверждённых выходов временно отключено.", status_code=409)
        return None
    if not ready:
        db.rollback()
        return HTMLResponse("Подтверждение совместного выхода временно недоступно. Обратитесь к администратору.", status_code=503)
    payload = read_visit_action(action, csrf) if action else None
    if action and (not payload or any(payload.get(k) != v for k, v in
                      {"point": point, "day": day.isoformat(), "slot": slot}.items())):
        db.rollback()
        return HTMLResponse("Форма устарела. Откройте календарь заново.", status_code=409)
    back = "/calendar-page?" + urlencode({"fio": merchant["fio"], "point_code": point})
    try:
        roles.lock_day(db, point, day)
        rows = roles.day_visits(db, point, day)
        existing = next((r for r in rows if r["merchant_id"] == merchant["id"] and r["slot"] == slot), None)
        # Old API callers receive the same confirmation screens. Only forms
        # signed by the server can authorize a delivery decision.
        if payload is None:
            payload = {"csrf": csrf, "point": point, "day": day.isoformat(), "slot": slot,
                       "adding": existing is None, "key": uuid.uuid4().hex}
            choice = ""
        old_event = roles.event_for_request(db, payload["key"])
        if old_event:
            db.rollback()
            if old_event.startswith("EXCEPTION_"):
                return HTMLResponse("Этот случай уже передан администратору. Выход не сохранён.", status_code=409)
            return RedirectResponse(back, status_code=303)
        roles.lock_merchants(db, [merchant["id"]] + [r["merchant_id"] for r in rows])
        if roles.submitted(db, merchant["id"], day):
            db.rollback()
            return HTMLResponse("Ваша сверка уже отправлена.", status_code=409)
        if bool(payload["adding"]) == bool(existing):
            db.rollback()
            return RedirectResponse(back, status_code=303)
        if payload["adding"]:
            peers = [[r["id"], r["merchant_id"], r["slot"]] for r in rows if r["merchant_id"] != merchant["id"]]
            if "peers" in payload and payload["peers"] != peers:
                db.rollback()
                return HTMLResponse("Отметки по этому дню изменились. Откройте календарь заново; если принятие поставки уже подтверждено, обратитесь к администратору.", status_code=409)
            if choice and not payload.get("stage"):
                db.rollback()
                return HTMLResponse("Сначала подтвердите совместный выход.", status_code=409)
            if choice in {"no_supply", "receiver"} and payload.get("stage") != "role":
                db.rollback()
                return HTMLResponse("Сначала выберите тип выхода на странице подтверждения.", status_code=409)
            decision = roles.decide(db, merchant_id=merchant["id"], point=point, day=day,
                                    slot=slot, choice=choice, key=payload["key"])
            stage = decision["stage"]
            if stage in {"confirm", "role"}:
                db.rollback()
                return _question(stage=stage, payload={**payload, "peers": peers, "stage": stage},
                                 fio=merchant["fio"], csrf=csrf, base_css=base_css,
                                 non_working=non_working, inventory=slot == SLOT_FULL_INVENT)
            if stage in {"exception", "blocked"}:
                if stage == "exception":
                    db.commit()
                else:
                    db.rollback()
                return HTMLResponse(escape(decision["message"]), status_code=409)
        if not payload['adding']:
            error = roles.remove_role(db, merchant_id=merchant['id'], point=point, day=day, visit=existing, key=payload['key'])
            if error:
                db.commit()
                return HTMLResponse(escape(error), status_code=409)
        if slot == SLOT_FULL_INVENT:
            toggle_inventory_visit(db, merchant["id"], point, day.year, day.month, day.day,
                                   special_inventory=special_inventory, commit=False)
        else:
            toggle_day_visit(db, merchant["id"], point, day.year, day.month, day.day, slot, commit=False)
        if payload["adding"]:
            roles.complete(db, decision=decision, merchant_id=merchant["id"], point=point,
                           day=day, slot=slot, key=payload["key"])
        else:
            roles.record_event(db, key=payload["key"], kind="VISIT_REMOVED", point=point,
                               day=day, second=merchant["id"], second_visit=existing["id"])
        if slot in REGULAR_PAY_SLOTS:
            from app.services import get_point_rates
            if get_point_rates(db, point, day.year, day.month)["coffee_enabled"]:
                sync_coffee_after_visit(db, merchant["id"], point, day.replace(day=1))
        db.commit()
        return RedirectResponse(back, status_code=303)
    except InventoryWeekLimitError as exc:
        db.rollback()
        return RedirectResponse(back + "&" + urlencode({"visit_error": "inventory_week", "inventory_date": exc.existing_date.isoformat()}), status_code=303)
    except Exception as exc:
        db.rollback()
        logging.getLogger(__name__).warning("delivery_visit_failed error_type=%s", type(exc).__name__)
        # Never return driver exceptions, SQL parameters or snapshots to a user.
        return HTMLResponse("Не удалось сохранить выход. Данные не изменены; повторите попытку.", status_code=503)
