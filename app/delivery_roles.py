"""Presence remains in visits; delivery acceptance is a separate dated role.

No commits or schema mutations here. HTTP callers own the whole transaction.
Inventory slots are never reclassified as paid delivery visits.
"""
from datetime import date
import os
import uuid

from sqlalchemy import inspect, text


def enabled():
    return os.getenv("DELIVERY_ROLE_FLOW_ENABLED", "0").strip().lower() in {"1", "true", "yes"}


def active_roles_sql():
    # Only current attendance can participate in payment state. Audit is never
    # consulted by the calculator or by a new attendance decision.
    return """SELECT r.* FROM visit_payment_roles r JOIN visits present
        ON present.id=r.visit_id AND present.merchant_id=r.merchant_id
        AND present.point_code=r.point_code AND present.visit_date=r.visit_date
        WHERE present.slot IN ('DAY','MORNING')"""


def supply_owner_predicate(alias):
    """Shared SQL for personal totals and admin/payroll calculations.

    Untouched legacy days retain their financial results. Once managed, a day
    pays only its active explicit receiver, including the first single visit.
    """
    state = f"({active_roles_sql()})"
    match = f"p.point_code={alias}.point_code AND p.visit_date={alias}.visit_date"
    return f"""(NOT EXISTS (SELECT 1 FROM {state} p WHERE {match})
        OR EXISTS (SELECT 1 FROM {state} p WHERE {match}
                   AND p.merchant_id={alias}.merchant_id AND p.role='RECEIVER'))"""


def available(db):
    inspector = inspect(db.connection())
    return all(inspector.has_table(t) for t in ("visit_payment_roles", "delivery_events"))


def roles_for_month(db, merchant_id, point_code, year, month):
    if not available(db):
        return {}
    from app.services import month_start, month_end_exclusive
    rows = db.execute(text('SELECT visit_date, role FROM (' + active_roles_sql() + ''') state
        WHERE merchant_id=:mid AND point_code=:point AND visit_date>=:start AND visit_date<:end'''),
        dict(mid=merchant_id, point=point_code, start=month_start(year, month), end=month_end_exclusive(year, month))).mappings()
    return {date.fromisoformat(str(r['visit_date'])).day: r['role'] for r in rows}


def payment_roles_for_month(db, merchant_id, point_code, year, month):
    if not available(db):
        return {}
    from app.services import month_start, month_end_exclusive
    rows = db.execute(text("""
        SELECT DISTINCT v.visit_date,
            CASE WHEN """ + supply_owner_predicate("v") + """ THEN 'RECEIVER' ELSE 'NO_SUPPLY' END AS role
        FROM visits v WHERE v.merchant_id=:mid AND v.point_code=:point
          AND v.visit_date>=:start AND v.visit_date<:end
    """), {"mid": merchant_id, "point": point_code, "start": month_start(year, month),
             "end": month_end_exclusive(year, month)}).mappings()
    return {date.fromisoformat(str(r["visit_date"])).day: r["role"] for r in rows}


def note_locked_days(db, merchant_id, point_code, year, month):
    """Keep the old single-visit correction until a joint decision is made."""
    if not available(db):
        return set()
    from app.services import month_start, month_end_exclusive
    rows = db.execute(text('SELECT visit_date FROM (' + active_roles_sql() + ''') state
        WHERE merchant_id=:mid AND point_code=:point AND visit_date>=:start AND visit_date<:end
          AND (role='NO_SUPPLY' OR NOT can_transfer)'''),
        dict(mid=merchant_id, point=point_code, start=month_start(year, month), end=month_end_exclusive(year, month)))
    return {date.fromisoformat(str(r[0])).day for r in rows}


def paid_as_supply(has_supply, role=None):
    # A claim does not manufacture a paid delivery after a supply correction.
    return bool(has_supply and role != "NO_SUPPLY")


def lock_day(db, point_code, day):
    if db.get_bind().dialect.name == "postgresql":
        db.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                   {"key": f"delivery:{point_code}:{day.isoformat()}"})


def lock_merchants(db, ids):
    if db.get_bind().dialect.name == "postgresql":
        for mid in sorted(set(ids)):
            db.execute(text("SELECT id FROM merchants WHERE id=:id FOR UPDATE"), {"id": mid})


def submitted(db, merchant_id, day):
    return db.execute(text("""SELECT status FROM monthly_submissions
        WHERE merchant_id=:id AND month_key=:mk"""),
        {"id": merchant_id, "mk": day.replace(day=1)}).scalar() == "submitted"


def day_visits(db, point_code, day):
    return [dict(r) for r in db.execute(text("""
        SELECT id, merchant_id, slot FROM visits
        WHERE point_code=:point AND visit_date=:day ORDER BY id
    """), {"point": point_code, "day": day}).mappings()]


def event_for_request(db, request_key):
    return db.execute(text("SELECT event_type FROM delivery_events WHERE request_key=:key"),
                      {"key": request_key}).scalar()


def _snapshot(db, mid):
    if mid is None:
        return {"fio": "", "phone": ""}
    return db.execute(text("""SELECT COALESCE(historical_fio, fio) AS fio,
        COALESCE(phone, '') AS phone FROM merchants WHERE id=:id"""),
        {"id": mid}).mappings().one()


def record_event(db, *, key, kind, point, day, second, first=None,
                 first_visit=None, second_visit=None, before=None, after=None,
                 second_rate=None, before_type=None, after_type=None, second_type=None):
    a, b = (_snapshot(db, first), _snapshot(db, second)) if kind in {"TRANSFER", "RECEIVER_RESTORED"} else ({"fio": "", "phone": ""}, {"fio": "", "phone": ""})
    event_id = uuid.uuid4().hex
    db.execute(text("""
        INSERT INTO delivery_events
          (id, request_key, event_type, point_code, visit_date,
           first_merchant_id, second_merchant_id, first_visit_id, second_visit_id,
           first_fio, first_phone, second_fio, second_phone, first_rate_before,
           first_rate_after, second_rate, first_type_before, first_type_after,
           second_type, initiator_id)
        VALUES (:id,:key,:kind,:point,:day,:first,:second,:fv,:sv,:af,:ap,:bf,:bp,
                :before,:after,:rate,:bt,:at,:st,:second)
    """), {"id": event_id, "key": key, "kind": kind, "point": point, "day": day,
             "first": first, "second": second, "fv": first_visit, "sv": second_visit,
             "af": a["fio"], "ap": a["phone"], "bf": b["fio"], "bp": b["phone"],
             "before": before, "after": after, "rate": second_rate,
             "bt": before_type, "at": after_type, "st": second_type})
    return event_id


def set_role(db, mid, point, day, role, visit_id, event_id, *, can_transfer=False, restore_merchant_id=None, restore_visit_id=None):
    db.execute(text("""
        INSERT INTO visit_payment_roles (merchant_id, point_code, visit_date, role, visit_id, event_id, can_transfer, restore_merchant_id, restore_visit_id)
        VALUES (:mid,:point,:day,:role,:vid,:event,:can_transfer,:restore,:restore_visit)
        ON CONFLICT (merchant_id, point_code, visit_date) DO UPDATE SET
            role=excluded.role, visit_id=excluded.visit_id, event_id=excluded.event_id,
            can_transfer=excluded.can_transfer, restore_merchant_id=excluded.restore_merchant_id,
            restore_visit_id=excluded.restore_visit_id
    """), {"mid": mid, "point": point, "day": day, "role": role,
             "vid": visit_id, "event": event_id, "can_transfer": can_transfer, "restore": restore_merchant_id, "restore_visit": restore_visit_id})


def current_roles(db, point, day):
    return {r['merchant_id']: dict(r) for r in db.execute(text(
        "SELECT * FROM (" + active_roles_sql() + ") state WHERE point_code=:point AND visit_date=:day"
    ), {'point': point, 'day': day}).mappings()}


def decide(db, *, merchant_id, point, day, slot, choice, key):
    """Under lock_day, return a decision before inserting any visit.

    Exceptions are durable audit entries, but leave both merchants' visits and
    payments intact. Ambiguous historical data is never silently repaired.
    """
    from app.services import (REGULAR_PAY_SLOTS, effective_has_supply, get_point_rates,
                              get_point_adjustment, no_supply_note_dates)
    rows = day_visits(db, point, day)
    others = [r for r in rows if r["merchant_id"] != merchant_id]
    lock_merchants(db, [merchant_id] + [r["merchant_id"] for r in others])
    if submitted(db, merchant_id, day):
        return {"stage": "blocked", "message": "Ваша сверка уже отправлена. Вернитесь к сверке."}
    rates = get_point_rates(db, point, day.year, day.month)
    boxes = db.execute(text("SELECT boxes FROM supplies WHERE point_code=:point AND supply_date=:day"),
                       {"point": point, "day": day}).scalar() or 0
    paid = effective_has_supply(int(boxes), rates["pay_lt5"])
    result = {"stage": "ready", "rates": rates, "others": others, "paid": paid, "first": None}
    if slot in REGULAR_PAY_SLOTS and any(r['merchant_id'] == merchant_id and r['slot'] in REGULAR_PAY_SLOTS for r in rows):
        return {**result, 'stage': 'blocked', 'message': 'Обычный выход уже отмечен. Сначала удалите прежнюю отметку.'}
    if not others:
        return {**result, "role": "RECEIVER", "can_transfer": True} if slot in REGULAR_PAY_SLOTS else result
    if not choice:
        return {**result, "stage": "confirm"}
    if slot not in REGULAR_PAY_SLOTS:
        return {**result, "kind": "DOUBLE_INVENTORY"}
    regular = [r for r in others if r["slot"] in REGULAR_PAY_SLOTS]
    first = regular[0] if regular else None

    def exception(kind, message):
        record_event(db, key=key, kind=kind, point=point, day=day, second=merchant_id,
                     first=first["merchant_id"] if first else None,
                     first_visit=first["id"] if first else None)
        return {**result, "stage": "exception", "message": message}

    adjustment = get_point_adjustment(db, merchant_id, point, day.year, day.month, ensure_schema=False) or {}
    if day in no_supply_note_dates(adjustment.get("note_comment"), day.year, day.month):
        return exception("EXCEPTION_LEGACY_NOTE", "По этому дню уже есть корректировка. Обратитесь к администратору; выход не сохранён.")
    state = current_roles(db, point, day)
    regular_ids = {r['merchant_id'] for r in regular}
    owners = [r for r in regular if (state.get(r['merchant_id'], {}).get('role') == 'RECEIVER'
              or not state)]
    if len({r['merchant_id'] for r in owners}) > 1 or (state and regular_ids - state.keys()):
        return exception("EXCEPTION_AMBIGUOUS", "Неоднозначные прежние выходы. Требуется администратор; выход не сохранён.")
    first = owners[0] if owners else None
    result['first'] = first
    # An inventory-only peer does not own delivery payment.
    if not regular:
        return {**result, "kind": "DOUBLE_REGULAR", "role": "RECEIVER", "can_transfer": True}
    if first is None:
        return exception("EXCEPTION_AMBIGUOUS", "Не определён текущий принимающий. Требуется администратор; выход не сохранён.")
    if not paid:
        return {**result, "kind": "DOUBLE_NO_DELIVERY", "role": "NO_SUPPLY", "materialize_first": True}
    if choice == "yes":
        return {**result, "stage": "role"}
    if choice not in {"no_supply", "receiver"}:
        raise ValueError("Invalid delivery choice")
    for mid in ({r["merchant_id"] for r in regular} if choice == "receiver" else set()):
        adjustment = get_point_adjustment(db, mid, point, day.year, day.month, ensure_schema=False) or {}
        if day in no_supply_note_dates(adjustment.get("note_comment"), day.year, day.month):
            return exception("EXCEPTION_LEGACY_NOTE", "По этому дню уже есть корректировка. Обратитесь к администратору; выход не сохранён.")
    if choice == "no_supply":
        return {**result, "kind": "DOUBLE_NO_SUPPLY", "role": "NO_SUPPLY", "materialize_first": True}
    if first and first['merchant_id'] in state and not state[first['merchant_id']]['can_transfer']:
        return exception("EXCEPTION_RECEIVER_ASSIGNED", "Принятие поставки уже подтверждено. Обратитесь к администратору; выход не сохранён.")
    if len(regular_ids) != 1:
        return exception("EXCEPTION_AMBIGUOUS", "Нельзя однозначно определить первый выход. Обратитесь к администратору; выход не сохранён.")
    if submitted(db, first["merchant_id"], day):
        return exception("EXCEPTION_SUBMITTED", "Сверка другого мерчендайзера уже подтверждена. Требуется обработка администратором; выход не сохранён.")
    return {**result, "kind": "TRANSFER", "role": "RECEIVER", "first": first}


def complete(db, *, decision, merchant_id, point, day, slot, key):
    """Called after insertion, before coffee sync and the single final commit."""
    kind = decision.get("kind", "VISIT_ADDED")
    visit_id = db.execute(text("""SELECT id FROM visits WHERE merchant_id=:mid
        AND point_code=:point AND visit_date=:day AND slot=:slot ORDER BY id LIMIT 1"""),
        {"mid": merchant_id, "point": point, "day": day, "slot": slot}).scalar_one()
    first = decision.get("first")
    rates = decision["rates"]
    transfer = kind == "TRANSFER"
    role = decision.get("role")
    event_id = record_event(db, key=key, kind=kind, point=point, day=day,
        second=merchant_id, first=first["merchant_id"] if first else None,
        first_visit=first["id"] if first else None, second_visit=visit_id,
        before=rates["rate_supply"] if transfer else None,
        after=rates["rate_no_supply"] if transfer else None,
        second_rate=(rates["rate_supply"] if role == "RECEIVER" and decision["paid"] else rates["rate_no_supply"]) if role else None,
        before_type="SUPPLY" if transfer else None, after_type="NO_SUPPLY" if transfer else None,
        second_type=role or slot)
    if transfer:
        set_role(db, first["merchant_id"], point, day, "NO_SUPPLY", first["id"], event_id)
    elif decision.get('materialize_first'):
        current = current_roles(db, point, day).get(first['merchant_id'], {})
        set_role(db, first['merchant_id'], point, day, 'RECEIVER', first['id'], event_id,
                 restore_merchant_id=current.get('restore_merchant_id'), restore_visit_id=current.get('restore_visit_id'))
    if role:
        set_role(db, merchant_id, point, day, role, visit_id, event_id,
                 can_transfer=decision.get('can_transfer', False),
                 restore_merchant_id=first['merchant_id'] if transfer else None,
                 restore_visit_id=first['id'] if transfer else None)


def remove_role(db, *, merchant_id, point, day, visit, key):
    """Before deleting attendance, under day/merchant locks. No audit reads.

    An unsafe restoration blocks the whole deletion, preserving both finances.
    Inventory removal has no effect on ordinary payment roles.
    """
    from app.services import REGULAR_PAY_SLOTS, get_point_rates, effective_has_supply
    if visit['slot'] not in REGULAR_PAY_SLOTS:
        return None
    state = current_roles(db, point, day)
    role = state.get(merchant_id)
    if not role:
        return None
    rows = [r for r in day_visits(db, point, day) if r['slot'] in REGULAR_PAY_SLOTS and r['id'] != visit['id']]
    remaining_self = next((r for r in rows if r['merchant_id'] == merchant_id), None)
    if remaining_self:
        db.execute(text('UPDATE visit_payment_roles SET visit_id=:vid WHERE merchant_id=:mid AND point_code=:point AND visit_date=:day'),
                   dict(vid=remaining_self['id'], mid=merchant_id, point=point, day=day))
        return None
    target = next((r for r in rows if r['merchant_id'] == role['restore_merchant_id'] and r['id'] == role['restore_visit_id']), None)
    if role['role'] == 'RECEIVER' and rows:
        safe = target and state.get(target['merchant_id'], {}).get('role') == 'NO_SUPPLY' and not submitted(db, target['merchant_id'], day)
        if not safe:
            record_event(db, key=key, kind='EXCEPTION_DELETE_RECEIVER', point=point, day=day,
                         second=merchant_id, first=target['merchant_id'] if target else None,
                         second_visit=visit['id'])
            return 'Нельзя безопасно восстановить оплату другого сотрудника. Требуется администратор; выход не удалён.'
    db.execute(text('DELETE FROM visit_payment_roles WHERE merchant_id=:mid AND point_code=:point AND visit_date=:day'),
               dict(mid=merchant_id, point=point, day=day))
    # A newly added visit by the same person is not the previous fallback.
    db.execute(text('''UPDATE visit_payment_roles SET restore_merchant_id=NULL, restore_visit_id=NULL
        WHERE point_code=:point AND visit_date=:day AND restore_visit_id=:vid'''),
        dict(point=point, day=day, vid=visit['id']))
    if role['role'] == 'RECEIVER' and target:
        rates = get_point_rates(db, point, day.year, day.month)
        boxes = db.execute(text('SELECT boxes FROM supplies WHERE point_code=:point AND supply_date=:day'),dict(point=point,day=day)).scalar() or 0
        restored_rate = rates['rate_supply'] if effective_has_supply(int(boxes), rates['pay_lt5']) else rates['rate_no_supply']
        event_id = record_event(db, key=key + ':restore', kind='RECEIVER_RESTORED', point=point, day=day,
            second=merchant_id, first=target['merchant_id'], first_visit=target['id'], second_visit=visit['id'],
            before=rates['rate_no_supply'], after=restored_rate, second_rate=0,
            before_type='NO_SUPPLY', after_type='RECEIVER', second_type='REMOVED')
        set_role(db, target['merchant_id'], point, day, 'RECEIVER', target['id'], event_id, can_transfer=True)
    return None


def events_for_period(db, year, month, *, exceptions=False):
    from app.services import month_start, month_end_exclusive
    if not available(db):
        return []
    predicate = "event_type LIKE 'EXCEPTION_%'" if exceptions else "event_type IN ('TRANSFER','RECEIVER_RESTORED')"
    return [dict(r) for r in db.execute(text(f"""
        SELECT * FROM delivery_events WHERE visit_date>=:start AND visit_date<:end
        AND {predicate} ORDER BY visit_date, created_at, id
    """), {"start": month_start(year, month), "end": month_end_exclusive(year, month)}).mappings()]
