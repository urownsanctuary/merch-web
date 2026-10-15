"""Presence remains in visits; delivery acceptance is a separate dated role.

No commits or schema mutations here. HTTP callers own the whole transaction.
Inventory slots are never reclassified as paid delivery visits.
"""
from datetime import date
import uuid

from sqlalchemy import inspect, text


def available(db):
    inspector = inspect(db.connection())
    return all(inspector.has_table(t) for t in ("visit_payment_roles", "delivery_events"))


def roles_for_month(db, merchant_id, point_code, year, month):
    if not available(db):
        return {}
    from app.services import month_start, month_end_exclusive
    rows = db.execute(text("""
        SELECT visit_date, role FROM visit_payment_roles
        WHERE merchant_id=:mid AND point_code=:point
          AND visit_date>=:start AND visit_date<:end
    """), {"mid": merchant_id, "point": point_code, "start": month_start(year, month),
             "end": month_end_exclusive(year, month)}).mappings()
    return {date.fromisoformat(str(r["visit_date"])).day: r["role"] for r in rows}


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
    a, b = (_snapshot(db, first), _snapshot(db, second)) if kind == "TRANSFER" else ({"fio": "", "phone": ""}, {"fio": "", "phone": ""})
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


def set_role(db, mid, point, day, role, visit_id, event_id):
    db.execute(text("""
        INSERT INTO visit_payment_roles (merchant_id, point_code, visit_date, role, visit_id, event_id)
        VALUES (:mid,:point,:day,:role,:vid,:event)
        ON CONFLICT (merchant_id, point_code, visit_date) DO UPDATE SET
            role=excluded.role, visit_id=excluded.visit_id, event_id=excluded.event_id
    """), {"mid": mid, "point": point, "day": day, "role": role,
             "vid": visit_id, "event": event_id})


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
    if not others:
        return result
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
    assigned = db.execute(text("""SELECT role FROM visit_payment_roles
        WHERE merchant_id=:mid AND point_code=:point AND visit_date=:day"""),
        {"mid": merchant_id, "point": point, "day": day}).scalar()
    if assigned:
        # Removing and re-adding attendance must not transfer an accepted role.
        return {**result, "kind": "DOUBLE_RESTORED", "role": assigned}
    original_first = db.execute(text("""SELECT id FROM delivery_events
        WHERE point_code=:point AND visit_date=:day AND event_type='DOUBLE_NO_SUPPLY'
        AND first_merchant_id=:mid LIMIT 1"""),
        {"point": point, "day": day, "mid": merchant_id}).scalar()
    if original_first:
        # The first merchant in the no-supply choice was never reclassified.
        # Restoring their attendance must retain the original automatic rate.
        return {**result, "kind": "DOUBLE_RESTORED"}
    if not paid:
        return {**result, "kind": "DOUBLE_NO_DELIVERY", "role": "NO_SUPPLY"}
    if choice == "yes":
        return {**result, "stage": "role"}
    if choice not in {"no_supply", "receiver"}:
        raise ValueError("Invalid delivery choice")
    for mid in ({r["merchant_id"] for r in regular} if choice == "receiver" else set()):
        adjustment = get_point_adjustment(db, mid, point, day.year, day.month, ensure_schema=False) or {}
        if day in no_supply_note_dates(adjustment.get("note_comment"), day.year, day.month):
            return exception("EXCEPTION_LEGACY_NOTE", "По этому дню уже есть корректировка. Обратитесь к администратору; выход не сохранён.")
    if choice == "no_supply":
        return {**result, "kind": "DOUBLE_NO_SUPPLY", "role": "NO_SUPPLY", "first": first}
    confirmed = db.execute(text("""SELECT id FROM delivery_events WHERE point_code=:point
        AND visit_date=:day AND event_type IN ('TRANSFER','DOUBLE_NO_SUPPLY') LIMIT 1"""),
        {"point": point, "day": day}).scalar()
    if confirmed:
        return exception("EXCEPTION_RECEIVER_ASSIGNED", "Принятие поставки уже подтверждено. Обратитесь к администратору; выход не сохранён.")
    if len(regular) != 1:
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
    if role:
        set_role(db, merchant_id, point, day, role, visit_id, event_id)


def events_for_period(db, year, month, *, exceptions=False):
    from app.services import month_start, month_end_exclusive
    if not available(db):
        return []
    predicate = "event_type LIKE 'EXCEPTION_%'" if exceptions else "event_type = 'TRANSFER'"
    return [dict(r) for r in db.execute(text(f"""
        SELECT * FROM delivery_events WHERE visit_date>=:start AND visit_date<:end
        AND {predicate} ORDER BY visit_date, created_at, id
    """), {"start": month_start(year, month), "end": month_end_exclusive(year, month)}).mappings()]
