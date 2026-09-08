"""Explicit coffee days; absent values retain the existing monthly calculation."""

import uuid
from sqlalchemy import bindparam, inspect, text


def ensure_coffee_days_schema(db):
    id_type = "SERIAL PRIMARY KEY" if db.get_bind().dialect.name == "postgresql" else "INTEGER PRIMARY KEY"
    db.execute(text(f"""
        CREATE TABLE IF NOT EXISTS coffee_bonus (
            id {id_type},
            merchant_id INTEGER NOT NULL, point_code TEXT NOT NULL,
            month_key DATE NOT NULL, enabled BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (merchant_id, point_code, month_key)
        )
    """))
    columns = {c["name"] for c in inspect(db.connection()).get_columns("coffee_bonus")}
    if "days_count" not in columns:
        db.execute(text("ALTER TABLE coffee_bonus ADD COLUMN days_count INTEGER CHECK (days_count >= 0)"))
    if "manual_delta" not in columns:
        db.execute(text("ALTER TABLE coffee_bonus ADD COLUMN manual_delta INTEGER"))
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS coffee_days_audit (
            id TEXT PRIMARY KEY, merchant_id INTEGER NOT NULL,
            point_code TEXT NOT NULL, month_key DATE NOT NULL,
            old_count INTEGER NOT NULL, new_count INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """))
    db.commit()


def coffee_count(db, merchant_id, point_code, month_key, eligible):
    stored = db.execute(text("""
        SELECT days_count FROM coffee_bonus
        WHERE merchant_id=:merchant_id AND point_code=:point_code AND month_key=:month_key
    """), dict(merchant_id=merchant_id, point_code=point_code, month_key=month_key)).scalar()
    return eligible if stored is None else min(eligible, max(0, int(stored)))


def change_coffee_days(db, merchant_id, point_code, month_key, delta, eligible, enabled):
    """Caller commits or rolls back, including the audit event."""
    if delta not in (-1, 1) or not enabled:
        raise ValueError("Изменение дней кофемашины недоступно для этой точки.")
    if db.get_bind().dialect.name == "postgresql":
        db.execute(text("SELECT id FROM merchants WHERE id=:id FOR UPDATE"), {"id": merchant_id})
    status = db.execute(text("""
        SELECT status FROM monthly_submissions
        WHERE merchant_id=:merchant_id AND month_key=:month_key
    """), dict(merchant_id=merchant_id, month_key=month_key)).scalar()
    if status == "submitted":
        raise ValueError("Сверка уже отправлена. Изменение дней кофемашины запрещено.")
    old = coffee_count(db, merchant_id, point_code, month_key, eligible)
    new = old + delta
    if not 0 <= new <= eligible:
        raise ValueError(f"Количество дней кофемашины должно быть от 0 до {eligible}.")
    params = dict(merchant_id=merchant_id, point_code=point_code, month_key=month_key, new=new, manual_delta=new-eligible)
    db.execute(text("""
        INSERT INTO coffee_bonus (merchant_id, point_code, month_key, enabled, days_count, manual_delta)
        VALUES (:merchant_id, :point_code, :month_key, TRUE, :new, :manual_delta)
        ON CONFLICT (merchant_id, point_code, month_key) DO UPDATE
        SET days_count=:new, manual_delta=:manual_delta, updated_at=CURRENT_TIMESTAMP
    """), params)
    db.execute(text("""
        INSERT INTO coffee_days_audit (id, merchant_id, point_code, month_key, old_count, new_count)
        VALUES (:id, :merchant_id, :point_code, :month_key, :old, :new)
    """), {**params, "id": uuid.uuid4().hex, "old": old})
    return new


def eligible_coffee_days(db, merchant_id, point_code, month_key):
    from app.services import REGULAR_PAY_SLOTS, month_end_exclusive
    return int(db.execute(text("""
        SELECT COUNT(DISTINCT visit_date) FROM visits
        WHERE merchant_id=:merchant_id AND point_code=:point_code
          AND visit_date>=:month_key AND visit_date<:end
          AND slot IN :slots
    """).bindparams(bindparam("slots", expanding=True)), dict(
        merchant_id=merchant_id, point_code=point_code, month_key=month_key,
        end=month_end_exclusive(month_key.year, month_key.month), slots=tuple(REGULAR_PAY_SLOTS),
    )).scalar() or 0)


def sync_coffee_after_visit(db, merchant_id, point_code, month_key, eligible_before):
    """Materialize the count for unchanged reports; caller owns the visit transaction.

    NULL delta is initialized lazily from the existing visible count, never by
    backfilling old months. Keep the delta even when the displayed count hits 0.
    """
    params = dict(merchant_id=merchant_id, point_code=point_code, month_key=month_key)
    row = db.execute(text("""
        SELECT days_count, manual_delta FROM coffee_bonus
        WHERE merchant_id=:merchant_id AND point_code=:point_code AND month_key=:month_key
    """), params).mappings().first()
    offset = row["manual_delta"] if row else None
    if offset is None:
        stored = row["days_count"] if row else None
        offset = 0 if stored is None else min(eligible_before, max(0, int(stored))) - eligible_before
    eligible_after = eligible_coffee_days(db, merchant_id, point_code, month_key)
    new = min(eligible_after, max(0, eligible_after + int(offset)))
    db.execute(text("""
        INSERT INTO coffee_bonus (merchant_id, point_code, month_key, enabled, days_count, manual_delta)
        VALUES (:merchant_id, :point_code, :month_key, TRUE, :new, :offset)
        ON CONFLICT (merchant_id, point_code, month_key) DO UPDATE
        SET days_count=:new, manual_delta=:offset, updated_at=CURRENT_TIMESTAMP
    """), {**params, "new":new, "offset":offset})
