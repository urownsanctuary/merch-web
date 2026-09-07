"""Explicit coffee days; absent values retain the existing monthly calculation."""

import uuid
from sqlalchemy import inspect, text


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
    params = dict(merchant_id=merchant_id, point_code=point_code, month_key=month_key, new=new)
    db.execute(text("""
        INSERT INTO coffee_bonus (merchant_id, point_code, month_key, enabled, days_count)
        VALUES (:merchant_id, :point_code, :month_key, TRUE, :new)
        ON CONFLICT (merchant_id, point_code, month_key) DO UPDATE
        SET days_count=:new, updated_at=CURRENT_TIMESTAMP
    """), params)
    db.execute(text("""
        INSERT INTO coffee_days_audit (id, merchant_id, point_code, month_key, old_count, new_count)
        VALUES (:id, :merchant_id, :point_code, :month_key, :old, :new)
    """), {**params, "id": uuid.uuid4().hex, "old": old})
    return new
