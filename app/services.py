import os
import re
import hashlib
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text

SECRET_SALT = os.getenv("SECRET_SALT")

if not SECRET_SALT:
    raise RuntimeError("SECRET_SALT is not set")

SLOT_DAY = "DAY"
SLOT_FULL_INVENT = "FULL_INVENT"

DEFAULT_RATE_SUPPLY = 800
DEFAULT_RATE_NO_SUPPLY = 400
DEFAULT_RATE_INVENTORY = 400
DEFAULT_RATE_COFFEE = 100


def fio_norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"[\u00A0\u2000-\u200B\u202F\u205F\u3000]", " ", s)
    s = re.sub(r"[^а-яa-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def hash_last4(last4: str) -> str:
    s = (last4.strip() + SECRET_SALT).encode("utf-8")
    return hashlib.sha256(s).hexdigest()


def get_active_period():
    today = date.today()

    if today.day <= 5:
        if today.month == 1:
            report_year = today.year - 1
            report_month = 12
        else:
            report_year = today.year
            report_month = today.month - 1
    else:
        report_year = today.year
        report_month = today.month

    editable_until_year = report_year
    editable_until_month = report_month + 1
    if editable_until_month == 13:
        editable_until_month = 1
        editable_until_year += 1

    editable_until = date(editable_until_year, editable_until_month, 5)

    return {
        "year": report_year,
        "month": report_month,
        "editable_until": editable_until.isoformat()
    }


def get_merchants_columns(db: Session):
    query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'merchants'
        ORDER BY ordinal_position
    """)
    rows = db.execute(query).fetchall()
    return [row[0] for row in rows]


def login_user(db: Session, fio: str, last4: str):
    fio_n = fio_norm(fio)

    query = text("""
        SELECT id, fio, fio_norm, pass_hash, telegram_id, tu, created_at
        FROM merchants
        WHERE fio_norm = :fio_norm
        LIMIT 1
    """)

    result = db.execute(query, {
        "fio_norm": fio_n
    }).mappings().first()

    if not result:
        return None

    incoming_hash = hash_last4(last4)

    if incoming_hash != result["pass_hash"]:
        return None

    return {
        "id": result["id"],
        "fio": result["fio"],
        "fio_norm": result["fio_norm"],
        "telegram_id": result["telegram_id"],
        "tu": result["tu"],
        "created_at": str(result["created_at"]) if result["created_at"] else None
    }


def get_merchant_by_fio(db: Session, fio: str):
    fio_n = fio_norm(fio)

    result = db.execute(text("""
        SELECT id, fio, fio_norm, telegram_id, tu, created_at
        FROM merchants
        WHERE fio_norm = :fio_norm
        LIMIT 1
    """), {
        "fio_norm": fio_n
    }).mappings().first()

    if not result:
        return None

    return {
        "id": result["id"],
        "fio": result["fio"],
        "fio_norm": result["fio_norm"],
        "telegram_id": result["telegram_id"],
        "tu": result["tu"],
        "created_at": str(result["created_at"]) if result["created_at"] else None
    }


def month_start(y: int, m: int) -> date:
    return date(y, m, 1)


def month_end_exclusive(y: int, m: int) -> date:
    return date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)


def days_in_month(y: int, m: int) -> int:
    return (month_end_exclusive(y, m) - timedelta(days=1)).day


def weekday_of(y: int, m: int, d: int) -> int:
    return date(y, m, d).weekday()


def month_title(y: int, m: int) -> str:
    names = [
        "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
    ]
    return f"{names[m - 1]} {y}"


def normalize_point_code(v) -> str:
    s = str(v or "").strip()
    s = re.sub(r"\s+", "", s)
    return s


def point_has_any_supply_in_month(db: Session, point_code: str, y: int, m: int) -> bool:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)

    result = db.execute(text("""
        SELECT 1
        FROM supplies
        WHERE point_code = :point_code
          AND supply_date >= :start_date
          AND supply_date < :end_date
        LIMIT 1
    """), {
        "point_code": point_code,
        "start_date": start,
        "end_date": end
    }).first()

    return result is not None


def get_supply_boxes_map(db: Session, point_code: str, y: int, m: int) -> dict[int, int]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)

    rows = db.execute(text("""
        SELECT supply_date, boxes
        FROM supplies
        WHERE point_code = :point_code
          AND supply_date >= :start_date
          AND supply_date < :end_date
        ORDER BY supply_date
    """), {
        "point_code": point_code,
        "start_date": start,
        "end_date": end
    }).mappings().all()

    result: dict[int, int] = {}
    for row in rows:
        day = row["supply_date"].day
        result[day] = int(row["boxes"] or 0)

    return result


def get_visits_for_month(db: Session, merchant_id: int, point_code: str, y: int, m: int) -> dict[int, set[str]]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)

    rows = db.execute(text("""
        SELECT visit_date, slot
        FROM visits
        WHERE merchant_id = :merchant_id
          AND point_code = :point_code
          AND visit_date >= :start_date
          AND visit_date < :end_date
    """), {
        "merchant_id": merchant_id,
        "point_code": point_code,
        "start_date": start,
        "end_date": end
    }).mappings().all()

    result: dict[int, set[str]] = {}
    for row in rows:
        day = row["visit_date"].day
        result.setdefault(day, set()).add(str(row["slot"]))

    return result


def toggle_day_visit(db: Session, merchant_id: int, point_code: str, y: int, m: int, day: int):
    visit_date = date(y, m, day)

    existing = db.execute(text("""
        SELECT id
        FROM visits
        WHERE merchant_id = :merchant_id
          AND point_code = :point_code
          AND visit_date = :visit_date
          AND slot = :slot
        LIMIT 1
    """), {
        "merchant_id": merchant_id,
        "point_code": point_code,
        "visit_date": visit_date,
        "slot": SLOT_DAY
    }).scalar()

    if existing:
        db.execute(text("DELETE FROM visits WHERE id = :id"), {"id": existing})
        db.commit()
        return "removed"

    db.execute(text("""
        INSERT INTO visits (merchant_id, point_code, visit_date, slot)
        VALUES (:merchant_id, :point_code, :visit_date, :slot)
        ON CONFLICT DO NOTHING
    """), {
        "merchant_id": merchant_id,
        "point_code": point_code,
        "visit_date": visit_date,
        "slot": SLOT_DAY
    })
    db.commit()
    return "added"


def toggle_inventory_visit(db: Session, merchant_id: int, point_code: str, y: int, m: int, day: int):
    visit_date = date(y, m, day)

    existing = db.execute(text("""
        SELECT id
        FROM visits
        WHERE merchant_id = :merchant_id
          AND point_code = :point_code
          AND visit_date = :visit_date
          AND slot = :slot
        LIMIT 1
    """), {
        "merchant_id": merchant_id,
        "point_code": point_code,
        "visit_date": visit_date,
        "slot": SLOT_FULL_INVENT
    }).scalar()

    if existing:
        db.execute(text("DELETE FROM visits WHERE id = :id"), {"id": existing})
        db.commit()
        return "removed"

    db.execute(text("""
        INSERT INTO visits (merchant_id, point_code, visit_date, slot)
        VALUES (:merchant_id, :point_code, :visit_date, :slot)
        ON CONFLICT DO NOTHING
    """), {
        "merchant_id": merchant_id,
        "point_code": point_code,
        "visit_date": visit_date,
        "slot": SLOT_FULL_INVENT
    })
    db.commit()
    return "added"


def get_point_rates(db: Session, point_code: str, y: int, m: int):
    mk = month_start(y, m)

    row = db.execute(text("""
        SELECT
            rate_supply,
            rate_no_supply,
            rate_inventory,
            coffee_enabled,
            coffee_rate,
            pay_lt5
        FROM point_rates
        WHERE point_code = :point_code
          AND month_key = :month_key
        LIMIT 1
    """), {
        "point_code": point_code,
        "month_key": mk
    }).mappings().first()

    if not row:
        return {
            "rate_supply": DEFAULT_RATE_SUPPLY,
            "rate_no_supply": DEFAULT_RATE_NO_SUPPLY,
            "rate_inventory": DEFAULT_RATE_INVENTORY,
            "coffee_enabled": False,
            "coffee_rate": DEFAULT_RATE_COFFEE,
            "pay_lt5": False,
        }

    return {
        "rate_supply": int(row["rate_supply"] or DEFAULT_RATE_SUPPLY),
        "rate_no_supply": int(row["rate_no_supply"] or DEFAULT_RATE_NO_SUPPLY),
        "rate_inventory": int(row["rate_inventory"] or DEFAULT_RATE_INVENTORY),
        "coffee_enabled": bool(row["coffee_enabled"]),
        "coffee_rate": int(row["coffee_rate"] or DEFAULT_RATE_COFFEE),
        "pay_lt5": bool(row["pay_lt5"]),
    }


def effective_has_supply(boxes: int, pay_lt5: bool) -> bool:
    if boxes <= 0:
        return False
    return True if pay_lt5 else (boxes >= 5)


def compute_point_total(db: Session, merchant_id: int, point_code: str, y: int, m: int):
    boxes_map = get_supply_boxes_map(db, point_code, y, m)
    visits = get_visits_for_month(db, merchant_id, point_code, y, m)
    rates = get_point_rates(db, point_code, y, m)

    total = 0
    cnt_supply = 0
    cnt_no_supply = 0
    cnt_day_total = 0
    cnt_full_inv = 0

    for day, slots in visits.items():
        if SLOT_DAY in slots:
            cnt_day_total += 1
            boxes = boxes_map.get(day, 0)
            if effective_has_supply(boxes, rates["pay_lt5"]):
                cnt_supply += 1
                total += rates["rate_supply"]
            else:
                cnt_no_supply += 1
                total += rates["rate_no_supply"]

        if SLOT_FULL_INVENT in slots:
            cnt_full_inv += 1
            total += rates["rate_inventory"]

    coffee_sum = 0
    if rates["coffee_enabled"] and cnt_day_total > 0:
        coffee_sum = rates["coffee_rate"] * cnt_day_total
        total += coffee_sum

    return {
        "total": total,
        "cnt_supply": cnt_supply,
        "cnt_no_supply": cnt_no_supply,
        "cnt_day_total": cnt_day_total,
        "cnt_full_inv": cnt_full_inv,
        "coffee_enabled": rates["coffee_enabled"],
        "coffee_rate": rates["coffee_rate"],
        "coffee_sum": coffee_sum,
        "pay_lt5": rates["pay_lt5"],
        "rate_supply": rates["rate_supply"],
        "rate_no_supply": rates["rate_no_supply"],
        "rate_inventory": rates["rate_inventory"],
    }


def get_points_for_month(db: Session, merchant_id: int, y: int, m: int) -> list[str]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)

    rows = db.execute(text("""
        SELECT DISTINCT point_code
        FROM visits
        WHERE merchant_id = :merchant_id
          AND visit_date >= :start_date
          AND visit_date < :end_date
        ORDER BY point_code
    """), {
        "merchant_id": merchant_id,
        "start_date": start,
        "end_date": end
    }).all()

    return [r[0] for r in rows if r and r[0]]


def compute_overall_total(db: Session, merchant_id: int, y: int, m: int):
    points = get_points_for_month(db, merchant_id, y, m)
    total = 0
    per_point: dict[str, int] = {}

    for point_code in points:
        point_result = compute_point_total(db, merchant_id, point_code, y, m)
        per_point[point_code] = point_result["total"]
        total += point_result["total"]

    return {
        "total": total,
        "per_point": per_point
    }
