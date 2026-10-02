import os
import re
import math
import hashlib
import hmac
from datetime import date, datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import text, bindparam, inspect
from openpyxl import load_workbook
from app.security import request_merchant_matches, request_session_issued_at
from app.runtime import maintenance_mode_enabled
from app.coffee_days import coffee_count, snapshot_coffee_for_submit
from app.merchant_admin import (
    create_merchant,
    deactivate_all_merchants,
    deactivate_merchants_by_tu,
    import_or_reactivate_merchant,
    validate_merchant_values,
)

SECRET_SALT = os.getenv("SECRET_SALT")
if not SECRET_SALT:
    raise RuntimeError("SECRET_SALT is not set")

SLOT_MORNING = "MORNING"
SLOT_EVENING = "EVENING"
SLOT_DAY = "DAY"  # backward-compatible value for records created before explicit slot selection
SLOT_FULL_INVENT = "FULL_INVENT"
VISIT_SLOTS = frozenset({SLOT_MORNING, SLOT_EVENING, SLOT_DAY})
OVERLAP_SLOTS = frozenset({SLOT_MORNING, SLOT_EVENING})
# Report fallback includes different explicit shifts; it does not change calendar badges.
PRESENCE_SLOTS = VISIT_SLOTS
INTERSECTION_CALENDAR_DAY = "CALENDAR_DAY"  # report level, never a stored visit slot
ALL_SLOTS = frozenset({*VISIT_SLOTS, SLOT_FULL_INVENT})
REGULAR_PAY_SLOTS = frozenset({SLOT_DAY, SLOT_MORNING})
INVENTORY_PAY_SLOTS = frozenset({SLOT_EVENING, SLOT_FULL_INVENT})
NORMALIZED_ADJUSTMENT_TABLES = (
    "point_notes",
    "point_reimbursements",
    "reimbursement_receipts",
)

DEFAULT_RATE_SUPPLY = 800
DEFAULT_RATE_NO_SUPPLY = 400
DEFAULT_RATE_INVENTORY = 400
DEFAULT_RATE_COFFEE = 100


class InventoryWeekLimitError(ValueError):
    """Raised when a point already has a full inventory in the same ISO week."""

    def __init__(self, existing_date: date):
        self.existing_date = existing_date
        super().__init__("Full inventory already exists in this ISO week")


def fio_norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"[\u00A0\u2000-\u200B\u202F\u205F\u3000]", " ", s)
    s = re.sub(r"[^а-яa-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def hash_last4(last4: str) -> str:
    normalized = str(last4 or "").strip()
    if not re.fullmatch(r"\d{4}", normalized):
        return ""
    s = (normalized + SECRET_SALT).encode("utf-8")
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
        "editable_until": editable_until.isoformat(),
    }


def get_merchants_columns(db: Session):
    query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'merchants'
        ORDER BY ordinal_position
        """
    )
    rows = db.execute(query).fetchall()
    return [row[0] for row in rows]


def login_user(db: Session, fio: str, last4: str):
    fio_n = fio_norm(fio)
    results = db.execute(
        text(
            """
            SELECT id, fio, fio_norm, pass_hash, telegram_id, tu, created_at
            FROM merchants
            WHERE fio_norm = :fio_norm
              AND COALESCE(is_active, TRUE) = TRUE
            ORDER BY id
            """
        ),
        {"fio_norm": fio_n},
    ).mappings().all()
    incoming = hash_last4(last4)
    if not incoming:
        return None
    result = next(
        (
            row
            for row in results
            if hmac.compare_digest(incoming, str(row["pass_hash"]))
        ),
        None,
    )
    if result is None:
        return None

    return {
        "id": result["id"],
        "fio": result["fio"],
        "fio_norm": result["fio_norm"],
        "telegram_id": result["telegram_id"],
        "tu": result["tu"],
        "created_at": str(result["created_at"]) if result["created_at"] else None,
    }


def get_merchant_by_fio(db: Session, fio: str):
    fio_n = fio_norm(fio)
    if not request_merchant_matches(fio_n):
        return None
    # A roster reset invalidates previously issued sessions even if the same
    # FIO is subsequently imported as a new live identity.
    cleared_at = db.execute(text("SELECT MAX(credentials_cleared_at) FROM merchants")).scalar()
    if cleared_at:
        if isinstance(cleared_at, str):
            cleared_at = datetime.fromisoformat(cleared_at)
        if cleared_at.tzinfo is None:
            cleared_at = cleared_at.replace(tzinfo=timezone.utc)
        if request_session_issued_at() <= cleared_at.timestamp():
            return None
    result = db.execute(
        text(
            """
            SELECT id, fio, fio_norm, telegram_id, tu, created_at
            FROM merchants
            WHERE fio_norm = :fio_norm
              AND COALESCE(is_active, TRUE) = TRUE
            LIMIT 1
            """
        ),
        {"fio_norm": fio_n},
    ).mappings().first()

    if not result:
        return None

    return {
        "id": result["id"],
        "fio": result["fio"],
        "fio_norm": result["fio_norm"],
        "telegram_id": result["telegram_id"],
        "tu": result["tu"],
        "created_at": str(result["created_at"]) if result["created_at"] else None,
    }


def month_start(y: int, m: int) -> date:
    return date(y, m, 1)


def month_end_exclusive(y: int, m: int) -> date:
    return date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)


def days_in_month(y: int, m: int) -> int:
    return (month_end_exclusive(y, m) - timedelta(days=1)).day


def weekday_of(y: int, m: int, d: int) -> int:
    return date(y, m, d).weekday()


def ensure_special_inventory_days_table(db: Session):
    if maintenance_mode_enabled():
        return
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS special_inventory_days (
            id SERIAL PRIMARY KEY,
            inv_date DATE UNIQUE NOT NULL
        )
    """))
    db.commit()


def add_special_inventory_day(db: Session, inv_date: date):
    ensure_special_inventory_days_table(db)
    db.execute(text("""
        INSERT INTO special_inventory_days (inv_date)
        VALUES (:inv_date)
        ON CONFLICT (inv_date) DO NOTHING
    """), {"inv_date": inv_date})
    db.commit()


def delete_special_inventory_day(db: Session, inv_date: date):
    ensure_special_inventory_days_table(db)
    db.execute(text("DELETE FROM special_inventory_days WHERE inv_date = :inv_date"), {"inv_date": inv_date})
    db.commit()


def get_special_inventory_days(db: Session) -> list[date]:
    ensure_special_inventory_days_table(db)
    rows = db.execute(text("SELECT inv_date FROM special_inventory_days ORDER BY inv_date")).all()
    return [r[0] for r in rows if r and r[0]]


def get_special_inventory_days_set(db: Session) -> set[date]:
    return set(get_special_inventory_days(db))


def is_inventory_allowed_date(db: Session, current_date: date) -> bool:
    return current_date.weekday() in (4, 5) or current_date in get_special_inventory_days_set(db)


def month_title(y: int, m: int) -> str:
    names = [
        "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
    ]
    return f"{names[m - 1]} {y}"


def normalize_point_code(v) -> str:
    s = str(v or "").strip()
    s = re.sub(r"\s+", "", s)
    return s if re.fullmatch(r"[A-Za-zА-Яа-я0-9_-]{1,32}", s) else ""


def point_has_any_supply_in_month(db: Session, point_code: str, y: int, m: int) -> bool:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    result = db.execute(
        text(
            """
            SELECT 1
            FROM supplies
            WHERE point_code = :point_code
              AND supply_date >= :start_date
              AND supply_date < :end_date
            LIMIT 1
            """
        ),
        {"point_code": point_code, "start_date": start, "end_date": end},
    ).first()
    return result is not None


def get_supply_boxes_map(db: Session, point_code: str, y: int, m: int) -> dict[int, int]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    rows = db.execute(
        text(
            """
            SELECT supply_date, boxes
            FROM supplies
            WHERE point_code = :point_code
              AND supply_date >= :start_date
              AND supply_date < :end_date
            ORDER BY supply_date
            """
        ),
        {"point_code": point_code, "start_date": start, "end_date": end},
    ).mappings().all()
    result: dict[int, int] = {}
    for row in rows:
        result[row["supply_date"].day] = int(row["boxes"] or 0)
    return result



def get_supply_days_for_point(db: Session, point_code: str, y: int, m: int) -> list[date]:
    """Return only paid supply days for the point.

    Days with fewer than 5 boxes are not treated as a paid supply unless
    the point has pay_lt5 enabled. This keeps the "Не принимал поставку"
    dropdown consistent with the calendar badge and payment calculation.
    """
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    rates = get_point_rates(db, point_code, y, m)
    rows = db.execute(text("""
        SELECT supply_date, boxes
        FROM supplies
        WHERE point_code = :point_code
          AND supply_date >= :start_date
          AND supply_date < :end_date
          AND COALESCE(boxes, 0) > 0
        ORDER BY supply_date
    """), {
        "point_code": point_code,
        "start_date": start,
        "end_date": end,
    }).mappings().all()

    result = []
    for row in rows:
        boxes = int(row["boxes"] or 0)
        if effective_has_supply(boxes, bool(rates.get("pay_lt5"))):
            result.append(row["supply_date"])
    return result


def get_supply_adjustment_amount(db: Session, point_code: str, y: int, m: int) -> int:
    rates = get_point_rates(db, point_code, y, m)
    diff = int(rates["rate_supply"] or 0) - int(rates["rate_no_supply"] or 0)
    return -max(0, diff)


def no_supply_adjustment_marker(supply_date: date) -> str:
    return f"Не принимал поставку {supply_date.strftime('%d.%m')}"


def filter_unadjusted_supply_days(supply_days: list[date], note_comment: str | None) -> list[date]:
    existing = str(note_comment or "")
    return [day for day in supply_days if no_supply_adjustment_marker(day) not in existing]


def get_visits_for_month(db: Session, merchant_id: int, point_code: str, y: int, m: int) -> dict[int, set[str]]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    rows = db.execute(
        text(
            """
            SELECT visit_date, slot
            FROM visits
            WHERE merchant_id = :merchant_id
              AND point_code = :point_code
              AND visit_date >= :start_date
              AND visit_date < :end_date
            """
        ),
        {
            "merchant_id": merchant_id,
            "point_code": point_code,
            "start_date": start,
            "end_date": end,
        },
    ).mappings().all()

    result: dict[int, set[str]] = {}
    for row in rows:
        day = row["visit_date"].day
        result.setdefault(day, set()).add(str(row["slot"]))
    return result


def normalize_visit_slot(slot: str | None, *, allow_legacy_day: bool = True) -> str:
    normalized = str(slot or "").strip().upper()
    allowed = VISIT_SLOTS if allow_legacy_day else OVERLAP_SLOTS
    if normalized not in allowed:
        raise ValueError("Unsupported visit slot")
    return normalized


def allowed_visit_slots(visit_date: date, *, special_inventory: bool = False) -> frozenset[str]:
    """Return user-selectable work slots for a calendar date.

    Friday and Saturday support a morning visit or an evening full inventory.
    Every other weekday uses the original single DAY visit. FULL_INVENT remains
    a separate special-inventory action and is intentionally not returned here.
    """
    if visit_date.weekday() in (4, 5):
        return OVERLAP_SLOTS
    slots = {SLOT_DAY}
    if special_inventory:
        slots.add(SLOT_FULL_INVENT)
    return frozenset(slots)


def inventory_date_in_iso_week(
    db: Session,
    merchant_id: int,
    point_code: str,
    visit_date: date,
) -> date | None:
    week_start = visit_date - timedelta(days=visit_date.isoweekday() - 1)
    week_end = week_start + timedelta(days=7)
    if getattr(db.get_bind().dialect, "name", "") == "postgresql":
        db.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
            {
                "lock_key": (
                    f"full-inventory:{merchant_id}:{point_code}:{week_start.isoformat()}"
                )
            },
        )
    row = db.execute(
        text(
            """
            SELECT visit_date
            FROM visits
            WHERE merchant_id = :merchant_id
              AND point_code = :point_code
              AND visit_date >= :week_start
              AND visit_date < :week_end
              AND slot IN (:slot_evening, :slot_full_invent)
            ORDER BY visit_date
            LIMIT 1
            """
        ),
        {
            "merchant_id": merchant_id,
            "point_code": point_code,
            "week_start": week_start,
            "week_end": week_end,
            "slot_evening": SLOT_EVENING,
            "slot_full_invent": SLOT_FULL_INVENT,
        },
    ).first()
    if not row:
        return None
    existing_date = row[0]
    if isinstance(existing_date, str):
        existing_date = date.fromisoformat(existing_date)
    return existing_date


def toggle_day_visit(
    db: Session,
    merchant_id: int,
    point_code: str,
    y: int,
    m: int,
    day: int,
    slot: str = SLOT_DAY,
    *,
    commit: bool = True,
):
    slot = normalize_visit_slot(slot)
    visit_date = date(y, m, day)
    existing = db.execute(
        text(
            """
            SELECT id
            FROM visits
            WHERE merchant_id = :merchant_id
              AND point_code = :point_code
              AND visit_date = :visit_date
              AND slot = :slot
            LIMIT 1
            """
        ),
        {
            "merchant_id": merchant_id,
            "point_code": point_code,
            "visit_date": visit_date,
            "slot": slot,
        },
    ).scalar()

    if existing:
        db.execute(text("DELETE FROM visits WHERE id = :id"), {"id": existing})
        if commit:
            db.commit()
        return "removed"

    if slot not in allowed_visit_slots(visit_date):
        raise ValueError("Visit slot is not allowed for this date")
    if slot == SLOT_EVENING:
        existing_inventory_date = inventory_date_in_iso_week(
            db, merchant_id, point_code, visit_date
        )
        if existing_inventory_date:
            raise InventoryWeekLimitError(existing_inventory_date)

    db.execute(
        text(
            """
            INSERT INTO visits (merchant_id, point_code, visit_date, slot)
            VALUES (:merchant_id, :point_code, :visit_date, :slot)
            ON CONFLICT DO NOTHING
            """
        ),
        {
            "merchant_id": merchant_id,
            "point_code": point_code,
            "visit_date": visit_date,
            "slot": slot,
        },
    )
    if commit:
        db.commit()
    return "added"


def toggle_inventory_visit(
    db: Session,
    merchant_id: int,
    point_code: str,
    y: int,
    m: int,
    day: int,
    *,
    special_inventory: bool = False,
):
    visit_date = date(y, m, day)
    if SLOT_FULL_INVENT not in allowed_visit_slots(
        visit_date, special_inventory=special_inventory
    ):
        raise ValueError("Full inventory is not allowed for this date")
    existing = db.execute(
        text(
            """
            SELECT id
            FROM visits
            WHERE merchant_id = :merchant_id
              AND point_code = :point_code
              AND visit_date = :visit_date
              AND slot = :slot
            LIMIT 1
            """
        ),
        {
            "merchant_id": merchant_id,
            "point_code": point_code,
            "visit_date": visit_date,
            "slot": SLOT_FULL_INVENT,
        },
    ).scalar()

    if existing:
        db.execute(text("DELETE FROM visits WHERE id = :id"), {"id": existing})
        db.commit()
        return "removed"

    existing_inventory_date = inventory_date_in_iso_week(
        db, merchant_id, point_code, visit_date
    )
    if existing_inventory_date:
        raise InventoryWeekLimitError(existing_inventory_date)

    db.execute(
        text(
            """
            INSERT INTO visits (merchant_id, point_code, visit_date, slot)
            VALUES (:merchant_id, :point_code, :visit_date, :slot)
            ON CONFLICT DO NOTHING
            """
        ),
        {
            "merchant_id": merchant_id,
            "point_code": point_code,
            "visit_date": visit_date,
            "slot": SLOT_FULL_INVENT,
        },
    )
    db.commit()
    return "added"


def get_point_rates(db: Session, point_code: str, y: int, m: int):
    mk = month_start(y, m)
    row = db.execute(
        text(
            """
            SELECT rate_supply, rate_no_supply, rate_inventory, coffee_enabled, coffee_rate, pay_lt5
            FROM point_rates
            WHERE point_code = :point_code
              AND month_key = :month_key
            LIMIT 1
            """
        ),
        {"point_code": point_code, "month_key": mk},
    ).mappings().first()

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


def ensure_monthly_submissions_table(db: Session):
    if maintenance_mode_enabled():
        return
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS monthly_submissions (
                id SERIAL PRIMARY KEY,
                merchant_id INTEGER NOT NULL,
                month_key DATE NOT NULL,
                comment TEXT,
                extra_amount INTEGER NOT NULL DEFAULT 0,
                receipt_path TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE (merchant_id, month_key)
            )
            """
        )
    )
    db.commit()


def get_monthly_submission(db: Session, merchant_id: int, y: int, m: int):
    ensure_monthly_submissions_table(db)
    mk = month_start(y, m)
    row = db.execute(
        text(
            """
            SELECT *
            FROM monthly_submissions
            WHERE merchant_id = :merchant_id
              AND month_key = :month_key
            LIMIT 1
            """
        ),
        {"merchant_id": merchant_id, "month_key": mk},
    ).mappings().first()
    return dict(row) if row else None


def upsert_monthly_submission_draft(db: Session, merchant_id: int, y: int, m: int, comment: str, extra_amount: int, receipt_path: str | None):
    ensure_monthly_submissions_table(db)
    mk = month_start(y, m)
    existing = get_monthly_submission(db, merchant_id, y, m)

    if existing:
        if receipt_path:
            db.execute(text("""
                UPDATE monthly_submissions
                SET comment=:comment, extra_amount=:extra_amount, receipt_path=:receipt_path, updated_at=NOW()
                WHERE merchant_id=:merchant_id AND month_key=:month_key
            """), {
                "comment": comment,
                "extra_amount": extra_amount,
                "receipt_path": receipt_path,
                "merchant_id": merchant_id,
                "month_key": mk,
            })
        else:
            db.execute(text("""
                UPDATE monthly_submissions
                SET comment=:comment, extra_amount=:extra_amount, updated_at=NOW()
                WHERE merchant_id=:merchant_id AND month_key=:month_key
            """), {
                "comment": comment,
                "extra_amount": extra_amount,
                "merchant_id": merchant_id,
                "month_key": mk,
            })
    else:
        db.execute(text("""
            INSERT INTO monthly_submissions (merchant_id, month_key, comment, extra_amount, receipt_path, status)
            VALUES (:merchant_id, :month_key, :comment, :extra_amount, :receipt_path, 'draft')
        """), {
            "merchant_id": merchant_id,
            "month_key": mk,
            "comment": comment,
            "extra_amount": extra_amount,
            "receipt_path": receipt_path,
        })
    db.commit()


def submit_monthly_submission(db: Session, merchant_id: int, y: int, m: int):
    ensure_monthly_submissions_table(db)
    mk = month_start(y, m)
    snapshot_coffee_for_submit(db, merchant_id, mk)
    db.execute(text("""
        INSERT INTO monthly_submissions (merchant_id, month_key, comment, extra_amount, receipt_path, status)
        VALUES (:merchant_id, :month_key, '', 0, NULL, 'submitted')
        ON CONFLICT (merchant_id, month_key)
        DO UPDATE SET status='submitted', updated_at=NOW()
    """), {"merchant_id": merchant_id, "month_key": mk})
    db.commit()


def reopen_monthly_submission(db: Session, merchant_id: int, y: int, m: int):
    ensure_monthly_submissions_table(db)
    mk = month_start(y, m)
    db.execute(text("""
        UPDATE monthly_submissions
        SET status='draft', updated_at=NOW()
        WHERE merchant_id=:merchant_id AND month_key=:month_key
    """), {"merchant_id": merchant_id, "month_key": mk})
    db.commit()


def get_points_for_month(db: Session, merchant_id: int, y: int, m: int) -> list[str]:
    """Return all points that should be included in the monthly total.

    Important: a point must be included even if there are no visits,
    but the merch added a point note or reimbursement.
    """
    ensure_point_adjustments_table(db)
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    if normalized_adjustments_available(db):
        rows = db.execute(text("""
            SELECT DISTINCT point_code
            FROM (
                SELECT point_code
                FROM visits
                WHERE merchant_id=:merchant_id
                  AND visit_date >= :start_date
                  AND visit_date < :end_date

                UNION

                SELECT point_code
                FROM point_notes
                WHERE merchant_id=:merchant_id
                  AND month_key=:month_key

                UNION

                SELECT point_code
                FROM point_reimbursements
                WHERE merchant_id=:merchant_id
                  AND month_key=:month_key
            ) points
            ORDER BY point_code
        """), {
            "merchant_id": merchant_id,
            "start_date": start,
            "end_date": end,
            "month_key": start,
        }).all()
        return [r[0] for r in rows if r and r[0]]

    rows = db.execute(text("""
        SELECT DISTINCT point_code
        FROM (
            SELECT point_code
            FROM visits
            WHERE merchant_id=:merchant_id
              AND visit_date >= :start_date
              AND visit_date < :end_date

            UNION

            SELECT point_code
            FROM point_adjustments
            WHERE merchant_id=:merchant_id
              AND month_key=:month_key
              AND (
                    COALESCE(note_amount, 0) <> 0
                 OR COALESCE(reimb_amount, 0) <> 0
                 OR COALESCE(TRIM(note_comment), '') <> ''
                 OR COALESCE(TRIM(reimb_comment), '') <> ''
                 OR COALESCE(TRIM(reimb_receipt), '') <> ''
              )
        ) points
        ORDER BY point_code
    """), {
        "merchant_id": merchant_id,
        "start_date": start,
        "end_date": end,
        "month_key": start,
    }).all()
    return [r[0] for r in rows if r and r[0]]


def normalized_adjustments_available(db: Session) -> bool:
    """Check the optional migrated storage without creating any table."""
    try:
        db_inspector = inspect(db.connection())
        return all(db_inspector.has_table(name) for name in NORMALIZED_ADJUSTMENT_TABLES)
    except Exception:
        return False


def ensure_point_adjustments_table(db: Session):
    if maintenance_mode_enabled():
        return
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS point_adjustments (
            id SERIAL PRIMARY KEY,
            merchant_id INTEGER NOT NULL,
            point_code TEXT NOT NULL,
            month_key DATE NOT NULL,
            note_amount INTEGER NOT NULL DEFAULT 0,
            note_comment TEXT,
            reimb_amount INTEGER NOT NULL DEFAULT 0,
            reimb_comment TEXT,
            reimb_receipt TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE (merchant_id, point_code, month_key)
        )
    """))
    db.commit()


def get_point_adjustment(db: Session, merchant_id: int, point_code: str, y: int, m: int, *, ensure_schema: bool = True):
    if ensure_schema:
        ensure_point_adjustments_table(db)
    mk = month_start(y, m)
    if normalized_adjustments_available(db):
        notes = db.execute(
            text(
                """
                SELECT amount, comment
                FROM point_notes
                WHERE merchant_id = :merchant_id
                  AND point_code = :point_code
                  AND month_key = :month_key
                ORDER BY created_at, id
                """
            ),
            {
                "merchant_id": merchant_id,
                "point_code": point_code,
                "month_key": mk,
            },
        ).mappings().all()
        reimbursements = db.execute(
            text(
                """
                SELECT id, amount, comment
                FROM point_reimbursements
                WHERE merchant_id = :merchant_id
                  AND point_code = :point_code
                  AND month_key = :month_key
                ORDER BY created_at, id
                """
            ),
            {
                "merchant_id": merchant_id,
                "point_code": point_code,
                "month_key": mk,
            },
        ).mappings().all()
        receipt_rows = db.execute(
            text(
                """
                SELECT rr.legacy_path
                FROM reimbursement_receipts rr
                JOIN point_reimbursements pr ON pr.id = rr.reimbursement_id
                WHERE pr.merchant_id = :merchant_id
                  AND pr.point_code = :point_code
                  AND pr.month_key = :month_key
                ORDER BY rr.created_at, rr.id
                """
            ),
            {
                "merchant_id": merchant_id,
                "point_code": point_code,
                "month_key": mk,
            },
        ).all()
        if not notes and not reimbursements and not receipt_rows:
            return None
        return {
            "merchant_id": merchant_id,
            "point_code": point_code,
            "month_key": mk,
            "note_amount": sum(int(row["amount"]) for row in notes),
            "note_comment": "\n".join(
                f"{int(row['amount'])} ₽ — {row['comment']}" for row in notes
            ),
            "reimb_amount": sum(int(row["amount"]) for row in reimbursements),
            "reimb_comment": "\n".join(
                f"{int(row['amount'])} ₽ — {row['comment']}"
                for row in reimbursements
            ),
            "reimb_receipt": "|".join(
                str(row[0]) for row in receipt_rows if row and row[0]
            )
            or None,
        }

    row = db.execute(text("""
        SELECT *
        FROM point_adjustments
        WHERE merchant_id = :merchant_id
          AND point_code = :point_code
          AND month_key = :month_key
        LIMIT 1
    """), {
        "merchant_id": merchant_id,
        "point_code": point_code,
        "month_key": mk
    }).mappings().first()
    return dict(row) if row else None


def upsert_point_adjustment(
    db: Session,
    merchant_id: int,
    point_code: str,
    y: int,
    m: int,
    note_amount: int,
    note_comment: str,
    reimb_amount: int,
    reimb_comment: str,
    reimb_receipt: str | None,
    *,
    commit: bool = True,
):
    if commit:
        ensure_point_adjustments_table(db)
    mk = month_start(y, m)
    existing = get_point_adjustment(db, merchant_id, point_code, y, m, ensure_schema=commit)

    if existing:
        db.execute(text("""
            UPDATE point_adjustments
            SET note_amount = :note_amount,
                note_comment = :note_comment,
                reimb_amount = :reimb_amount,
                reimb_comment = :reimb_comment,
                reimb_receipt = COALESCE(:reimb_receipt, reimb_receipt),
                updated_at = NOW()
            WHERE merchant_id = :merchant_id
              AND point_code = :point_code
              AND month_key = :month_key
        """), {
            "merchant_id": merchant_id,
            "point_code": point_code,
            "month_key": mk,
            "note_amount": note_amount,
            "note_comment": note_comment,
            "reimb_amount": reimb_amount,
            "reimb_comment": reimb_comment,
            "reimb_receipt": reimb_receipt,
        })
    else:
        db.execute(text("""
            INSERT INTO point_adjustments (
                merchant_id, point_code, month_key,
                note_amount, note_comment, reimb_amount, reimb_comment, reimb_receipt
            ) VALUES (
                :merchant_id, :point_code, :month_key,
                :note_amount, :note_comment, :reimb_amount, :reimb_comment, :reimb_receipt
            )
        """), {
            "merchant_id": merchant_id,
            "point_code": point_code,
            "month_key": mk,
            "note_amount": note_amount,
            "note_comment": note_comment,
            "reimb_amount": reimb_amount,
            "reimb_comment": reimb_comment,
            "reimb_receipt": reimb_receipt,
        })

    if commit:
        db.commit()

def compute_point_total(db: Session, merchant_id: int, point_code: str, y: int, m: int):
    ensure_point_adjustments_table(db)
    boxes_map = get_supply_boxes_map(db, point_code, y, m)
    visits = get_visits_for_month(db, merchant_id, point_code, y, m)
    rates = get_point_rates(db, point_code, y, m)
    point_adj = get_point_adjustment(db, merchant_id, point_code, y, m)

    total = 0
    cnt_supply = 0
    cnt_no_supply = 0
    cnt_day_total = 0
    cnt_full_inv = 0
    sum_supply = 0
    sum_no_supply = 0
    sum_inventory = 0

    for day, slots in visits.items():
        for work_slot in slots.intersection(REGULAR_PAY_SLOTS):
            cnt_day_total += 1
            boxes = boxes_map.get(day, 0)
            if effective_has_supply(boxes, rates["pay_lt5"]):
                cnt_supply += 1
                total += rates["rate_supply"]
                sum_supply += rates["rate_supply"]
            else:
                cnt_no_supply += 1
                total += rates["rate_no_supply"]
                sum_no_supply += rates["rate_no_supply"]

        if slots.intersection(INVENTORY_PAY_SLOTS):
            cnt_full_inv += 1
            total += rates["rate_inventory"]
            sum_inventory += rates["rate_inventory"]

    coffee_sum = 0
    coffee_cnt = 0
    if rates["coffee_enabled"] and cnt_day_total > 0:
        coffee_cnt = coffee_count(db, merchant_id, point_code, month_start(y, m), cnt_day_total)
        coffee_sum = rates["coffee_rate"] * coffee_cnt
        total += coffee_sum

    note_amount = 0
    note_comment = ""
    reimb_amount = 0
    reimb_comment = ""
    reimb_receipt = None

    if point_adj:
        note_amount = int(point_adj.get("note_amount") or 0)
        note_comment = point_adj.get("note_comment") or ""
        reimb_amount = int(point_adj.get("reimb_amount") or 0)
        reimb_comment = point_adj.get("reimb_comment") or ""
        reimb_receipt = point_adj.get("reimb_receipt")
        total += note_amount + reimb_amount

    return {
        "total": total,
        "cnt_supply": cnt_supply,
        "cnt_no_supply": cnt_no_supply,
        "cnt_day_total": cnt_day_total,
        "cnt_full_inv": cnt_full_inv,
        "sum_supply": sum_supply,
        "sum_no_supply": sum_no_supply,
        "sum_inventory": sum_inventory,
        "coffee_enabled": rates["coffee_enabled"],
        "coffee_rate": rates["coffee_rate"],
        "coffee_sum": coffee_sum,
        "coffee_cnt": coffee_cnt,
        "coffee_auto_days": sum(bool(slots.intersection(REGULAR_PAY_SLOTS)) for slots in visits.values()),
        "pay_lt5": rates["pay_lt5"],
        "rate_supply": rates["rate_supply"],
        "rate_no_supply": rates["rate_no_supply"],
        "rate_inventory": rates["rate_inventory"],
        "note_amount": note_amount,
        "note_comment": note_comment,
        "reimb_amount": reimb_amount,
        "reimb_comment": reimb_comment,
        "reimb_receipt": reimb_receipt,
    }


def compute_overall_total(db: Session, merchant_id: int, y: int, m: int):
    ensure_monthly_submissions_table(db)
    points = get_points_for_month(db, merchant_id, y, m)
    total = 0
    per_point = {}
    per_point_details = {}

    for point_code in points:
        detail = compute_point_total(db, merchant_id, point_code, y, m)
        per_point[point_code] = detail["total"]
        per_point_details[point_code] = detail
        total += detail["total"]

    monthly = get_monthly_submission(db, merchant_id, y, m)
    extra_amount = 0
    comment = ""
    receipt_path = None
    submission_status = "draft"
    if monthly:
        extra_amount = int(monthly.get("extra_amount") or 0)
        comment = monthly.get("comment") or ""
        receipt_path = monthly.get("receipt_path")
        submission_status = monthly.get("status") or "draft"
        total += extra_amount

    return {
        "total": total,
        "per_point": per_point,
        "per_point_details": per_point_details,
        "extra_amount": extra_amount,
        "comment": comment,
        "receipt_path": receipt_path,
        "submission_status": submission_status,
    }


def get_all_tu_values(db: Session) -> list[str]:
    rows = db.execute(text("""
        SELECT DISTINCT tu
        FROM (SELECT tu FROM merchants UNION SELECT historical_tu AS tu FROM merchants) identities
        WHERE tu IS NOT NULL AND TRIM(tu) <> ''
        ORDER BY tu
    """)).all()
    return [r[0] for r in rows if r and r[0]]


def get_admin_report_rows(db: Session, y: int, m: int, tu: str | None = None, status: str | None = None):
    """Fast admin report builder.

    Важно: отчёт должен включать точки, где нет выходов, но есть примечание/возмещение.
    Поэтому базовый список точек берём из visits UNION point_adjustments.
    Пересечение: выходы «В» разных сотрудников на одну точку и дату,
    либо существующее пересечение одинаковых MORNING/EVENING слотов.
    """
    # Read-only report: compatibility DDL belongs to explicit schema setup.
    start_date = month_start(y, m)
    end_date = month_end_exclusive(y, m)

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "month_key": start_date,
        "slot_day": SLOT_DAY,
        "slot_morning": SLOT_MORNING,
        "slot_evening": SLOT_EVENING,
        "slot_full_invent": SLOT_FULL_INVENT,
        "default_rate_supply": DEFAULT_RATE_SUPPLY,
        "default_rate_no_supply": DEFAULT_RATE_NO_SUPPLY,
        "default_rate_inventory": DEFAULT_RATE_INVENTORY,
        "default_rate_coffee": DEFAULT_RATE_COFFEE,
    }

    tu_sql = ""
    if tu:
        tu_sql = " AND m.tu = :tu"
        params["tu"] = tu

    if normalized_adjustments_available(db):
        adjustment_ctes = """
        note_agg AS (
            SELECT merchant_id, point_code, month_key,
                   SUM(amount) AS note_amount,
                   STRING_AGG(
                       CAST(amount AS TEXT) || ' ₽ — ' || comment,
                       CHR(10) ORDER BY created_at, id
                   ) AS note_comment
            FROM point_notes
            GROUP BY merchant_id, point_code, month_key
        ),
        reimb_agg AS (
            SELECT merchant_id, point_code, month_key,
                   SUM(amount) AS reimb_amount,
                   STRING_AGG(
                       CAST(amount AS TEXT) || ' ₽ — ' || comment,
                       CHR(10) ORDER BY created_at, id
                   ) AS reimb_comment
            FROM point_reimbursements
            GROUP BY merchant_id, point_code, month_key
        ),
        receipt_agg AS (
            SELECT pr.merchant_id, pr.point_code, pr.month_key,
                   STRING_AGG(rr.legacy_path, '|' ORDER BY rr.created_at, rr.id) AS reimb_receipt
            FROM point_reimbursements pr
            JOIN reimbursement_receipts rr ON rr.reimbursement_id = pr.id
            GROUP BY pr.merchant_id, pr.point_code, pr.month_key
        ),
        adjustment_keys AS (
            SELECT merchant_id, point_code, month_key FROM point_notes
            UNION
            SELECT merchant_id, point_code, month_key FROM point_reimbursements
        ),
        adjustment_source AS (
            SELECT k.merchant_id, k.point_code, k.month_key,
                   COALESCE(n.note_amount, 0) AS note_amount,
                   COALESCE(n.note_comment, '') AS note_comment,
                   COALESCE(r.reimb_amount, 0) AS reimb_amount,
                   COALESCE(r.reimb_comment, '') AS reimb_comment,
                   x.reimb_receipt
            FROM adjustment_keys k
            LEFT JOIN note_agg n
              ON n.merchant_id = k.merchant_id
             AND n.point_code = k.point_code
             AND n.month_key = k.month_key
            LEFT JOIN reimb_agg r
              ON r.merchant_id = k.merchant_id
             AND r.point_code = k.point_code
             AND r.month_key = k.month_key
            LEFT JOIN receipt_agg x
              ON x.merchant_id = k.merchant_id
             AND x.point_code = k.point_code
             AND x.month_key = k.month_key
        )
        """
    else:
        adjustment_ctes = """
        adjustment_source AS (
            SELECT merchant_id, point_code, month_key,
                   note_amount, note_comment,
                   reimb_amount, reimb_comment, reimb_receipt
            FROM point_adjustments
        )
        """

    sql = f"""
        WITH {adjustment_ctes},
        base_points AS (
            SELECT DISTINCT merchant_id, point_code
            FROM visits
            WHERE visit_date >= :start_date
              AND visit_date < :end_date

            UNION

            SELECT DISTINCT merchant_id, point_code
            FROM adjustment_source
            WHERE month_key = :month_key
              AND (
                    COALESCE(note_amount, 0) <> 0
                 OR COALESCE(reimb_amount, 0) <> 0
                 OR COALESCE(TRIM(note_comment), '') <> ''
                 OR COALESCE(TRIM(reimb_comment), '') <> ''
                 OR COALESCE(TRIM(reimb_receipt), '') <> ''
              )
        ),
        visit_by_day AS (
            SELECT
                merchant_id,
                point_code,
                visit_date,
                SUM(CASE WHEN slot IN (:slot_day, :slot_morning) THEN 1 ELSE 0 END) AS cnt_regular,
                MAX(CASE WHEN slot IN (:slot_evening, :slot_full_invent) THEN 1 ELSE 0 END) AS cnt_full_inv
            FROM visits
            WHERE visit_date >= :start_date
              AND visit_date < :end_date
            GROUP BY merchant_id, point_code, visit_date
        ),
        visit_agg AS (
            SELECT
                v.merchant_id,
                v.point_code,
                SUM(CASE
                    WHEN COALESCE(s.boxes, 0) > 0
                     AND (COALESCE(pr.pay_lt5, FALSE) = TRUE OR COALESCE(s.boxes, 0) >= 5)
                    THEN v.cnt_regular ELSE 0 END) AS cnt_supply,

                SUM(CASE
                    WHEN NOT (
                        COALESCE(s.boxes, 0) > 0
                        AND (COALESCE(pr.pay_lt5, FALSE) = TRUE OR COALESCE(s.boxes, 0) >= 5)
                     )
                    THEN v.cnt_regular ELSE 0 END) AS cnt_no_supply,

                SUM(v.cnt_full_inv) AS cnt_full_inv,
                SUM(v.cnt_regular) AS cnt_day_total
            FROM visit_by_day v
            LEFT JOIN supplies s
              ON s.point_code = v.point_code
             AND s.supply_date = v.visit_date
            LEFT JOIN point_rates pr
              ON pr.point_code = v.point_code
             AND pr.month_key = :month_key
            GROUP BY v.merchant_id, v.point_code
        ),
        overlap_rows AS (
            SELECT DISTINCT v1.merchant_id, v1.point_code
            FROM visits v1
            JOIN visits v2
              ON v1.visit_date = v2.visit_date
             AND v1.point_code = v2.point_code
             AND v1.slot = v2.slot
             AND v1.merchant_id <> v2.merchant_id
            WHERE v1.visit_date >= :start_date
              AND v1.visit_date < :end_date
              AND v1.slot IN (:slot_morning, :slot_evening)
        )
        SELECT
            bp.merchant_id,
            bp.point_code,
            m.fio,
            COALESCE(m.tu, '') AS tu,
            COALESCE(ms.status, 'не отправлено') AS status,
            COALESCE(ms.comment, '') AS monthly_comment,
            COALESCE(ms.extra_amount, 0) AS monthly_extra_amount,
            ms.receipt_path AS monthly_receipt_path,

            COALESCE(pr.rate_supply, :default_rate_supply) AS rate_supply,
            COALESCE(pr.rate_no_supply, :default_rate_no_supply) AS rate_no_supply,
            COALESCE(pr.rate_inventory, :default_rate_inventory) AS rate_inventory,
            COALESCE(pr.coffee_enabled, FALSE) AS coffee_enabled,
            COALESCE(pr.coffee_rate, :default_rate_coffee) AS coffee_rate,
            COALESCE(pr.pay_lt5, FALSE) AS pay_lt5,

            COALESCE(pa.note_amount, 0) AS note_amount,
            COALESCE(pa.note_comment, '') AS note_comment,
            COALESCE(pa.reimb_amount, 0) AS reimb_amount,
            COALESCE(pa.reimb_comment, '') AS reimb_comment,
            pa.reimb_receipt AS reimb_receipt,
            CASE WHEN ov.merchant_id IS NULL THEN FALSE ELSE TRUE END AS has_overlap,

            COALESCE(va.cnt_supply, 0) AS cnt_supply,
            COALESCE(va.cnt_no_supply, 0) AS cnt_no_supply,
            COALESCE(va.cnt_full_inv, 0) AS cnt_full_inv,
            COALESCE(va.cnt_day_total, 0) AS cnt_day_total,
            cb.days_count AS coffee_days_count
        FROM base_points bp
        JOIN (SELECT id, COALESCE(historical_fio, fio) AS fio,
                     COALESCE(historical_tu, tu) AS tu FROM merchants) m
          ON m.id = bp.merchant_id
        LEFT JOIN coffee_bonus cb
          ON cb.merchant_id = bp.merchant_id
         AND cb.point_code = bp.point_code
         AND cb.month_key = :month_key
        LEFT JOIN visit_agg va
          ON va.merchant_id = bp.merchant_id
         AND va.point_code = bp.point_code
        LEFT JOIN point_rates pr
          ON pr.point_code = bp.point_code
         AND pr.month_key = :month_key
        LEFT JOIN adjustment_source pa
          ON pa.merchant_id = bp.merchant_id
         AND pa.point_code = bp.point_code
         AND pa.month_key = :month_key
        LEFT JOIN monthly_submissions ms
          ON ms.merchant_id = bp.merchant_id
         AND ms.month_key = :month_key
        LEFT JOIN overlap_rows ov
          ON ov.merchant_id = bp.merchant_id
         AND ov.point_code = bp.point_code
        WHERE 1=1
          {tu_sql}
        ORDER BY m.tu NULLS LAST, m.fio, bp.point_code
    """

    rows = db.execute(text(sql), params).mappings().all()
    valid_overlap_keys: set[tuple[int, str]] = set()
    for overlap in (
        _valid_intersection_candidates(db, y, m, tu)
        + _calendar_intersection_candidates(db, y, m, tu)
    ):
        point_code = str(overlap["point_code"])
        valid_overlap_keys.add((int(overlap["merchant_id1"]), point_code))
        valid_overlap_keys.add((int(overlap["merchant_id2"]), point_code))
    result = []
    for row in rows:
        status_value = row["status"] or "не отправлено"
        if status and status_value != status:
            continue

        cnt_supply = int(row["cnt_supply"] or 0)
        cnt_no_supply = int(row["cnt_no_supply"] or 0)
        cnt_full_inv = int(row["cnt_full_inv"] or 0)
        cnt_day_total = int(row["cnt_day_total"] or 0)
        rate_supply = int(row["rate_supply"] or DEFAULT_RATE_SUPPLY)
        rate_no_supply = int(row["rate_no_supply"] or DEFAULT_RATE_NO_SUPPLY)
        rate_inventory = int(row["rate_inventory"] or DEFAULT_RATE_INVENTORY)
        coffee_rate = int(row["coffee_rate"] or DEFAULT_RATE_COFFEE)
        coffee_enabled = bool(row["coffee_enabled"])
        note_amount = int(row["note_amount"] or 0)
        reimb_amount = int(row["reimb_amount"] or 0)

        sum_supply = cnt_supply * rate_supply
        sum_no_supply = cnt_no_supply * rate_no_supply
        sum_inventory = cnt_full_inv * rate_inventory
        stored_coffee = row.get("coffee_days_count")
        coffee_cnt = (cnt_day_total if stored_coffee is None else min(cnt_day_total, max(0, int(stored_coffee)))) if coffee_enabled else 0
        period = get_active_period()
        if coffee_enabled and (y, m) == (period["year"], period["month"]) and status_value != "submitted":
            coffee_cnt = coffee_count(db, row["merchant_id"], row["point_code"], start_date, cnt_day_total)
        coffee_sum = coffee_cnt * coffee_rate if coffee_enabled else 0
        point_total = sum_supply + sum_no_supply + sum_inventory + coffee_sum + note_amount + reimb_amount

        result.append({
            "merchant_id": row["merchant_id"],
            "fio": row["fio"],
            "tu": row["tu"] or "",
            "point_code": row["point_code"],
            "month_key": str(start_date),
            "status": status_value,
            "comment": row["monthly_comment"] or "",
            "extra_amount": int(row["monthly_extra_amount"] or 0),
            "receipt_path": row["monthly_receipt_path"],
            "note_amount": note_amount,
            "note_comment": row["note_comment"] or "",
            "reimb_amount": reimb_amount,
            "reimb_comment": row["reimb_comment"] or "",
            "reimb_receipt": row["reimb_receipt"],
            "cnt_supply": cnt_supply,
            "cnt_no_supply": cnt_no_supply,
            "cnt_total_exits": cnt_supply + cnt_no_supply,
            "cnt_full_inv": cnt_full_inv,
            "sum_supply": sum_supply,
            "sum_no_supply": sum_no_supply,
            "sum_inventory": sum_inventory,
            "coffee_enabled": coffee_enabled,
            "coffee_cnt": coffee_cnt,
            "coffee_rate": coffee_rate,
            "coffee_sum": coffee_sum,
            "has_overlap": (
                int(row["merchant_id"]), str(row["point_code"])
            ) in valid_overlap_keys,
            "point_total": point_total,
        })
    return result

def get_admin_payroll_rows(db: Session, y: int, m: int, tu: str | None = None, status: str | None = None):
    rows = get_admin_report_rows(db, y, m, tu, status)
    grouped: dict[int, dict] = {}
    for r in rows:
        mid = r["merchant_id"]
        if mid not in grouped:
            grouped[mid] = {
                "merchant_id": mid,
                "fio": r["fio"],
                "tu": r["tu"] or "",
                "clean_total": 0,
                "status": r["status"],
            }
        grouped[mid]["clean_total"] += int(r["point_total"] or 0)
        if grouped[mid]["status"] != "submitted" and r["status"] == "submitted":
            grouped[mid]["status"] = "submitted"

    result = []
    for row in grouped.values():
        clean_total = int(row["clean_total"] or 0)
        row["payroll_total"] = math.ceil(clean_total / 0.87) if clean_total > 0 else 0
        result.append(row)
    result.sort(key=lambda x: (x.get("tu") or "", x.get("fio") or ""))
    return result


def _valid_intersection_candidates(
    db: Session, y: int, m: int, tu: str | None = None
) -> list[dict]:
    start = month_start(y, m)
    end = month_end_exclusive(y, m)
    sql = """
        SELECT DISTINCT v1.merchant_id AS merchant_id1,
               v2.merchant_id AS merchant_id2,
               v1.visit_date, v1.point_code,
               m1.fio AS fio1, m1.tu AS tu1,
               m2.fio AS fio2, m2.tu AS tu2,
               v1.slot AS slot1, v2.slot AS slot2
        FROM visits v1
        JOIN visits v2
          ON v1.visit_date = v2.visit_date
         AND v1.point_code = v2.point_code
         AND v1.slot = v2.slot
         AND v1.merchant_id < v2.merchant_id
        JOIN (SELECT id, COALESCE(historical_fio, fio) AS fio,
                     COALESCE(historical_tu, tu) AS tu FROM merchants) m1 ON m1.id = v1.merchant_id
        JOIN (SELECT id, COALESCE(historical_fio, fio) AS fio,
                     COALESCE(historical_tu, tu) AS tu FROM merchants) m2 ON m2.id = v2.merchant_id
        WHERE v1.visit_date >= :start_date
          AND v1.visit_date < :end_date
          AND v1.slot IN ('MORNING', 'EVENING')
    """
    params = {"start_date": start, "end_date": end}
    if tu:
        sql += " AND (m1.tu = :tu OR m2.tu = :tu)"
        params["tu"] = tu
    sql += " ORDER BY v1.visit_date, v1.point_code, v1.slot, m1.fio, m2.fio"
    rows = db.execute(text(sql), params).mappings().all()
    if not rows:
        return []
    # Fetch notes once per export instead of inspecting schema and querying each pair.
    normalized = normalized_adjustments_available(db)
    note_table = "point_notes" if normalized else "point_adjustments"
    note_column = "comment" if normalized else "note_comment"
    merchant_ids = sorted({int(row[key]) for row in rows for key in ("merchant_id1", "merchant_id2")})
    note_rows = db.execute(text(f"""
        SELECT merchant_id, point_code, {note_column} AS comment
        FROM {note_table}
        WHERE month_key = :month_key AND merchant_id IN :merchant_ids
    """).bindparams(bindparam("merchant_ids", expanding=True)),
        {"month_key": start, "merchant_ids": merchant_ids}).mappings().all()
    adjustment_cache: dict[tuple[int, str], str] = {}
    for note in note_rows:
        key = (int(note["merchant_id"]), str(note["point_code"]))
        if normalized:
            adjustment_cache[key] = adjustment_cache.get(key, "") + "\n" + str(note["comment"] or "")
        else:
            adjustment_cache.setdefault(key, str(note["comment"] or ""))
    result = []
    for raw_row in rows:
        row = dict(raw_row)
        visit_date = row["visit_date"]
        if isinstance(visit_date, str):
            visit_date = date.fromisoformat(visit_date)
        marker = no_supply_adjustment_marker(visit_date)
        suppressed = False
        for merchant_id in (row["merchant_id1"], row["merchant_id2"]):
            key = (int(merchant_id), str(row["point_code"]))
            if marker in adjustment_cache.get(key, ""):
                suppressed = True
                break
        if not suppressed:
            result.append(row)
    return result


def _calendar_intersection_candidates(
    db: Session, y: int, m: int, tu: str | None = None
) -> list[dict]:
    """Fallback for distinct people present without a common explicit shift.

    Grouping collapses raw repeats. A shared MORNING/EVENING belongs only to
    the existing slot calculation, including its no-supply exclusions.
    """
    sql = """
        WITH presence AS (
            SELECT merchant_id, visit_date, point_code,
                   MAX(CASE WHEN slot = 'MORNING' THEN 1 ELSE 0 END) AS morning,
                   MAX(CASE WHEN slot = 'EVENING' THEN 1 ELSE 0 END) AS evening
            FROM visits
            WHERE visit_date >= :start_date AND visit_date < :end_date
              AND slot IN :presence_slots
            GROUP BY merchant_id, visit_date, point_code
        )
        SELECT v1.merchant_id AS merchant_id1,
               v2.merchant_id AS merchant_id2,
               v1.visit_date, v1.point_code,
               m1.fio AS fio1, m1.tu AS tu1,
               m2.fio AS fio2, m2.tu AS tu2
        FROM presence v1
        JOIN presence v2
          ON v1.visit_date = v2.visit_date
         AND v1.point_code = v2.point_code
         AND v1.merchant_id < v2.merchant_id
        JOIN (SELECT id, COALESCE(historical_fio, fio) AS fio,
                     COALESCE(historical_tu, tu) AS tu FROM merchants) m1 ON m1.id = v1.merchant_id
        JOIN (SELECT id, COALESCE(historical_fio, fio) AS fio,
                     COALESCE(historical_tu, tu) AS tu FROM merchants) m2 ON m2.id = v2.merchant_id
        WHERE NOT ((v1.morning = 1 AND v2.morning = 1)
                OR (v1.evening = 1 AND v2.evening = 1))
    """
    params = {"start_date": month_start(y, m), "end_date": month_end_exclusive(y, m),
              "presence_slots": sorted(PRESENCE_SLOTS)}
    if tu:
        sql += " AND (m1.tu = :tu OR m2.tu = :tu)"
        params["tu"] = tu
    sql += " ORDER BY v1.visit_date, v1.point_code, v1.merchant_id, v2.merchant_id"
    rows = db.execute(text(sql).bindparams(bindparam("presence_slots", expanding=True)), params).mappings().all()
    return [dict(row, slot1=INTERSECTION_CALENDAR_DAY, slot2=INTERSECTION_CALENDAR_DAY)
            for row in rows]


def get_intersections_rows(
    db: Session, y: int, m: int, tu: str | None = None, *, include_calendar_days: bool = False
):
    """Keep the legacy slot-only contract; the registry/export opts into day rows."""
    rows = _valid_intersection_candidates(db, y, m, tu)
    if include_calendar_days:
        rows += _calendar_intersection_candidates(db, y, m, tu)
    return [{
        "visit_date": str(r["visit_date"]),
        "point_code": r["point_code"],
        "fio1": r["fio1"],
        "tu1": r["tu1"] or "",
        "slot1": r["slot1"],
        "fio2": r["fio2"],
        "tu2": r["tu2"] or "",
        "slot2": r["slot2"],
        **({"merchant_id1": r["merchant_id1"], "merchant_id2": r["merchant_id2"],
            "intersection_level": "calendar_day" if r["slot1"] == INTERSECTION_CALENDAR_DAY else "slot"}
           if include_calendar_days else {}),
    } for r in rows]


def find_visit_intersections(visits: list[dict]) -> list[dict]:
    """Legacy slot-only reference; calendar exits are a separate report level."""
    grouped: dict[tuple, dict[int, str]] = {}
    for visit in visits:
        slot = str(visit.get("slot") or "").upper()
        if slot not in OVERLAP_SLOTS:
            continue
        key = (visit.get("point_code"), visit.get("visit_date"), slot)
        grouped.setdefault(key, {})[int(visit["merchant_id"])] = str(visit.get("fio") or "")
    result = []
    for (point_code, visit_date, slot), merchants in sorted(grouped.items(), key=lambda item: str(item[0])):
        ids = sorted(merchants)
        for left_index, merchant_a in enumerate(ids):
            for merchant_b in ids[left_index + 1:]:
                result.append({
                    "point_code": point_code,
                    "visit_date": visit_date,
                    "slot": slot,
                    "merchant_a": merchant_a,
                    "fio_a": merchants[merchant_a],
                    "merchant_b": merchant_b,
                    "fio_b": merchants[merchant_b],
                })
    return result


# ===== импорт файлов =====

def upsert_supply_row(db: Session, point_code: str, supply_date: date, boxes: int):
    db.execute(text("DELETE FROM supplies WHERE point_code=:point_code AND supply_date=:supply_date"), {
        "point_code": point_code, "supply_date": supply_date
    })

    columns = [row[0] for row in db.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'supplies'
        ORDER BY ordinal_position
    """)).fetchall()]

    if 'has_supply' in columns:
        db.execute(text("""
            INSERT INTO supplies (point_code, supply_date, boxes, has_supply)
            VALUES (:point_code, :supply_date, :boxes, :has_supply)
        """), {
            "point_code": point_code,
            "supply_date": supply_date,
            "boxes": boxes,
            "has_supply": True
        })
    else:
        db.execute(text("""
            INSERT INTO supplies (point_code, supply_date, boxes)
            VALUES (:point_code, :supply_date, :boxes)
        """), {
            "point_code": point_code,
            "supply_date": supply_date,
            "boxes": boxes
        })


def import_supplies_xlsx(db: Session, file_obj) -> dict:
    """
    Быстрая загрузка поставок.

    Что изменено:
    - Excel читается один раз;
    - старые поставки по загружаемым точкам/датам удаляются одним SQL-запросом;
    - новые поставки вставляются пачкой через executemany;
    - commit выполняется один раз в конце.

    Формат файла прежний:
    - 1-я колонка: код точки;
    - 1-я строка: даты или номера дней месяца;
    - значения в ячейках: количество коробок;
    - пустые ячейки пропускаются.
    """
    wb = load_workbook(file_obj, data_only=True, read_only=True)
    ws = wb[wb.sheetnames[0]]

    rows_iter = ws.iter_rows(values_only=True)
    try:
        headers = next(rows_iter)
    except StopIteration:
        return {"loaded_rows": 0, "loaded_points": 0}

    active = get_active_period()
    report_year = active["year"]
    report_month = active["month"]

    date_columns: list[tuple[int, date]] = []
    for idx, value in enumerate(headers[1:], start=1):
        if value is None:
            continue

        if isinstance(value, datetime):
            date_columns.append((idx, value.date()))
            continue

        if isinstance(value, date):
            date_columns.append((idx, value))
            continue

        raw = str(value).strip()
        match = re.match(r"^(\d{1,2})", raw)
        if not match:
            continue

        day_num = int(match.group(1))
        try:
            supply_date = date(report_year, report_month, day_num)
        except ValueError:
            continue

        date_columns.append((idx, supply_date))

    if not date_columns:
        return {"loaded_rows": 0, "loaded_points": 0}

    supply_rows = []
    loaded_points = set()
    loaded_dates = set()

    for row_idx, row in enumerate(rows_iter, start=2):
        if not row:
            continue

        point_code = normalize_point_code(row[0] if len(row) > 0 else None)
        if not point_code:
            if any(value not in (None, "") for value in row):
                raise ValueError(f"Ошибка в строке поставок {row_idx}: некорректный номер точки")
            continue

        row_has_any_supply = False

        for col_idx, supply_date in date_columns:
            raw_boxes = row[col_idx] if col_idx < len(row) else None
            if raw_boxes in (None, ""):
                continue

            try:
                boxes = int(float(raw_boxes))
            except Exception as exc:
                raise ValueError(f"Ошибка в строке поставок {row_idx}: некорректное число коробок") from exc
            if boxes < 0:
                raise ValueError(f"Ошибка в строке поставок {row_idx}: число коробок не может быть отрицательным")

            supply_rows.append({
                "point_code": point_code,
                "supply_date": supply_date,
                "boxes": boxes,
                "has_supply": True,
            })
            loaded_dates.add(supply_date)
            row_has_any_supply = True

        if row_has_any_supply:
            loaded_points.add(point_code)

    if not supply_rows:
        db.commit()
        return {"loaded_rows": 0, "loaded_points": 0}

    columns = [row[0] for row in db.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'supplies'
        ORDER BY ordinal_position
    """)).fetchall()]
    has_supply_column = "has_supply" in columns

    delete_stmt = text("""
        DELETE FROM supplies
        WHERE point_code IN :point_codes
          AND supply_date IN :supply_dates
    """).bindparams(
        bindparam("point_codes", expanding=True),
        bindparam("supply_dates", expanding=True),
    )

    db.execute(delete_stmt, {
        "point_codes": list(loaded_points),
        "supply_dates": list(loaded_dates),
    })

    if has_supply_column:
        insert_stmt = text("""
            INSERT INTO supplies (point_code, supply_date, boxes, has_supply)
            VALUES (:point_code, :supply_date, :boxes, :has_supply)
        """)
        db.execute(insert_stmt, supply_rows)
    else:
        insert_stmt = text("""
            INSERT INTO supplies (point_code, supply_date, boxes)
            VALUES (:point_code, :supply_date, :boxes)
        """)
        db.execute(insert_stmt, [
            {
                "point_code": row["point_code"],
                "supply_date": row["supply_date"],
                "boxes": row["boxes"],
            }
            for row in supply_rows
        ])

    db.commit()
    return {"loaded_rows": len(supply_rows), "loaded_points": len(loaded_points)}


def upsert_rate_row(db: Session, point_code: str, month_key: date, rate_supply: int, rate_no_supply: int, rate_inventory: int, coffee_enabled: bool, coffee_rate: int, pay_lt5: bool):
    db.execute(text("DELETE FROM point_rates WHERE point_code=:point_code AND month_key=:month_key"), {
        "point_code": point_code, "month_key": month_key
    })
    db.execute(text("""
        INSERT INTO point_rates (
            point_code, month_key, rate_supply, rate_no_supply, rate_inventory,
            coffee_enabled, coffee_rate, pay_lt5
        ) VALUES (
            :point_code, :month_key, :rate_supply, :rate_no_supply, :rate_inventory,
            :coffee_enabled, :coffee_rate, :pay_lt5
        )
    """), {
        "point_code": point_code,
        "month_key": month_key,
        "rate_supply": rate_supply,
        "rate_no_supply": rate_no_supply,
        "rate_inventory": rate_inventory,
        "coffee_enabled": coffee_enabled,
        "coffee_rate": coffee_rate,
        "pay_lt5": pay_lt5,
    })


def import_rates_xlsx(db: Session, file_obj, year: int, month: int) -> dict:
    wb = load_workbook(file_obj, data_only=True, read_only=True)
    ws = wb[wb.sheetnames[0]]
    month_key = month_start(year, month)
    parsed = []
    for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        if not any(value not in (None, "") for value in row):
            continue
        try:
            point_code = normalize_point_code(row[0])
            if not point_code:
                raise ValueError("пустой номер точки")
            rates = [int(row[index] or 0) for index in (1, 2, 3, 6)]
            if any(value < 0 for value in rates):
                raise ValueError("ставка не может быть отрицательной")
            coffee_enabled = str(row[4] or "").strip().lower() in {"да", "true", "1"}
            pay_lt5 = str(row[5] or "").strip().lower() in {"да", "true", "1"}
            parsed.append((point_code, rates[0], rates[1], rates[2], coffee_enabled, rates[3], pay_lt5))
        except (ValueError, TypeError, IndexError) as exc:
            raise ValueError(f"Ошибка в строке ставок {row_idx}: {exc}") from exc
    if not parsed:
        raise ValueError("В файле ставок нет данных")
    try:
        for values in parsed:
            upsert_rate_row(db, values[0], month_key, *values[1:])
        db.commit()
    except Exception:
        db.rollback()
        raise
    return {"loaded_rows": len(parsed)}


def upsert_merchant_row(
    db: Session,
    fio: str,
    last4: str,
    tu: str,
    *,
    actor: str = "admin",
    confirm_same_name: bool = False,
):
    """Backward-compatible entry point; creation never silently updates a row."""
    return create_merchant(
        db,
        fio,
        last4,
        tu,
        actor=actor,
        fio_normalizer=fio_norm,
        last4_hasher=hash_last4,
        confirm_same_name=confirm_same_name,
    )


def import_merchants_xlsx(
    db: Session,
    file_obj,
    tu: str,
    *,
    actor: str = "admin-import",
) -> dict:
    wb = load_workbook(file_obj, data_only=True, read_only=True)
    ws = wb[wb.sheetnames[0]]
    tu = str(tu or "").strip()
    if not tu:
        raise ValueError("ТУ обязателен")
    parsed: list[dict[str, str]] = []
    seen_identities: dict[tuple[str, str], int] = {}
    for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        if not any(value not in (None, "") for value in row):
            continue
        try:
            values = validate_merchant_values(
                row[0] if row else "",
                row[1] if len(row) > 1 else "",
                tu,
                fio_normalizer=fio_norm,
            )
            identity = (values["fio_norm"], values["last4"])
            if identity in seen_identities:
                raise ValueError(
                    f"точный дубль строк {seen_identities[identity]} и {row_idx}"
                )
            seen_identities[identity] = row_idx
            parsed.append(values)
        except ValueError as exc:
            raise ValueError(
                f"Ошибка в строке мерчендайзеров {row_idx}: {exc}"
            ) from exc
    if not parsed:
        raise ValueError("В файле мерчендайзеров нет данных")
    try:
        created = 0
        reactivated = 0
        for values in parsed:
            imported = import_or_reactivate_merchant(
                db,
                values["fio"],
                values["last4"],
                values["tu"],
                actor=actor,
                fio_normalizer=fio_norm,
                last4_hasher=hash_last4,
            )
            created += int(imported["created"])
            reactivated += int(imported["reactivated"])
        db.commit()
    except Exception:
        db.rollback()
        raise
    return {
        "loaded_rows": len(parsed),
        "created": created,
        "reactivated": reactivated,
    }


def clear_month_data(db: Session, year: int, month: int) -> dict:
    ensure_monthly_submissions_table(db)
    ensure_point_adjustments_table(db)
    start = month_start(year, month)
    end = month_end_exclusive(year, month)
    deleted_visits = db.execute(text("DELETE FROM visits WHERE visit_date >= :start_date AND visit_date < :end_date"), {
        "start_date": start, "end_date": end
    }).rowcount or 0
    deleted_supplies = db.execute(text("DELETE FROM supplies WHERE supply_date >= :start_date AND supply_date < :end_date"), {
        "start_date": start, "end_date": end
    }).rowcount or 0
    deleted_rates = db.execute(text("DELETE FROM point_rates WHERE month_key = :month_key"), {"month_key": start}).rowcount or 0
    deleted_monthly = db.execute(text("DELETE FROM monthly_submissions WHERE month_key = :month_key"), {"month_key": start}).rowcount or 0
    deleted_point_adjustments = db.execute(text("DELETE FROM point_adjustments WHERE month_key = :month_key"), {"month_key": start}).rowcount or 0
    db.commit()
    return {
        "deleted_visits": deleted_visits,
        "deleted_supplies": deleted_supplies,
        "deleted_rates": deleted_rates,
        "deleted_monthly": deleted_monthly,
        "deleted_point_adjustments": deleted_point_adjustments,
    }


def clear_merchants_by_tu(
    db: Session,
    tu: str,
    *,
    actor: str = "admin",
) -> int:
    """Compatibility operation: deactivate merchants without deleting history."""
    deleted = deactivate_merchants_by_tu(db, tu, actor=actor)
    db.commit()
    return deleted


def clear_all_merchants(
    db: Session,
    *,
    actor: str = "admin",
) -> int:
    """Deactivate every active merchant while preserving all linked history."""
    deactivated = deactivate_all_merchants(db, actor=actor)
    db.commit()
    return deactivated

