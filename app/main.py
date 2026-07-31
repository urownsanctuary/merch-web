
import os
import json
import uuid
import hashlib
import hmac
import logging
import threading
from datetime import date, datetime
from io import BytesIO
from html import escape
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlencode, urlsplit

from fastapi import FastAPI, Depends, HTTPException, Form, UploadFile, File, Cookie, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import text
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment

from app.db import SessionLocal, engine
from app.services import (
    get_active_period,
    fio_norm,
    hash_last4,
    login_user,
    get_merchants_columns,
    normalize_point_code,
    point_has_any_supply_in_month,
    get_supply_boxes_map,
    get_visits_for_month,
    get_merchant_by_fio,
    toggle_day_visit,
    toggle_inventory_visit,
    compute_point_total,
    compute_overall_total,
    days_in_month,
    weekday_of,
    month_title,
    get_monthly_submission,
    upsert_monthly_submission_draft,
    submit_monthly_submission,
    reopen_monthly_submission,
    get_admin_report_rows,
    get_admin_payroll_rows,
    get_intersections_rows,
    get_all_tu_values,
    import_supplies_xlsx,
    import_rates_xlsx,
    import_merchants_xlsx,
    clear_month_data,
    clear_all_merchants,
    clear_merchants_by_tu,
    get_point_adjustment,
    upsert_point_adjustment,
    add_special_inventory_day,
    delete_special_inventory_day,
    get_special_inventory_days,
    is_inventory_allowed_date,
    get_supply_days_for_point,
    get_supply_adjustment_amount,
    no_supply_adjustment_marker,
    filter_unadjusted_supply_days,
    get_point_rates,
    effective_has_supply,
    allowed_visit_slots,
    InventoryWeekLimitError,
    SLOT_DAY,
    SLOT_MORNING,
    SLOT_EVENING,
    SLOT_FULL_INVENT,
    normalize_visit_slot,
)
from app.production_calendar import (
    calendar_day_off,
    ensure_production_calendar_table,
    get_calendar_status,
    get_calendar_overrides,
    import_calendar_xlsx,
    reset_manual_calendar_override,
    set_manual_calendar_override,
    sync_approved_calendars,
)
from app.merchant_admin import (
    DELETE_ALL_CONFIRMATION,
    MERCHANT_SORTS,
    MerchantInputError,
    count_merchant_owned_rows,
    create_merchant,
    delete_all_merchants_and_data,
    ensure_merchant_admin_schema,
    get_merchant_for_admin,
    list_merchant_audit,
    list_merchants,
    set_merchant_active,
    update_merchant,
)
from app.security import (
    MERCHANT_COOKIE,
    create_merchant_session,
    read_merchant_session,
    reset_request_merchant,
    secure_cookie,
    set_request_merchant,
    verify_csrf,
)

app = FastAPI()
logger = logging.getLogger(__name__)


def require_draft_month(db: Session, merchant_id: int, period: dict) -> None:
    overall = compute_overall_total(db, merchant_id, period["year"], period["month"])
    if overall["submission_status"] == "submitted":
        raise HTTPException(status_code=409, detail="Reconciliation is submitted")


@app.middleware("http")
async def reject_cross_site_mutations(request: Request, call_next):
    if request.method in {"POST", "PUT", "PATCH", "DELETE"} and request.url.path not in {
        "/login",
        "/login-page",
        "/admin-login",
    }:
        if request.headers.get("sec-fetch-site", "").lower() == "cross-site":
            return HTMLResponse("Cross-site request rejected", status_code=403)
        origin = request.headers.get("origin")
        if origin:
            origin_host = urlsplit(origin).netloc.lower()
            request_host = request.headers.get("host", "").lower()
            if not origin_host or not hmac.compare_digest(origin_host, request_host):
                return HTMLResponse("Cross-site request rejected", status_code=403)
    return await call_next(request)


@app.middleware("http")
async def bind_merchant_identity(request: Request, call_next):
    session = read_merchant_session(request.cookies.get(MERCHANT_COOKIE))
    context_token = set_request_merchant(session.get("sub") if session else None)
    try:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        return response
    finally:
        reset_request_merchant(context_token)

app.mount("/static", StaticFiles(directory="app/static"), name="static")

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

ADMIN_LOGIN = os.getenv("ADMIN_LOGIN", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
SECRET_SALT = os.getenv("SECRET_SALT", "")
MAX_RECEIPT_BYTES = int(os.getenv("MAX_RECEIPT_BYTES", str(5 * 1024 * 1024)))
NON_WORKING_CONFIRM_MESSAGE = (
    "Это выходной или праздничный день по производственному календарю. "
    "Вы действительно работали в этот день?"
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.on_event("startup")
def ensure_admin_schema_on_startup():
    db = SessionLocal()
    try:
        ensure_merchant_admin_schema(db)
        ensure_production_calendar_table(db)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    if (
        os.getenv("ENVIRONMENT", "").lower() != "test"
        and os.getenv("AUTO_SYNC_PRODUCTION_CALENDAR", "1") == "1"
    ):
        threading.Thread(
            target=_sync_calendar_background,
            kwargs={"years": None},
            name="production-calendar-sync",
            daemon=True,
        ).start()


def _sync_calendar_background(years: list[int] | None) -> None:
    db = SessionLocal()
    try:
        result = sync_approved_calendars(db, years)
        logger.info(
            "production_calendar_sync_complete years=%s rows=%s unavailable=%s",
            result["synced_years"],
            result["loaded_rows"],
            result["unavailable_years"],
        )
    except Exception:
        db.rollback()
        logger.exception("production_calendar_sync_failed")
    finally:
        db.close()


def get_admin_cookie_value() -> str:
    raw = f"{ADMIN_LOGIN}:{ADMIN_PASSWORD}:{SECRET_SALT}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def is_admin_authenticated(admin_auth: Optional[str]) -> bool:
    if not ADMIN_LOGIN or not ADMIN_PASSWORD or not SECRET_SALT:
        return False
    return bool(admin_auth) and hmac.compare_digest(admin_auth, get_admin_cookie_value())


def get_admin_csrf_token(admin_auth: str) -> str:
    return hmac.new(
        SECRET_SALT.encode("utf-8"),
        f"admin-csrf:{admin_auth}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_admin_csrf(admin_auth: str | None, csrf_token: str) -> bool:
    if not is_admin_authenticated(admin_auth) or not csrf_token:
        return False
    return hmac.compare_digest(
        get_admin_csrf_token(str(admin_auth)), str(csrf_token)
    )


def log_redacted_exception(event: str, exc: Exception) -> None:
    """Log traceback frames without exception values or SQL parameters."""
    logger.error(
        "%s error_type=%s",
        event,
        type(exc).__name__,
        exc_info=(
            RuntimeError,
            RuntimeError("technical details redacted"),
            exc.__traceback__,
        ),
    )


def safe_admin_tu_values(db: Session) -> list[str]:
    try:
        return get_all_tu_values(db)
    except Exception as exc:
        db.rollback()
        log_redacted_exception("admin_merchant_tu_lookup_failed", exc)
        return []


def style_sheet(ws):
    green_fill = PatternFill("solid", fgColor="E8F5E9")
    bold = Font(bold=True)
    for cell in ws[1]:
        cell.font = bold
        cell.fill = green_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for cell in col:
            value = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(value))
            cell.alignment = Alignment(vertical="top", wrap_text=True)
        ws.column_dimensions[col_letter].width = min(max(max_len + 2, 12), 35)
    ws.freeze_panes = "A2"


def build_excel_response(wb: Workbook, filename: str) -> StreamingResponse:
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )



def split_receipt_paths(value: str | None) -> list[str]:
    if not value:
        return []
    return [p.strip() for p in str(value).split("|") if p and p.strip()]


def append_receipt_paths(existing: str | None, new_paths: list[str]) -> str | None:
    paths = split_receipt_paths(existing)
    paths.extend([p for p in new_paths if p])
    return "|".join(paths) if paths else None


def safe_receipt_filename(filename: str | None) -> str:
    raw = (filename or "receipt").strip() or "receipt"
    ext = Path(raw).suffix.lower()
    if ext not in {".jpg", ".jpeg", ".png", ".pdf", ".webp"}:
        ext = ".bin"
    return f"receipt{ext}"


def ensure_receipt_files_table(db: Session):
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS receipt_files (
            file_id TEXT PRIMARY KEY,
            original_filename TEXT,
            content_type TEXT,
            data BYTEA NOT NULL,
            merchant_id INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """))
    db.execute(text("ALTER TABLE receipt_files ADD COLUMN IF NOT EXISTS merchant_id INTEGER"))
    db.commit()


def validate_receipt(original_filename: str | None, content_type: str | None, content: bytes) -> None:
    name = Path(original_filename or "").name
    extension = Path(name).suffix.lower()
    content_type = str(content_type or "").lower()
    allowed = {
        "application/pdf": ({".pdf"}, (b"%PDF-",)),
        "image/png": ({".png"}, (b"\x89PNG\r\n\x1a\n",)),
        "image/jpeg": ({".jpg", ".jpeg"}, (b"\xff\xd8\xff",)),
        "image/webp": ({".webp"}, (b"RIFF",)),
    }
    if not content or len(content) > MAX_RECEIPT_BYTES:
        raise ValueError("Чек пуст или превышает допустимый размер")
    if content_type not in allowed or extension not in allowed[content_type][0]:
        raise ValueError("Разрешены только PDF, PNG, JPEG и WEBP с корректным MIME")
    if not any(content.startswith(signature) for signature in allowed[content_type][1]):
        raise ValueError("Содержимое чека не соответствует заявленному типу")
    if content_type == "image/webp" and content[8:12] != b"WEBP":
        raise ValueError("Некорректный WEBP")


def save_receipt_file_to_db(
    db: Session,
    merchant_id: int,
    original_filename: str | None,
    content_type: str | None,
    content: bytes,
) -> str:
    ensure_receipt_files_table(db)
    validate_receipt(original_filename, content_type, content)
    file_id = uuid.uuid4().hex
    display_filename = safe_receipt_filename(original_filename)
    db.execute(text("""
        INSERT INTO receipt_files (file_id, original_filename, content_type, data, merchant_id)
        VALUES (:file_id, :original_filename, :content_type, :data, :merchant_id)
    """), {
        "file_id": file_id,
        "original_filename": original_filename or display_filename,
        "content_type": content_type or "application/octet-stream",
        "data": content,
        "merchant_id": merchant_id,
    })
    return f"receipts/{file_id}/{display_filename}"


def render_receipt_links(value: str | None, text: str = "Открыть") -> str:
    paths = split_receipt_paths(value)
    if not paths:
        return "—"
    links = []
    for idx, path in enumerate(paths, start=1):
        label = text if len(paths) == 1 else f"{text} {idx}"
        links.append(f"<a href='/{escape(path)}' target='_blank'>{label}</a>")
    return "<br>".join(links)

def render_multiline_text(value: str | None) -> str:
    text_value = (value or "").strip()
    if not text_value:
        return "—"
    return escape(text_value).replace("\n", "<br>")


def append_multiline_comment(existing: str | None, amount: int, comment: str) -> str:
    line = f"{amount} ₽ — {comment.strip()}"
    existing_clean = (existing or "").strip()
    return line if not existing_clean else existing_clean + "\n" + line



def split_adjustment_lines(value: str | None) -> list[str]:
    """Возвращает непустые строки примечаний/возмещений."""
    if not value:
        return []
    return [line.strip() for line in str(value).splitlines() if line and line.strip()]


def parse_amount_from_adjustment_line(line: str) -> int:
    """Достаёт сумму из строки формата '1500 ₽ — комментарий' или '-400 ₽ — комментарий'."""
    match = __import__("re").match(r"^\s*([+-]?\d+)", str(line or "").strip())
    if not match:
        return 0
    try:
        return int(match.group(1))
    except Exception:
        return 0


def sum_adjustment_lines(lines: list[str]) -> int:
    return sum(parse_amount_from_adjustment_line(line) for line in lines)


def render_adjustment_items(
    value: str | None,
    delete_url: str,
    fio: str,
    point_code: str,
    empty_text: str = "—",
) -> str:
    lines = split_adjustment_lines(value)
    if not lines:
        return empty_text

    html = "<div style='display:flex; flex-direction:column; gap:10px; margin-top:10px;'>"
    for idx, line in enumerate(lines):
        html += f"""
        <div style="border:1px solid #E5E7EB; border-radius:12px; padding:10px 12px; background:#FFFFFF;">
            <div style="font-size:15px; line-height:1.35;">{escape(line)}</div>
            <form method="post" action="{delete_url}" style="margin-top:8px;">
                <input type="hidden" name="fio" value="{escape(fio)}" />
                <input type="hidden" name="point_code" value="{escape(point_code)}" />
                <input type="hidden" name="item_index" value="{idx}" />
                <button class="btn btn-danger btn-small" type="submit" style="width:auto; padding:8px 12px; font-size:13px; margin-top:0;">
                    Удалить
                </button>
            </form>
        </div>
        """
    html += "</div>"
    return html



def base_css():
    return """
    <style>
        @font-face {
            font-family: 'Villula';
            src: url('/static/fonts/villula-regular.ttf') format('truetype');
            font-weight: normal;
            font-style: normal;
        }

        :root {
            --bg: #F6F8F7;
            --card: #FFFFFF;
            --text: #1F2937;
            --muted: #6B7280;
            --line: #D1D5DB;
            --green: #2E7D32;
            --green-dark: #27682A;
            --soft: #EEF4EF;
            --soft-2: #F3F7F3;
            --error: #B91C1C;
            --ok: #166534;
            --shadow: 0 12px 32px rgba(0, 0, 0, 0.08);
        }

        * { box-sizing: border-box; }

        body {
            margin: 0;
            background: var(--bg);
            color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
            min-height: 100vh;
            padding: 20px;
        }

        .page {
            max-width: 1580px;
            margin: 0 auto;
            min-height: calc(100vh - 40px);
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .card {
            width: 100%;
            max-width: 430px;
            background: var(--card);
            border-radius: 24px;
            padding: 32px 28px;
            box-shadow: var(--shadow);
        }

        .card-wide {
            width: 100%;
            max-width: 1580px;
            background: var(--card);
            border-radius: 24px;
            padding: 22px 20px 28px;
            box-shadow: var(--shadow);
        }

        .brand {
            font-family: 'Villula', -apple-system, sans-serif;
            font-size: 28px;
            line-height: 1;
            color: var(--green);
            margin-bottom: 8px;
        }

        h1 {
            font-family: 'Villula', -apple-system, sans-serif;
            font-size: 34px;
            line-height: 1.05;
            margin: 0 0 8px 0;
            color: var(--text);
        }

        .subtitle {
            color: var(--muted);
            font-size: 15px;
            line-height: 1.45;
            margin-bottom: 18px;
        }

        label {
            display: block;
            margin: 14px 0 6px;
            font-size: 14px;
            font-weight: 700;
            color: var(--text);
        }

        input, textarea, select {
            width: 100%;
            padding: 14px 16px;
            border: 1px solid var(--line);
            border-radius: 14px;
            font-size: 16px;
            background: #fff;
            font-family: inherit;
        }

        textarea {
            resize: vertical;
            min-height: 92px;
        }

        input:focus, textarea:focus, select:focus {
            outline: none;
            border-color: var(--green);
            box-shadow: 0 0 0 3px rgba(46, 125, 50, 0.10);
        }

        .btn {
            display: inline-block;
            width: 100%;
            margin-top: 16px;
            padding: 15px 16px;
            border: none;
            border-radius: 14px;
            background: var(--green);
            color: #fff;
            font-size: 16px;
            font-weight: 800;
            text-align: center;
            text-decoration: none;
            cursor: pointer;
        }

        .btn:hover { background: var(--green-dark); }

        .btn-secondary {
            background: var(--soft);
            color: var(--text);
        }

        .btn-secondary:hover { background: #e3ece3; }

        .btn-danger {
            background: #B91C1C;
            color: #fff;
        }

        .btn-danger:hover {
            background: #991B1B;
        }

        .btn-small {
            margin-top: 12px;
            padding: 12px 14px;
            font-size: 15px;
        }

        .btn-inline {
            width: auto;
            margin-top: 0;
            padding: 12px 16px;
            font-size: 14px;
        }

        .footer {
            margin-top: 18px;
            color: #9CA3AF;
            font-size: 12px;
            text-align: center;
        }

        .back {
            display: inline-block;
            margin-top: 18px;
            color: var(--green);
            text-decoration: none;
            font-weight: 800;
        }

        .hint {
            margin-top: 16px;
            padding: 12px 14px;
            border-radius: 14px;
            background: var(--soft);
            color: var(--muted);
            font-size: 13px;
            line-height: 1.4;
        }

        .error-box {
            margin-top: 16px;
            background: #FEF2F2;
            color: var(--error);
            border-radius: 14px;
            padding: 14px;
            line-height: 1.45;
            font-weight: 700;
        }

        .success-box {
            margin-top: 16px;
            background: #ECFDF3;
            color: var(--ok);
            border-radius: 14px;
            padding: 14px;
            line-height: 1.45;
            font-weight: 700;
        }

        .calendar-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            margin-bottom: 14px;
            flex-wrap: wrap;
        }

        .calendar-month {
            font-family: 'Villula', -apple-system, sans-serif;
            font-size: 28px;
            line-height: 1;
        }

        .calendar-meta {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }

        .mini-pill {
            background: var(--soft-2);
            border-radius: 999px;
            padding: 8px 12px;
            font-size: 13px;
            color: var(--text);
            font-weight: 700;
        }

        .sum-strip {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
            margin-bottom: 16px;
        }

        .sum-card {
            background: #F7FBF8;
            border: 1px solid #E5E7EB;
            border-radius: 16px;
            padding: 14px 16px;
        }

        .sum-title {
            color: var(--muted);
            font-size: 13px;
            margin-bottom: 6px;
        }

        .sum-value {
            font-size: 22px;
            font-weight: 900;
        }

        .details-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
            margin-bottom: 18px;
        }

        .detail-card {
            background: #FAFCFA;
            border: 1px solid #E5E7EB;
            border-radius: 16px;
            padding: 14px 16px;
        }

        .detail-title {
            color: var(--muted);
            font-size: 13px;
            margin-bottom: 8px;
        }

        .point-adjustment-card {
            padding: 18px 18px 20px;
        }

        .point-adjustment-card .detail-title {
            color: var(--text);
            font-size: 22px;
            font-weight: 800;
            margin-bottom: 14px;
            line-height: 1.2;
        }

        .point-adjustment-card label {
            font-size: 18px;
            font-weight: 800;
            color: var(--text);
            margin: 14px 0 8px;
        }

        .point-adjustment-card input[type="text"],
        .point-adjustment-card input[type="number"],
        .point-adjustment-card input[type="file"] {
            font-size: 18px;
        }

        .point-adjustment-card .hint {
            font-size: 15px;
            line-height: 1.45;
        }

        .detail-line {
            font-size: 15px;
            font-weight: 700;
            line-height: 1.5;
        }

        .calendar-wrap { margin-top: 4px; }

        .weekdays, .calendar-grid {
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 10px;
        }

        .weekdays { margin-bottom: 10px; }

        .weekday {
            text-align: center;
            font-size: 13px;
            color: var(--muted);
            font-weight: 700;
            padding: 6px 0;
        }

        .day, .day-empty {
            min-height: 90px;
            border-radius: 18px;
            padding: 10px;
        }

        .day {
            background: #F8FAF8;
            border: 1px solid #E5E7EB;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            text-decoration: none;
            color: inherit;
            cursor: pointer;
            font: inherit;
            text-align: left;
        }

        .direct-day-form {
            margin: 0;
            min-width: 0;
        }

        .direct-day-form .day {
            width: 100%;
            height: 100%;
        }

        .day:hover {
            border-color: var(--green);
            box-shadow: 0 0 0 2px rgba(46, 125, 50, 0.06);
        }

        .day-red {
            background: #FFF5F5;
            border-color: #FECACA;
        }

        .day-red .day-number {
            color: #B91C1C;
        }

        .day-empty { background: transparent; }

        .day-disabled {
            opacity: 0.65;
            cursor: default;
            pointer-events: none;
        }

        .day-number {
            font-size: 18px;
            font-weight: 800;
        }

        .day-badges {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            margin-top: 10px;
        }

        .badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 4px 8px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: 800;
            line-height: 1;
            min-width: 22px;
            height: 22px;
        }

        .badge-supply {
            background: #2E7D32;
            color: #fff;
            border-radius: 6px;
        }

        .badge-day {
            background: #DBEAFE;
            color: #1D4ED8;
        }

        .badge-inv {
            background: #FCE7F3;
            color: #BE185D;
        }

        .legend {
            margin-top: 18px;
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
        }

        .legend-item {
            background: var(--soft);
            border-radius: 999px;
            padding: 8px 12px;
            font-size: 13px;
            color: var(--text);
            font-weight: 700;
        }

        .calendar-note {
            margin-top: 14px;
            color: var(--muted);
            line-height: 1.5;
            font-size: 14px;
        }

        details.point-detail {
            margin-top: 10px;
            border: 1px solid #E5E7EB;
            border-radius: 14px;
            background: #FAFCFA;
            padding: 10px 14px;
        }

        details.point-detail summary {
            cursor: pointer;
            font-weight: 800;
            list-style: none;
        }

        details.point-detail summary::-webkit-details-marker {
            display: none;
        }

        .summary-content {
            margin-top: 10px;
            color: var(--text);
            line-height: 1.6;
        }

        .filter-grid {
            display: grid;
            grid-template-columns: 140px 140px 240px 220px;
            gap: 12px;
            margin-bottom: 16px;
            align-items: end;
        }

        .table-wrap {
            border: 1px solid #E5E7EB;
            border-radius: 16px;
            background: #fff;
            overflow: visible;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;
        }

        th, td {
            padding: 10px 8px;
            border-bottom: 1px solid #E5E7EB;
            text-align: left;
            vertical-align: top;
            font-size: 12px;
            word-break: break-word;
        }

        th {
            background: #F7FBF8;
            font-weight: 800;
        }

        .admin-actions {
            display: flex;
            gap: 12px;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
            flex-wrap: wrap;
        }

        .admin-export-buttons {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 14px;
        }

        .data-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 16px;
        }

        .merchant-filter-grid {
            display: grid;
            grid-template-columns: 2fr 1fr 1fr 1fr 1.4fr auto;
            gap: 12px;
            align-items: end;
        }

        .merchant-filter-grid label { margin-top: 0; }

        .merchant-table table { table-layout: auto; min-width: 1040px; }
        .merchant-table th, .merchant-table td { font-size: 14px; padding: 12px; }

        .merchant-cards { display: none; }

        .merchant-card {
            border: 1px solid #E5E7EB;
            border-radius: 16px;
            padding: 16px;
            background: #fff;
        }

        .merchant-card + .merchant-card { margin-top: 12px; }

        .merchant-card-title {
            font-size: 18px;
            font-weight: 900;
            margin-bottom: 10px;
        }

        .merchant-meta {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px 12px;
            font-size: 14px;
            line-height: 1.4;
        }

        .status-pill {
            display: inline-flex;
            border-radius: 999px;
            padding: 6px 10px;
            font-size: 12px;
            font-weight: 900;
            background: #ECFDF3;
            color: #166534;
        }

        .status-pill.inactive {
            background: #F3F4F6;
            color: #4B5563;
        }

        .field-error {
            margin-top: 6px;
            color: var(--error);
            font-size: 13px;
            font-weight: 700;
        }

        .danger-panel {
            border: 1px solid #FECACA;
            border-radius: 16px;
            padding: 14px;
            background: #FFF7F7;
            margin-top: 18px;
        }

        .audit-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
            margin-top: 12px;
        }

        .audit-item {
            border: 1px solid #E5E7EB;
            border-radius: 12px;
            padding: 10px 12px;
            font-size: 13px;
            line-height: 1.4;
            background: #fff;
        }

        @media (max-width: 960px) {
            .page { align-items: flex-start; }
            .card-wide { padding: 18px 14px 24px; }
            .sum-strip, .details-grid, .filter-grid, .data-grid { grid-template-columns: 1fr; }
            .weekdays, .calendar-grid { gap: 8px; }
            .day, .day-empty {
                min-height: 80px;
                border-radius: 14px;
                padding: 8px;
            }
            .day-number { font-size: 16px; }
            h1 { font-size: 30px; }
            .brand { font-size: 24px; }
            .calendar-month { font-size: 24px; }
            .table-wrap { overflow-x: auto; }
            table { min-width: 1200px; }
            .merchant-filter-grid { grid-template-columns: 1fr; }
            .merchant-table { display: none; }
            .merchant-cards { display: block; }
            .merchant-meta { grid-template-columns: 1fr; }
            .merchant-card .btn-inline { width: 100%; margin-top: 10px; }
        }
    </style>
    """


@app.get("/")
def root():
    return RedirectResponse(url="/login-page")


@app.get("/db-check")
def db_check():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok", "db": "connected"}


@app.get("/receipts/{file_id}/{filename}")
def receipt_file(
    request: Request,
    file_id: str,
    filename: str,
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    ensure_receipt_files_table(db)
    row = db.execute(text("""
        SELECT rf.original_filename, rf.content_type, rf.data, rf.merchant_id, m.fio_norm
        FROM receipt_files rf
        LEFT JOIN merchants m ON m.id=rf.merchant_id
        WHERE rf.file_id = :file_id
        LIMIT 1
    """), {"file_id": file_id}).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Not Found")
    session = read_merchant_session(request.cookies.get(MERCHANT_COOKIE))
    owner_allowed = bool(session and row["fio_norm"] and session.get("sub") == row["fio_norm"])
    if session and row["merchant_id"] is None:
        owner_allowed = db.execute(text("""
            SELECT 1
            FROM point_adjustments pa
            JOIN merchants m ON m.id=pa.merchant_id
            WHERE m.fio_norm=:fio_norm AND pa.reimb_receipt LIKE :receipt_path
            LIMIT 1
        """), {
            "fio_norm": session["sub"],
            "receipt_path": f"%receipts/{file_id}/%",
        }).first() is not None
    if not owner_allowed and not is_admin_authenticated(admin_auth):
        raise HTTPException(status_code=403, detail="Forbidden")

    content = bytes(row["data"])
    media_type = row.get("content_type") or "application/octet-stream"
    return StreamingResponse(BytesIO(content), media_type=media_type)


@app.get("/active-period")
def active_period():
    return get_active_period()


@app.get("/debug/merchants-columns")
def merchants_columns(
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        raise HTTPException(status_code=403, detail="Forbidden")
    cols = get_merchants_columns(db)
    return {"table": "merchants", "columns": cols}


@app.post("/login")
def login_api(response: Response, fio: str, last4: str, db: Session = Depends(get_db)):
    user = login_user(db, fio, last4)

    if not user:
        raise HTTPException(status_code=401, detail="Неверные данные")

    response.set_cookie(
        MERCHANT_COOKIE,
        create_merchant_session(user["fio_norm"]),
        httponly=True,
        secure=secure_cookie(),
        samesite="lax",
        max_age=12 * 60 * 60,
    )
    return {"status": "ok", "active_period": get_active_period(), "user": user}


@app.get("/login-page", response_class=HTMLResponse)
def login_page():
    period = get_active_period()
    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>ВкусВилл</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>Сверки мерчендайзеров</h1>
            <div class="subtitle">Введите ФИО и последние 4 цифры телефона</div>

            <form method="post" action="/login-page">
                <label for="fio">ФИО</label>
                <input id="fio" name="fio" type="text" placeholder="Иванов Иван Иванович" required />

                <label for="last4">Последние 4 цифры телефона</label>
                <input id="last4" name="last4" type="text" inputmode="numeric" maxlength="4" placeholder="1234" required />

                <button class="btn" type="submit">Войти</button>
            </form>

            <div class="hint">Сейчас открыт период за {month_title(period["year"], period["month"])}.</div>

            <div class="footer">Веб-версия сверок мерчендайзеров</div>
        </div>
    </div>
</body>
</html>
"""


@app.post("/login-page", response_class=HTMLResponse)
def login_submit(
    fio: str = Form(...),
    last4: str = Form(...),
    db: Session = Depends(get_db)
):
    user = login_user(db, fio, last4)

    if not user:
        return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Ошибка входа</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <h1>Ошибка входа</h1>
            <div class="error-box">Неверные данные. Проверьте ФИО и последние 4 цифры телефона.</div>
            <a class="back" href="/login-page">← Попробовать снова</a>
        </div>
    </div>
</body>
</html>
"""

    response = RedirectResponse(url=f"/menu-page?fio={user['fio']}", status_code=303)
    response.set_cookie(
        MERCHANT_COOKIE,
        create_merchant_session(user["fio_norm"]),
        httponly=True,
        secure=secure_cookie(),
        samesite="lax",
        max_age=12 * 60 * 60,
    )
    return response


@app.get("/merchant-logout")
def merchant_logout():
    response = RedirectResponse(url="/login-page", status_code=303)
    response.delete_cookie(MERCHANT_COOKIE)
    return response


@app.get("/menu-page", response_class=HTMLResponse)
def menu_page(fio: str = "", db: Session = Depends(get_db)):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    overall = {"total": 0}
    if merchant:
        overall = compute_overall_total(db, merchant["id"], period["year"], period["month"])

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Главное меню</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>Главное меню</h1>
            <div class="subtitle">{escape(fio)}</div>
            <div class="hint">Сейчас открыт период за {month_title(period["year"], period["month"])}.</div>

            <div class="sum-card" style="margin-top: 18px;">
                <div class="sum-title">Общая сумма за месяц</div>
                <div class="sum-value">{overall["total"]} ₽</div>
            </div>

            <a class="btn" href="/point-page?fio={escape(fio)}">Заполнить сверку</a>
            <a class="btn btn-secondary" href="/summary-page?fio={escape(fio)}">Моя сумма</a>
            <a class="btn btn-secondary" href="/monthly-submit-page?fio={escape(fio)}">Отправить сверку за месяц</a>
            <a class="btn btn-secondary" href="/merchant-logout">Выйти</a>
        </div>
    </div>
</body>
</html>
"""


@app.get("/point-page", response_class=HTMLResponse)
def point_page(fio: str = ""):
    period = get_active_period()

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Выбор точки</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>Выбор точки</h1>
            <div class="subtitle">{escape(fio)}</div>

            <div class="hint" style="margin-top: 0; margin-bottom: 18px;">
                Сверка заполняется за {month_title(period["year"], period["month"])}.
            </div>

            <form method="post" action="/point-page">
                <input type="hidden" name="fio" value="{escape(fio)}" />

                <label for="point_code">Номер точки</label>
                <input id="point_code" name="point_code" type="text" placeholder="2674" required />

                <button class="btn" type="submit">Продолжить</button>
            </form>

            <a class="back" href="/menu-page?fio={escape(fio)}">← Назад</a>
        </div>
    </div>
</body>
</html>
"""


@app.post("/point-page", response_class=HTMLResponse)
def point_submit(
    fio: str = Form(...),
    point_code: str = Form(...),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    point_code = normalize_point_code(point_code)

    if not point_code or len(point_code) < 3:
        return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Ошибка</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <h1>Ошибка</h1>
            <div class="error-box">Номер точки слишком короткий.</div>
            <a class="back" href="/point-page?fio={escape(fio)}">← Назад</a>
        </div>
    </div>
</body>
</html>
"""

    has_supply = point_has_any_supply_in_month(db, point_code, period["year"], period["month"])

    if not has_supply:
        return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Точка не найдена</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <h1>Точка не найдена</h1>
            <div class="error-box">
                В периоде {month_title(period["year"], period["month"])} по точке {escape(point_code)} нет поставок.
                <br><br>
                Проверьте номер точки или обратитесь к управляющему.
            </div>
            <a class="back" href="/point-page?fio={escape(fio)}">← Попробовать снова</a>
        </div>
    </div>
</body>
</html>
"""

    return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code)}", status_code=303)


def build_day_href(fio: str, point_code: str, y: int, m: int, day: int, is_submitted: bool, inventory_allowed: bool) -> str:
    if is_submitted:
        return "#"
    return f"/day-action-page?fio={escape(fio)}&point_code={escape(point_code)}&day={day}"


def russian_non_working_dates(year: int) -> set[date]:
    """Основные нерабочие праздничные дни РФ + перенос на понедельник, если праздник выпал на выходной.

    Это нужно только для визуальной подсветки календаря. Расчёт оплаты не меняет.
    """
    fixed_ranges = []
    # Новогодние каникулы и Рождество
    for d in range(1, 9):
        fixed_ranges.append(date(year, 1, d))

    fixed_days = [
        date(year, 2, 23),
        date(year, 3, 8),
        date(year, 5, 1),
        date(year, 5, 9),
        date(year, 6, 12),
        date(year, 11, 4),
    ]

    result = set(fixed_ranges + fixed_days)

    # Базовый перенос: если праздник попал на субботу/воскресенье, подсвечиваем ближайший понедельник.
    # Для точных ежегодных переносов можно позже добавить отдельную админ-таблицу производственного календаря.
    for d in fixed_days:
        if d.weekday() == 5:
            result.add(d + __import__("datetime").timedelta(days=2))
        elif d.weekday() == 6:
            result.add(d + __import__("datetime").timedelta(days=1))

    return result


def is_calendar_red_day(current_date: date, overrides: dict[date, bool]) -> bool:
    return calendar_day_off(current_date, overrides)


def build_calendar_html(
    fio: str,
    point_code: str,
    y: int,
    m: int,
    boxes_map: dict[int, int],
    visits: dict[int, set[str]],
    is_submitted: bool,
    special_inventory_days: set[date],
    calendar_overrides: dict[date, bool],
    pay_lt5: bool = False,
    csrf_token: str = "",
) -> str:
    dim = days_in_month(y, m)
    first_wd = weekday_of(y, m, 1)
    weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

    html = '<div class="weekdays">'
    for wd in weekdays:
        html += f'<div class="weekday">{wd}</div>'
    html += '</div>'

    html += '<div class="calendar-grid">'

    for _ in range(first_wd):
        html += '<div class="day-empty"></div>'

    for day in range(1, dim + 1):
        boxes = boxes_map.get(day, 0)
        day_visits = visits.get(day, set())

        badges = ""
        if effective_has_supply(boxes, pay_lt5):
            badges += '<span class="badge badge-supply">П</span>'
        if day_visits.intersection({"DAY", "MORNING"}):
            badges += '<span class="badge badge-day">В</span>'
        if day_visits.intersection({"EVENING", "FULL_INVENT"}):
            badges += '<span class="badge badge-inv">И</span>'

        current_date = date(y, m, day)
        inventory_allowed = current_date.weekday() in (4, 5) or current_date in special_inventory_days
        allowed_slots = allowed_visit_slots(
            current_date,
            special_inventory=current_date in special_inventory_days,
        )
        direct_day_toggle = allowed_slots == {SLOT_DAY}
        cls_parts = ["day"]
        non_working_day = is_calendar_red_day(current_date, calendar_overrides)
        if non_working_day:
            cls_parts.append("day-red")
        if is_submitted:
            cls_parts.append("day-disabled")
        cls = " ".join(cls_parts)

        day_content = f"""
            <div class="day-number">{day}</div>
            <div class="day-badges">{badges}</div>
        """
        if direct_day_toggle and not is_submitted:
            adding_day = SLOT_DAY not in day_visits
            confirmation_input = (
                '<input type="hidden" name="confirm_non_working" value="1" />'
                if non_working_day and adding_day
                else ""
            )
            confirmation_attr = (
                f' data-confirm="{escape(NON_WORKING_CONFIRM_MESSAGE)}"'
                if non_working_day and adding_day
                else ""
            )
            html += f"""
            <form class="direct-day-form" method="post" action="/toggle-day"{confirmation_attr}>
                <input type="hidden" name="fio" value="{escape(fio)}" />
                <input type="hidden" name="point_code" value="{escape(point_code)}" />
                <input type="hidden" name="day" value="{day}" />
                <input type="hidden" name="slot" value="{SLOT_DAY}" />
                <input type="hidden" name="csrf_token" value="{escape(csrf_token)}" />
                {confirmation_input}
                <button class="{cls}" type="submit">{day_content}</button>
            </form>
            """
        else:
            href = build_day_href(
                fio, point_code, y, m, day, is_submitted, inventory_allowed
            )
            html += f"""
            <a class="{cls}" href="{href}">
                {day_content}
            </a>
            """

    html += '</div>'
    return html


@app.get("/calendar-page", response_class=HTMLResponse)
def calendar_page(
    request: Request,
    fio: str,
    point_code: str,
    saved: str = "",
    visit_error: str = "",
    inventory_date: str = "",
    db: Session = Depends(get_db)
):
    period = get_active_period()
    y = period["year"]
    m = period["month"]

    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    session = read_merchant_session(request.cookies.get(MERCHANT_COOKIE))
    if not session:
        return RedirectResponse(url="/login-page", status_code=303)

    point_code = normalize_point_code(point_code)

    overall = compute_overall_total(db, merchant["id"], y, m)
    monthly_submitted = overall["submission_status"] == "submitted"

    boxes_map = get_supply_boxes_map(db, point_code, y, m)
    visits = get_visits_for_month(db, merchant["id"], point_code, y, m)
    point_total = compute_point_total(db, merchant["id"], point_code, y, m)
    point_adj = get_point_adjustment(db, merchant["id"], point_code, y, m) or {}
    special_inventory_days = set(get_special_inventory_days(db))
    calendar_overrides = get_calendar_overrides(db, y, m)

    calendar_html = build_calendar_html(
        fio=fio,
        point_code=point_code,
        y=y,
        m=m,
        boxes_map=boxes_map,
        visits=visits,
        is_submitted=monthly_submitted,
        special_inventory_days=special_inventory_days,
        calendar_overrides=calendar_overrides,
        pay_lt5=bool(point_total.get("pay_lt5")),
        csrf_token=session["csrf"],
    )

    info_box = ""
    if saved == "1":
        info_box = "<div class=\"success-box\">Данные по точке сохранены.</div>"
    if visit_error == "inventory_week":
        try:
            first_inventory_date = date.fromisoformat(inventory_date)
        except ValueError:
            first_inventory_date = None
        if first_inventory_date:
            info_box += (
                '<div class="error-box">'
                "На этой точке уже отмечен полный инвент на этой неделе: "
                f"{first_inventory_date.strftime('%d.%m.%Y')}. "
                "Разрешён только один полный инвент в неделю."
                "</div>"
            )

    point_receipt_links = render_receipt_links(point_total.get("reimb_receipt"))
    point_receipt_link = ""
    if point_receipt_links != "—":
        point_receipt_link = f"<div class='hint' style='margin-top:10px'>Чеки по возмещению:<br>{point_receipt_links}</div>"

    coffee_meta_html = ""
    coffee_card_html = ""
    if point_total.get("coffee_enabled"):
        coffee_meta_html = '<div class="mini-pill">КМ: Да</div>'
        coffee_card_html = (
            '<div class="detail-card">'
            '<div class="detail-title">Кофемашина</div>'
            f'<div class="detail-line">{point_total["coffee_cnt"]} × {point_total["coffee_rate"]} ₽ = {point_total["coffee_sum"]} ₽</div>'
            '</div>'
        )

    supply_policy_note = (
        "Для этой точки поставки от 1 коробки оплачиваются по ставке поставки."
        if point_total.get("pay_lt5")
        else "Поставки до 5 коробок не оплачиваются."
    )

    point_form = ""
    if not monthly_submitted:
        point_form = f"""
            <div class="details-grid" style="margin-top:18px;">
                <div class="detail-card point-adjustment-card">
                    <div class="detail-title">Примечание по точке</div>
                    <div class="detail-line">{point_total['note_amount']} ₽</div>
                    <div class="calendar-note">{render_adjustment_items(point_total['note_comment'], '/delete-point-note', fio, point_code)}</div>
                    <a class="btn btn-secondary" href="/point-note-page?fio={escape(fio)}&point_code={escape(point_code)}">Добавить примечание</a>
                </div>

                <div class="detail-card point-adjustment-card">
                    <div class="detail-title">Возмещение по точке</div>
                    <div class="detail-line">{point_total['reimb_amount']} ₽</div>
                    <div class="calendar-note">{render_adjustment_items(point_total['reimb_comment'], '/delete-point-reimbursement', fio, point_code)}</div>
                    {point_receipt_link}
                    <a class="btn btn-secondary" href="/point-reimbursement-page?fio={escape(fio)}&point_code={escape(point_code)}">Добавить возмещение</a>
                </div>
            </div>
        """

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Календарь</title>
    {base_css()}
    <script>
    document.addEventListener('DOMContentLoaded', function() {{
        const savedY = sessionStorage.getItem('calendarScrollY');
        if (savedY) {{
            window.scrollTo(0, parseInt(savedY, 10));
            sessionStorage.removeItem('calendarScrollY');
        }}
        const cleanUrl = new URL(window.location.href);
        if (cleanUrl.searchParams.has('visit_error')) {{
            cleanUrl.searchParams.delete('visit_error');
            cleanUrl.searchParams.delete('inventory_date');
            window.history.replaceState({{}}, '', cleanUrl.toString());
        }}
        document.querySelectorAll('a.day').forEach(el => {{
            el.addEventListener('click', function() {{
                sessionStorage.setItem('calendarScrollY', String(window.scrollY));
            }});
        }});
        document.querySelectorAll('.direct-day-form').forEach(form => {{
            form.addEventListener('submit', async function(event) {{
                event.preventDefault();
                const confirmation = form.dataset.confirm;
                if (confirmation && !window.confirm(confirmation)) {{
                    return;
                }}
                sessionStorage.setItem('calendarScrollY', String(window.scrollY));
                const button = form.querySelector('button[type="submit"]');
                button.disabled = true;
                try {{
                    const response = await fetch(form.action, {{
                        method: 'POST',
                        body: new FormData(form),
                        credentials: 'same-origin',
                    }});
                    if (!response.ok) {{
                        const message = await response.text();
                        window.alert(message || 'Не удалось сохранить выход. Попробуйте ещё раз.');
                        button.disabled = false;
                        return;
                    }}
                    window.location.assign(response.url);
                }} catch (error) {{
                    window.alert('Не удалось сохранить выход. Проверьте соединение и попробуйте ещё раз.');
                    button.disabled = false;
                }}
            }});
        }});
    }});
    </script>
</head>
<body>
    <div class="page">
        <div class="card-wide">
            <div class="calendar-head">
                <div>
                    <div class="brand">ВкусВилл</div>
                    <div class="calendar-month">{month_title(y, m)}</div>
                </div>

                <div class="calendar-meta">
                    <div class="mini-pill">Точка: {escape(point_code)}</div>
                    <div class="mini-pill">{escape(fio)}</div>
                    {coffee_meta_html}
                    <div class="mini-pill">Месячная сверка: {"Отправлена" if monthly_submitted else "Черновик"}</div>
                </div>
            </div>

            {info_box}

            <div class="sum-strip">
                <div class="sum-card">
                    <div class="sum-title">Сумма по точке</div>
                    <div class="sum-value">{point_total["total"]} ₽</div>
                </div>

                <div class="sum-card">
                    <div class="sum-title">Общая сумма за месяц</div>
                    <div class="sum-value">{overall["total"]} ₽</div>
                </div>
            </div>

            <div class="details-grid">
                <div class="detail-card">
                    <div class="detail-title">Выходы с поставкой</div>
                    <div class="detail-line">{point_total["cnt_supply"]} × {point_total["rate_supply"]} ₽ = {point_total["sum_supply"]} ₽</div>
                </div>

                <div class="detail-card">
                    <div class="detail-title">Выходы без поставки</div>
                    <div class="detail-line">{point_total["cnt_no_supply"]} × {point_total["rate_no_supply"]} ₽ = {point_total["sum_no_supply"]} ₽</div>
                </div>

                <div class="detail-card">
                    <div class="detail-title">Полные инвенты</div>
                    <div class="detail-line">{point_total["cnt_full_inv"]} × {point_total["rate_inventory"]} ₽ = {point_total["sum_inventory"]} ₽</div>
                </div>

                {coffee_card_html}
            </div>


            <div class="calendar-wrap">
                {calendar_html}
            </div>

            <div class="legend">
                <div class="legend-item">П — оплачиваемая поставка</div>
                <div class="legend-item">В — отмечен выход</div>
                <div class="legend-item">И — полный инвент</div>
            </div>

            <div class="calendar-note">
                В обычные дни отмечается один выход. В пятницу и субботу можно отдельно
                отметить утренний выход и вечерний полный инвент.
            </div>

            <div class="calendar-note">
                {supply_policy_note}
            </div>

            {point_form}

            <div class="admin-export-buttons" style="margin-top:18px;">
                <a class="btn btn-secondary btn-inline" href="/point-page?fio={escape(fio)}">Следующая точка</a>
                <a class="btn btn-secondary btn-inline" href="/summary-page?fio={escape(fio)}">Моя сумма</a>
                <a class="btn btn-secondary btn-inline" href="/monthly-submit-page?fio={escape(fio)}">Отправить сверку за месяц</a>
            </div>

            <a class="back" href="/menu-page?fio={escape(fio)}">← На главный экран</a>
        </div>
    </div>
</body>
</html>
"""


@app.get("/point-note-page", response_class=HTMLResponse)
def point_note_page(
    fio: str,
    point_code: str,
    mode: str = "",
    db: Session = Depends(get_db)
):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    require_draft_month(db, merchant["id"], period)

    point_code_clean = normalize_point_code(point_code)
    point_total = compute_point_total(db, merchant["id"], point_code_clean, period["year"], period["month"])

    if not mode:
        return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Добавить примечание</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>Добавить примечание</h1>
            <div class="subtitle">Точка: {escape(point_code_clean)}</div>

            <a class="btn" href="/point-note-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&mode=normal">
                1. Обычное примечание<br>
                <span style="font-size:13px;font-weight:600;">например: закрытие точки</span>
            </a>

            <a class="btn btn-secondary" href="/point-note-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&mode=no_supply">
                2. Не принимал поставку в день с поставкой<br>
                <span style="font-size:13px;font-weight:600;">если вы вышли на точку в день с поставкой, но поставку принимал другой сотрудник</span>
            </a>

            <a class="back" href="/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}">← Назад к точке</a>
        </div>
    </div>
</body>
</html>
"""

    if mode == "normal":
        return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Обычное примечание</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>Обычное примечание</h1>
            <div class="subtitle">Точка: {escape(point_code_clean)}</div>

            <form method="post" action="/save-point-note-normal" autocomplete="off">
                <input type="hidden" name="fio" value="{escape(fio)}" />
                <input type="hidden" name="point_code" value="{escape(point_code_clean)}" />

                <label for="note_amount">Сумма, ₽</label>
                <input id="note_amount" name="note_amount" type="number" min="1" value="" placeholder="Например: 1500" required />

                <label for="note_comment">Комментарий</label>
                <input id="note_comment" name="note_comment" type="text" value="" placeholder="Например: Закрытие точки" required />

                <button class="btn" type="submit">Сохранить примечание</button>
            </form>

            <a class="back" href="/point-note-page?fio={escape(fio)}&point_code={escape(point_code_clean)}">← Назад</a>
        </div>
    </div>
</body>
</html>
"""

    if mode == "no_supply":
        supply_days = get_supply_days_for_point(db, point_code_clean, period["year"], period["month"])
        existing = get_point_adjustment(
            db, merchant["id"], point_code_clean, period["year"], period["month"]
        ) or {}
        existing_note_comment = str(existing.get("note_comment") or "")
        supply_days = filter_unadjusted_supply_days(supply_days, existing_note_comment)
        adjustment = get_supply_adjustment_amount(db, point_code_clean, period["year"], period["month"])
        options = "".join([f"<option value='{d.day}'>{d.strftime('%d.%m.%Y')}</option>" for d in supply_days])
        disabled = "" if supply_days else " disabled"
        if not options:
            options = "<option value='' selected>Нет дней с поставкой</option>"

        return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Не принимал поставку</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>Не принимал поставку</h1>
            <div class="subtitle">Точка: {escape(point_code_clean)}</div>

            <div class="hint">Выберите день с поставкой. Сумма корректировки рассчитается автоматически.</div>

            <form method="post" action="/save-point-note-no-supply" autocomplete="off">
                <input type="hidden" name="fio" value="{escape(fio)}" />
                <input type="hidden" name="point_code" value="{escape(point_code_clean)}" />

                <label for="supply_day">День с поставкой</label>
                <select id="supply_day" name="supply_day" required{disabled}>
                    {options}
                </select>

                <div class="sum-card" style="margin-top:16px;">
                    <div class="sum-title">Корректировка</div>
                    <div class="sum-value">{adjustment} ₽</div>
                </div>

                <label for="comment">Комментарий</label>
                <input id="comment" name="comment" type="text" placeholder="Не принимал поставку" />

                <button class="btn" type="submit"{disabled}>Сохранить примечание</button>
            </form>

            <a class="back" href="/point-note-page?fio={escape(fio)}&point_code={escape(point_code_clean)}">← Назад</a>
        </div>
    </div>
</body>
</html>
"""

    return RedirectResponse(url=f"/point-note-page?fio={escape(fio)}&point_code={escape(point_code_clean)}", status_code=303)


@app.post("/save-point-note-normal")
def save_point_note_normal(
    fio: str = Form(...),
    point_code: str = Form(...),
    note_amount: int = Form(...),
    note_comment: str = Form(...),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    require_draft_month(db, merchant["id"], period)

    point_code_clean = normalize_point_code(point_code)
    note_amount_value = int(note_amount or 0)
    note_comment_value = (note_comment or "").strip()

    if note_amount_value <= 0 or not note_comment_value:
        return HTMLResponse(f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Ошибка</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <h1>Ошибка</h1>
            <div class="error-box">Для примечания необходимо заполнить сумму больше 0 и комментарий.</div>
            <a class="back" href="/point-note-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&mode=normal">← Вернуться к примечанию</a>
        </div>
    </div>
</body>
</html>
        """, status_code=400)

    existing = get_point_adjustment(db, merchant["id"], point_code_clean, period["year"], period["month"]) or {}
    existing_note_amount = int(existing.get("note_amount") or 0)
    existing_note_comment = existing.get("note_comment") or ""
    new_note_comment = append_multiline_comment(existing_note_comment, note_amount_value, note_comment_value)

    upsert_point_adjustment(
        db=db,
        merchant_id=merchant["id"],
        point_code=point_code_clean,
        y=period["year"],
        m=period["month"],
        note_amount=existing_note_amount + note_amount_value,
        note_comment=new_note_comment,
        reimb_amount=int(existing.get("reimb_amount") or 0),
        reimb_comment=existing.get("reimb_comment") or "",
        reimb_receipt=existing.get("reimb_receipt"),
    )

    return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&saved=1", status_code=303)


@app.post("/save-point-note-no-supply")
def save_point_note_no_supply(
    fio: str = Form(...),
    point_code: str = Form(...),
    supply_day: int = Form(...),
    comment: str = Form(""),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    require_draft_month(db, merchant["id"], period)

    point_code_clean = normalize_point_code(point_code)
    supply_date = date(period["year"], period["month"], int(supply_day))
    allowed_dates = set(get_supply_days_for_point(
        db, point_code_clean, period["year"], period["month"]
    ))
    if supply_date not in allowed_dates:
        raise HTTPException(status_code=400, detail="Дата не является оплачиваемым днём поставки")

    adjustment = get_supply_adjustment_amount(db, point_code_clean, period["year"], period["month"])
    base_comment = no_supply_adjustment_marker(supply_date)
    note_comment = base_comment if not comment else f"{base_comment}. {comment}"
    existing = get_point_adjustment(db, merchant["id"], point_code_clean, period["year"], period["month"]) or {}
    existing_note_amount = int(existing.get("note_amount") or 0)
    existing_note_comment = existing.get("note_comment") or ""
    if base_comment in existing_note_comment:
        raise HTTPException(status_code=409, detail="Корректировка для этой даты уже добавлена")
    new_note_comment = append_multiline_comment(existing_note_comment, adjustment, note_comment)

    upsert_point_adjustment(
        db=db,
        merchant_id=merchant["id"],
        point_code=point_code_clean,
        y=period["year"],
        m=period["month"],
        note_amount=existing_note_amount + adjustment,
        note_comment=new_note_comment,
        reimb_amount=int(existing.get("reimb_amount") or 0),
        reimb_comment=existing.get("reimb_comment") or "",
        reimb_receipt=existing.get("reimb_receipt"),
    )

    return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&saved=1", status_code=303)


@app.get("/point-reimbursement-page", response_class=HTMLResponse)
def point_reimbursement_page(
    fio: str,
    point_code: str,
    db: Session = Depends(get_db)
):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    require_draft_month(db, merchant["id"], period)

    point_code_clean = normalize_point_code(point_code)
    point_total = compute_point_total(db, merchant["id"], point_code_clean, period["year"], period["month"])
    receipt_links = render_receipt_links(point_total.get("reimb_receipt"))

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Возмещение</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>Добавить возмещение</h1>
            <div class="subtitle">Точка: {escape(point_code_clean)}</div>

            <form method="post" action="/save-point-reimbursement" enctype="multipart/form-data" autocomplete="off">
                <input type="hidden" name="fio" value="{escape(fio)}" />
                <input type="hidden" name="point_code" value="{escape(point_code_clean)}" />

                <label for="reimb_amount">Сумма, ₽</label>
                <input id="reimb_amount" name="reimb_amount" type="number" min="1" value="" placeholder="Например: 150" required />

                <label for="reimb_comment">Комментарий</label>
                <input id="reimb_comment" name="reimb_comment" type="text" value="" placeholder="Например: Покупка пакетов" required />

                <label for="reimb_receipts">Чеки</label>
                <input id="reimb_receipts" name="reimb_receipts" type="file" accept=".jpg,.jpeg,.png,.pdf,.webp" multiple />
                <div class="hint">Если указано возмещение, необходимо прикрепить чек. Можно загрузить несколько чеков.</div>

                <div class="hint" style="margin-top:10px;">Уже загруженные чеки по этой точке:<br>{receipt_links}</div>

                <button class="btn" type="submit">Сохранить возмещение</button>
            </form>

            <a class="back" href="/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}">← Назад к точке</a>
        </div>
    </div>
</body>
</html>
"""


@app.post("/save-point-reimbursement")
async def save_point_reimbursement(
    fio: str = Form(...),
    point_code: str = Form(...),
    reimb_amount: int = Form(0),
    reimb_comment: str = Form(""),
    reimb_receipts: List[UploadFile] = File(default=[]),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    require_draft_month(db, merchant["id"], period)

    point_code_clean = normalize_point_code(point_code)
    reimb_amount_value = int(reimb_amount or 0)
    reimb_comment_value = (reimb_comment or "").strip()
    existing = get_point_adjustment(db, merchant["id"], point_code_clean, period["year"], period["month"]) or {}

    new_paths = []
    for receipt in reimb_receipts or []:
        if receipt and receipt.filename:
            content = await receipt.read()
            if content:
                new_paths.append(save_receipt_file_to_db(db, merchant["id"], receipt.filename, receipt.content_type, content))

    if reimb_amount_value <= 0 or not reimb_comment_value or not new_paths:
        return HTMLResponse(f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Ошибка</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <h1>Ошибка</h1>
            <div class="error-box">Для возмещения необходимо заполнить сумму больше 0, комментарий и приложить чек.</div>
            <a class="back" href="/point-reimbursement-page?fio={escape(fio)}&point_code={escape(point_code_clean)}">← Вернуться к возмещению</a>
        </div>
    </div>
</body>
</html>
        """, status_code=400)

    combined_receipts = append_receipt_paths(existing.get("reimb_receipt"), new_paths)
    existing_reimb_amount = int(existing.get("reimb_amount") or 0)
    existing_reimb_comment = existing.get("reimb_comment") or ""
    new_reimb_comment = append_multiline_comment(existing_reimb_comment, reimb_amount_value, reimb_comment_value)

    upsert_point_adjustment(
        db=db,
        merchant_id=merchant["id"],
        point_code=point_code_clean,
        y=period["year"],
        m=period["month"],
        note_amount=int(existing.get("note_amount") or 0),
        note_comment=existing.get("note_comment") or "",
        reimb_amount=existing_reimb_amount + reimb_amount_value,
        reimb_comment=new_reimb_comment,
        reimb_receipt=combined_receipts,
    )

    return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&saved=1", status_code=303)


@app.post("/save-point-adjustment")
async def save_point_adjustment(
    fio: str = Form(...),
    point_code: str = Form(...),
    note_amount: int = Form(0),
    note_comment: str = Form(""),
    reimb_amount: int = Form(0),
    reimb_comment: str = Form(""),
    reimb_receipt: UploadFile | None = File(None),
    db: Session = Depends(get_db)
):
    # Compatibility route for old form versions.
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    require_draft_month(db, merchant["id"], period)
    point_code_clean = normalize_point_code(point_code)
    existing = get_point_adjustment(db, merchant["id"], point_code_clean, period["year"], period["month"]) or {}
    # Старый маршрут оставлен только для совместимости со старыми версиями формы.
    # Важно: он НЕ перетирает уже внесённые примечания/возмещения, а добавляет новые значения.
    receipt_path = existing.get("reimb_receipt")
    new_receipt_paths = []
    if reimb_receipt and reimb_receipt.filename:
        content = await reimb_receipt.read()
        if content:
            new_receipt_paths.append(save_receipt_file_to_db(db, merchant["id"], reimb_receipt.filename, reimb_receipt.content_type, content))
            receipt_path = append_receipt_paths(receipt_path, new_receipt_paths)

    add_note_amount = int(note_amount or 0)
    add_note_comment = (note_comment or "").strip()
    add_reimb_amount = int(reimb_amount or 0)
    add_reimb_comment = (reimb_comment or "").strip()

    existing_note_amount = int(existing.get("note_amount") or 0)
    existing_note_comment = existing.get("note_comment") or ""
    existing_reimb_amount = int(existing.get("reimb_amount") or 0)
    existing_reimb_comment = existing.get("reimb_comment") or ""

    final_note_amount = existing_note_amount
    final_note_comment = existing_note_comment
    if add_note_amount > 0 or add_note_comment:
        if add_note_amount <= 0 or not add_note_comment:
            return RedirectResponse(url=f"/point-note-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&mode=normal", status_code=303)
        final_note_amount = existing_note_amount + add_note_amount
        final_note_comment = append_multiline_comment(existing_note_comment, add_note_amount, add_note_comment)

    final_reimb_amount = existing_reimb_amount
    final_reimb_comment = existing_reimb_comment
    if add_reimb_amount > 0 or add_reimb_comment or new_receipt_paths:
        if add_reimb_amount <= 0 or not add_reimb_comment or not new_receipt_paths:
            return RedirectResponse(url=f"/point-reimbursement-page?fio={escape(fio)}&point_code={escape(point_code_clean)}", status_code=303)
        final_reimb_amount = existing_reimb_amount + add_reimb_amount
        final_reimb_comment = append_multiline_comment(existing_reimb_comment, add_reimb_amount, add_reimb_comment)

    upsert_point_adjustment(
        db=db,
        merchant_id=merchant["id"],
        point_code=point_code_clean,
        y=period["year"],
        m=period["month"],
        note_amount=final_note_amount,
        note_comment=final_note_comment,
        reimb_amount=final_reimb_amount,
        reimb_comment=final_reimb_comment,
        reimb_receipt=receipt_path,
    )
    return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&saved=1", status_code=303)




@app.post("/delete-point-note")
def delete_point_note(
    fio: str = Form(...),
    point_code: str = Form(...),
    item_index: int = Form(...),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)

    point_code_clean = normalize_point_code(point_code)
    overall = compute_overall_total(db, merchant["id"], period["year"], period["month"])
    if overall["submission_status"] == "submitted":
        return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}", status_code=303)

    existing = get_point_adjustment(db, merchant["id"], point_code_clean, period["year"], period["month"]) or {}
    note_lines = split_adjustment_lines(existing.get("note_comment"))

    if 0 <= int(item_index) < len(note_lines):
        note_lines.pop(int(item_index))

    new_note_comment = "\n".join(note_lines)
    new_note_amount = sum_adjustment_lines(note_lines)

    upsert_point_adjustment(
        db=db,
        merchant_id=merchant["id"],
        point_code=point_code_clean,
        y=period["year"],
        m=period["month"],
        note_amount=new_note_amount,
        note_comment=new_note_comment,
        reimb_amount=int(existing.get("reimb_amount") or 0),
        reimb_comment=existing.get("reimb_comment") or "",
        reimb_receipt=existing.get("reimb_receipt"),
    )

    return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&saved=1", status_code=303)


@app.post("/delete-point-reimbursement")
def delete_point_reimbursement(
    fio: str = Form(...),
    point_code: str = Form(...),
    item_index: int = Form(...),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)

    point_code_clean = normalize_point_code(point_code)
    overall = compute_overall_total(db, merchant["id"], period["year"], period["month"])
    if overall["submission_status"] == "submitted":
        return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}", status_code=303)

    existing = get_point_adjustment(db, merchant["id"], point_code_clean, period["year"], period["month"]) or {}
    reimb_lines = split_adjustment_lines(existing.get("reimb_comment"))

    if 0 <= int(item_index) < len(reimb_lines):
        reimb_lines.pop(int(item_index))

    new_reimb_comment = "\n".join(reimb_lines)
    new_reimb_amount = sum_adjustment_lines(reimb_lines)

    upsert_point_adjustment(
        db=db,
        merchant_id=merchant["id"],
        point_code=point_code_clean,
        y=period["year"],
        m=period["month"],
        note_amount=int(existing.get("note_amount") or 0),
        note_comment=existing.get("note_comment") or "",
        reimb_amount=new_reimb_amount,
        reimb_comment=new_reimb_comment,
        reimb_receipt=existing.get("reimb_receipt"),
    )

    return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code_clean)}&saved=1", status_code=303)


@app.get("/monthly-submit-page", response_class=HTMLResponse)
def monthly_submit_page(
    request: Request,
    fio: str,
    submitted: str = "",
    reopened: str = "",
    db: Session = Depends(get_db)
):
    period = get_active_period()
    y = period["year"]
    m = period["month"]

    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    session = read_merchant_session(request.cookies.get(MERCHANT_COOKIE))
    if not session:
        return RedirectResponse(url="/login-page", status_code=303)

    overall = compute_overall_total(db, merchant["id"], y, m)
    monthly_submitted = overall["submission_status"] == "submitted"

    info_box = ""
    if submitted == "1":
        info_box += "<div class='success-box'>Месячная сверка отправлена.</div>"
    if reopened == "1":
        info_box += "<div class='success-box'>Месячная сверка разблокирована для редактирования.</div>"

    points_html = ""
    if overall["per_point_details"]:
        for point_code, d in overall["per_point_details"].items():
            points_html += f"""
            <details class="point-detail">
                <summary>{escape(point_code)} — {d["total"]} ₽</summary>
                <div class="summary-content">
                    <div>С поставкой: {d["cnt_supply"]} × {d["rate_supply"]} ₽ = {d["sum_supply"]} ₽</div>
                    <div>Без поставки: {d["cnt_no_supply"]} × {d["rate_no_supply"]} ₽ = {d["sum_no_supply"]} ₽</div>
                    <div>Полный инвент: {d["cnt_full_inv"]} × {d["rate_inventory"]} ₽ = {d["sum_inventory"]} ₽</div>
                    {f'<div>Кофемашина: {d["coffee_cnt"]} × {d["coffee_rate"]} ₽ = {d["coffee_sum"]} ₽</div>' if d["coffee_enabled"] else ''}
                    <div>Примечание по точке: {d["note_amount"]} ₽ — {escape(d["note_comment"]) if d["note_comment"] else "—"}</div>
                    <div>Возмещение по точке: {d["reimb_amount"]} ₽ — {escape(d["reimb_comment"]) if d["reimb_comment"] else "—"}</div>
                    <div>Чек по возмещению: {render_receipt_links(d["reimb_receipt"], "открыть")}</div>
                </div>
            </details>
            """
    else:
        points_html = "<div class='hint'>В этом месяце пока нет отмеченных точек.</div>"

    action_block = ""
    if monthly_submitted:
        action_block = f"""
        <div class="detail-card" style="margin-top:18px;">
            <div class="detail-title">Статус</div>
            <div class="detail-line">Сверка за месяц отправлена</div>
            <form method="post" action="/reopen-monthly-submission">
                <input type="hidden" name="fio" value="{escape(fio)}" />
                <input type="hidden" name="csrf_token" value="{escape(session["csrf"])}" />
                <button class="btn btn-secondary" type="submit">Редактировать сверку</button>
            </form>
        </div>
        """
    else:
        action_block = f"""
        <form method="post" action="/submit-monthly-submission">
            <input type="hidden" name="fio" value="{escape(fio)}" />
            <input type="hidden" name="csrf_token" value="{escape(session["csrf"])}" />
            <button class="btn" type="submit">Отправить сверку за месяц</button>
        </form>
        """

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Отправка месячной сверки</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card-wide">
            <div class="brand">ВкусВилл</div>
            <h1>Отправить сверку за месяц</h1>
            <div class="subtitle">{escape(fio)} · {month_title(y, m)}</div>

            {info_box}

            <div class="sum-strip">
                <div class="sum-card">
                    <div class="sum-title">Сумма по точкам</div>
                    <div class="sum-value">{sum(overall["per_point"].values())} ₽</div>
                </div>

                <div class="sum-card">
                    <div class="sum-title">Итог за месяц</div>
                    <div class="sum-value">{overall["total"]} ₽</div>
                </div>
            </div>

            <div class="hint">
                Пожалуйста, убедитесь перед отправкой, что данные по всем точкам заполнены корректно.
            </div>

            {points_html}

            {action_block}

            <div class="admin-export-buttons" style="margin-top:18px;">
                <a class="btn btn-secondary btn-inline" href="/point-page?fio={escape(fio)}">Перейти к другой точке</a>
                <a class="btn btn-secondary btn-inline" href="/summary-page?fio={escape(fio)}">Моя сумма</a>
                <a class="btn btn-secondary btn-inline" href="/menu-page?fio={escape(fio)}">Главный экран</a>
            </div>
        </div>
    </div>
</body>
</html>
"""


@app.post("/submit-monthly-submission")
async def submit_monthly_submission_route(
    request: Request,
    fio: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    if not verify_csrf(read_merchant_session(request.cookies.get(MERCHANT_COOKIE)), csrf_token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)

    try:
        submit_monthly_submission(db, merchant["id"], period["year"], period["month"])
    except Exception:
        db.rollback()
        logger.exception(
            "monthly_submission_failed merchant_id=%s year=%s month=%s",
            merchant["id"],
            period["year"],
            period["month"],
        )
        return HTMLResponse(
            "Не удалось отправить сверку. Данные не изменены; попробуйте ещё раз.",
            status_code=503,
        )

    return RedirectResponse(
        url=f"/monthly-submit-page?fio={escape(fio)}&submitted=1",
        status_code=303
    )


@app.get("/reopen-monthly-submission")
def reopen_monthly_submission_route(
    fio: str,
    db: Session = Depends(get_db)
):
    return RedirectResponse(
        url=f"/monthly-submit-page?fio={escape(fio)}",
        status_code=303
    )


@app.post("/reopen-monthly-submission")
def reopen_monthly_submission_post(
    request: Request,
    fio: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    if not verify_csrf(read_merchant_session(request.cookies.get(MERCHANT_COOKIE)), csrf_token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    reopen_monthly_submission(db, merchant["id"], period["year"], period["month"])
    return RedirectResponse(url=f"/monthly-submit-page?fio={escape(fio)}&reopened=1", status_code=303)

@app.get("/day-action-page", response_class=HTMLResponse)
def day_action_page(
    request: Request,
    fio: str,
    point_code: str,
    day: int,
    db: Session = Depends(get_db)
):
    period = get_active_period()
    y = period["year"]
    m = period["month"]

    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)

    overall = compute_overall_total(db, merchant["id"], y, m)
    if overall["submission_status"] == "submitted":
        return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code)}", status_code=303)

    if day < 1 or day > days_in_month(y, m):
        return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code)}", status_code=303)

    visits = get_visits_for_month(db, merchant["id"], point_code, y, m)
    day_visits = visits.get(day, set())

    current_date = date(y, m, day)
    special_inventory = current_date in set(get_special_inventory_days(db))
    calendar_overrides = get_calendar_overrides(db, y, m)
    non_working_day = is_calendar_red_day(current_date, calendar_overrides)
    allowed_slots = allowed_visit_slots(
        current_date, special_inventory=special_inventory
    )
    session = read_merchant_session(request.cookies.get(MERCHANT_COOKIE))
    if not session:
        return RedirectResponse(url="/login-page", status_code=303)
    if allowed_slots == {SLOT_DAY}:
        return RedirectResponse(
            url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code)}",
            status_code=303,
        )

    action_forms = []

    def visit_form(slot: str, button_text: str, *, inventory: bool = False) -> str:
        action = "/toggle-inventory" if inventory else "/toggle-day"
        slot_input = "" if inventory else f'<input type="hidden" name="slot" value="{slot}" />'
        button_class = "btn btn-secondary btn-small" if inventory else "btn btn-small"
        adding = slot not in day_visits
        confirmation = ""
        confirmation_attr = ""
        if non_working_day and adding:
            confirmation = '<input type="hidden" name="confirm_non_working" value="1" />'
            confirmation_attr = (
                f' onsubmit="return window.confirm('
                f"'{escape(NON_WORKING_CONFIRM_MESSAGE)}'"
                f');"'
            )
        return f"""
            <form method="post" action="{action}"{confirmation_attr}>
                <input type="hidden" name="fio" value="{escape(fio)}" />
                <input type="hidden" name="point_code" value="{escape(point_code)}" />
                <input type="hidden" name="day" value="{day}" />
                {slot_input}
                <input type="hidden" name="csrf_token" value="{escape(session["csrf"])}" />
                {confirmation}
                <button class="{button_class}" type="submit">{button_text}</button>
            </form>
        """

    if SLOT_DAY in allowed_slots:
        action_forms.append(
            visit_form(
                SLOT_DAY,
                "Убрать выход" if SLOT_DAY in day_visits else "Добавить выход",
            )
        )
    if SLOT_MORNING in allowed_slots:
        action_forms.append(
            visit_form(
                SLOT_MORNING,
                "Убрать утренний выход"
                if SLOT_MORNING in day_visits
                else "Добавить утренний выход",
            )
        )
    if SLOT_EVENING in allowed_slots:
        action_forms.append(
            visit_form(
                SLOT_EVENING,
                "Убрать вечерний полный инвент"
                if SLOT_EVENING in day_visits
                else "Добавить вечерний полный инвент",
            )
        )
    if SLOT_FULL_INVENT in allowed_slots:
        action_forms.append(
            visit_form(
                SLOT_FULL_INVENT,
                "Убрать полный инвент"
                if SLOT_FULL_INVENT in day_visits
                else "Добавить полный инвент",
                inventory=True,
            )
        )
    if SLOT_DAY in day_visits and SLOT_DAY not in allowed_slots:
        action_forms.append(
            visit_form(SLOT_DAY, "Убрать ранее отмеченный выход")
        )

    action_title = "Выбор действия" if len(action_forms) > 1 else "Отметить выход"
    action_forms_html = "".join(action_forms)

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Действие по дню</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>{action_title}</h1>
            <div class="subtitle">
                Точка: {escape(point_code)}<br>
                Дата: {day:02d}.{m:02d}.{y}
            </div>

            {action_forms_html}

            <a class="back" href="/calendar-page?fio={escape(fio)}&point_code={escape(point_code)}">← Назад к календарю</a>
        </div>
    </div>
</body>
</html>
"""


@app.get("/toggle-day")
def toggle_day(
    fio: str,
    point_code: str,
    day: int,
    db: Session = Depends(get_db)
):
    return RedirectResponse(
        url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code)}",
        status_code=303,
    )


@app.post("/toggle-day")
def toggle_day_post(
    request: Request,
    fio: str = Form(...),
    point_code: str = Form(...),
    day: int = Form(...),
    slot: str = Form(...),
    csrf_token: str = Form(...),
    confirm_non_working: str = Form(""),
    db: Session = Depends(get_db),
):
    session = read_merchant_session(request.cookies.get(MERCHANT_COOKIE))
    if not verify_csrf(session, csrf_token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    session = read_merchant_session(request.cookies.get(MERCHANT_COOKIE))
    if not session:
        return RedirectResponse(url="/login-page", status_code=303)
    if compute_overall_total(db, merchant["id"], period["year"], period["month"])["submission_status"] == "submitted":
        raise HTTPException(status_code=409, detail="Reconciliation is submitted")
    if not 1 <= day <= days_in_month(period["year"], period["month"]):
        db.rollback()
        return HTMLResponse("Некорректная дата выхода.", status_code=400)

    current_date = date(period["year"], period["month"], day)
    try:
        normalized_slot = normalize_visit_slot(slot)
    except ValueError:
        db.rollback()
        return HTMLResponse("Неизвестный тип выхода.", status_code=400)

    allowed_slots = allowed_visit_slots(current_date)
    existing = get_visits_for_month(
        db, merchant["id"], point_code, period["year"], period["month"]
    ).get(day, set())
    removing_legacy_day = (
        normalized_slot == SLOT_DAY
        and SLOT_DAY in existing
        and SLOT_DAY not in allowed_slots
    )
    if normalized_slot not in allowed_slots and not removing_legacy_day:
        db.rollback()
        return HTMLResponse(
            "Для этой даты выбранный тип выхода недоступен. Вернитесь в календарь.",
            status_code=409,
        )
    if (
        normalized_slot not in existing
        and is_calendar_red_day(
            current_date,
            get_calendar_overrides(db, period["year"], period["month"]),
        )
        and confirm_non_working != "1"
    ):
        db.rollback()
        return HTMLResponse(
            NON_WORKING_CONFIRM_MESSAGE,
            status_code=409,
        )

    try:
        toggle_day_visit(db, merchant["id"], point_code, period["year"], period["month"], day, normalized_slot)
    except InventoryWeekLimitError as exc:
        db.rollback()
        query = urlencode(
            {
                "fio": fio,
                "point_code": point_code,
                "visit_error": "inventory_week",
                "inventory_date": exc.existing_date.isoformat(),
            }
        )
        return RedirectResponse(
            url=f"/calendar-page?{query}",
            status_code=303,
        )
    except ValueError:
        db.rollback()
        return HTMLResponse(
            "Для этой даты выбранный тип выхода недоступен. Вернитесь в календарь.",
            status_code=409,
        )
    except Exception as exc:
        db.rollback()
        log_redacted_exception("toggle_day_failed", exc)
        return HTMLResponse(
            "Не удалось сохранить выход из-за временной ошибки. Попробуйте ещё раз.",
            status_code=503,
        )
    return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code)}", status_code=303)


@app.get("/toggle-inventory")
def toggle_inventory(
    fio: str,
    point_code: str,
    day: int,
    db: Session = Depends(get_db)
):
    return RedirectResponse(
        url=f"/day-action-page?fio={escape(fio)}&point_code={escape(point_code)}&day={day}",
        status_code=303,
    )


@app.post("/toggle-inventory")
def toggle_inventory_post(
    request: Request,
    fio: str = Form(...),
    point_code: str = Form(...),
    day: int = Form(...),
    csrf_token: str = Form(...),
    confirm_non_working: str = Form(""),
    db: Session = Depends(get_db),
):
    session = read_merchant_session(request.cookies.get(MERCHANT_COOKIE))
    if not verify_csrf(session, csrf_token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)
    if compute_overall_total(db, merchant["id"], period["year"], period["month"])["submission_status"] == "submitted":
        raise HTTPException(status_code=409, detail="Reconciliation is submitted")
    if not 1 <= day <= days_in_month(period["year"], period["month"]):
        db.rollback()
        return HTMLResponse("Некорректная дата полного инвента.", status_code=400)

    current_date = date(period["year"], period["month"], day)
    special_inventory = current_date in set(get_special_inventory_days(db))
    if SLOT_FULL_INVENT not in allowed_visit_slots(
        current_date, special_inventory=special_inventory
    ):
        db.rollback()
        return HTMLResponse(
            "Полный инвент недоступен для этой даты. Вернитесь в календарь.",
            status_code=409,
        )
    existing = get_visits_for_month(
        db, merchant["id"], point_code, period["year"], period["month"]
    ).get(day, set())
    if (
        SLOT_FULL_INVENT not in existing
        and is_calendar_red_day(
            current_date,
            get_calendar_overrides(db, period["year"], period["month"]),
        )
        and confirm_non_working != "1"
    ):
        db.rollback()
        return HTMLResponse(
            NON_WORKING_CONFIRM_MESSAGE,
            status_code=409,
        )
    try:
        toggle_inventory_visit(
            db,
            merchant["id"],
            point_code,
            period["year"],
            period["month"],
            day,
            special_inventory=special_inventory,
        )
    except InventoryWeekLimitError as exc:
        db.rollback()
        query = urlencode(
            {
                "fio": fio,
                "point_code": point_code,
                "visit_error": "inventory_week",
                "inventory_date": exc.existing_date.isoformat(),
            }
        )
        return RedirectResponse(
            url=f"/calendar-page?{query}",
            status_code=303,
        )
    except ValueError:
        db.rollback()
        return HTMLResponse(
            "Полный инвент недоступен для этой даты. Вернитесь в календарь.",
            status_code=409,
        )
    return RedirectResponse(url=f"/calendar-page?fio={escape(fio)}&point_code={escape(point_code)}", status_code=303)


@app.get("/summary-page", response_class=HTMLResponse)
def summary_page(fio: str = "", db: Session = Depends(get_db)):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    overall = {"total": 0, "per_point": {}, "per_point_details": {}}

    if merchant:
        overall = compute_overall_total(db, merchant["id"], period["year"], period["month"])

    details_html = ""
    if overall["per_point_details"]:
        for point_code, d in overall["per_point_details"].items():
            details_html += f"""
            <details class="point-detail">
                <summary>{escape(point_code)} — {d["total"]} ₽</summary>
                <div class="summary-content">
                    <div>С поставкой: {d["cnt_supply"]} × {d["rate_supply"]} ₽ = {d["sum_supply"]} ₽</div>
                    <div>Без поставки: {d["cnt_no_supply"]} × {d["rate_no_supply"]} ₽ = {d["sum_no_supply"]} ₽</div>
                    <div>Полный инвент: {d["cnt_full_inv"]} × {d["rate_inventory"]} ₽ = {d["sum_inventory"]} ₽</div>
                    {f'<div>Кофемашина: {d["coffee_cnt"]} × {d["coffee_rate"]} ₽ = {d["coffee_sum"]} ₽</div>' if d["coffee_enabled"] else ''}
                    <div><strong>Итого по точке: {d["total"]} ₽</strong></div>
                </div>
            </details>
            """
    else:
        details_html = "<div class='hint' style='margin-top:10px'>Пока нет отмеченных точек за этот месяц.</div>"


    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Моя сумма</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>Моя сумма</h1>
            <div class="subtitle">{escape(fio)}</div>

            <div class="sum-card">
                <div class="sum-title">Общая сумма за месяц</div>
                <div class="sum-value">{overall["total"]} ₽</div>
            </div>

            {details_html}

            <div class="hint">Сейчас открыт период за {month_title(period["year"], period["month"])}.</div>

            <a class="back" href="/menu-page?fio={escape(fio)}">← Назад</a>
        </div>
    </div>
</body>
</html>
"""


@app.get("/admin-login", response_class=HTMLResponse)
def admin_login_page(error: str = ""):
    error_box = ""
    if error == "1":
        error_box = "<div class='error-box'>Неверный логин или пароль.</div>"

    env_box = ""
    if not ADMIN_LOGIN or not ADMIN_PASSWORD:
        env_box = "<div class='error-box'>В Render нужно задать ADMIN_LOGIN и ADMIN_PASSWORD.</div>"

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Админ-вход</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="brand">ВкусВилл</div>
            <h1>Админка</h1>
            <div class="subtitle">Вход в отчёт по сверкам</div>

            {env_box}
            {error_box}

            <form method="post" action="/admin-login">
                <label for="login">Логин</label>
                <input id="login" name="login" type="text" required />

                <label for="password">Пароль</label>
                <input id="password" name="password" type="password" required />

                <button class="btn" type="submit">Войти</button>
            </form>
        </div>
    </div>
</body>
</html>
"""


@app.post("/admin-login")
def admin_login_submit(login: str = Form(...), password: str = Form(...)):
    if not ADMIN_LOGIN or not ADMIN_PASSWORD:
        return RedirectResponse(url="/admin-login?error=1", status_code=303)

    if login != ADMIN_LOGIN or password != ADMIN_PASSWORD:
        return RedirectResponse(url="/admin-login?error=1", status_code=303)

    response = RedirectResponse(url="/admin-report", status_code=303)
    response.set_cookie(
        key="admin_auth",
        value=get_admin_cookie_value(),
        httponly=True,
        samesite="lax",
        secure=secure_cookie(),
        max_age=60 * 60 * 12,
    )
    return response


@app.get("/admin-logout")
def admin_logout():
    response = RedirectResponse(url="/admin-login", status_code=303)
    response.delete_cookie("admin_auth")
    return response


def _merchant_return_query(
    fio_query: str = "",
    last4_query: str = "",
    filter_tu: str = "",
    filter_status: str = "",
    sort: str = "fio_asc",
) -> str:
    values = {
        "fio_query": str(fio_query or ""),
        "last4_query": str(last4_query or ""),
        "tu": str(filter_tu or ""),
        "status": str(filter_status or ""),
        "sort": sort if sort in MERCHANT_SORTS else "fio_asc",
    }
    return urlencode({key: value for key, value in values.items() if value})


def _merchant_redirect_url(message_key: str, message: str, **filters) -> str:
    query = _merchant_return_query(**filters)
    suffix = f"&{query}" if query else ""
    return f"/admin-merchants?{message_key}={urlencode({message_key: message}).split('=', 1)[1]}{suffix}"


def _format_admin_date(value) -> str:
    if not value:
        return "—"
    if hasattr(value, "strftime"):
        return value.strftime("%d.%m.%Y %H:%M")
    return escape(str(value))


def _merchant_hidden_filters(
    fio_query: str,
    last4_query: str,
    filter_tu: str,
    filter_status: str,
    sort: str,
) -> str:
    return f"""
        <input type="hidden" name="fio_query" value="{escape(fio_query)}" />
        <input type="hidden" name="last4_query" value="{escape(last4_query)}" />
        <input type="hidden" name="filter_tu" value="{escape(filter_tu)}" />
        <input type="hidden" name="filter_status" value="{escape(filter_status)}" />
        <input type="hidden" name="sort" value="{escape(sort)}" />
    """


def render_merchant_form_page(
    *,
    admin_auth: str,
    tu_values: list[str],
    values: dict,
    errors: dict[str, str] | None = None,
    message: str = "",
    duplicate_id: int | None = None,
    same_name_id: int | None = None,
    requires_confirmation: bool = False,
    merchant_id: int | None = None,
    audit_rows: list[dict] | None = None,
    fio_query: str = "",
    last4_query: str = "",
    filter_tu: str = "",
    filter_status: str = "",
    sort: str = "fio_asc",
) -> str:
    errors = errors or {}
    editing = merchant_id is not None
    title = "Редактировать мерчендайзера" if editing else "Добавить мерчендайзера"
    action = f"/admin-merchants/{merchant_id}" if editing else "/admin-add-merchant"
    csrf_token = get_admin_csrf_token(admin_auth)
    tu_options = "".join(
        f'<option value="{escape(item)}"></option>' for item in tu_values
    )
    error_box = f"<div class='error-box'>{escape(message)}</div>" if message else ""
    duplicate_box = ""
    if duplicate_id:
        duplicate_box = (
            "<div class='hint'>Новая запись не создана. "
            f"<a href='/admin-merchants/{duplicate_id}/edit'>"
            "Открыть существующего сотрудника</a>.</div>"
        )
    confirm_box = ""
    if requires_confirmation:
        existing_link = (
            f"<a href='/admin-merchants/{same_name_id}/edit'>Открыть сотрудника с таким ФИО</a>."
            if same_name_id
            else ""
        )
        confirm_box = f"""
        <div class="danger-panel">
            <strong>Возможное совпадение ФИО.</strong>
            <div style="margin-top:6px;">{existing_link}</div>
            <label style="display:flex;gap:10px;align-items:flex-start;">
                <input type="checkbox" name="confirm_same_name" value="1" style="width:auto;margin-top:3px;" required />
                Я проверил последние четыре цифры и подтверждаю, что это другой человек.
            </label>
        </div>
        """
    status_field = ""
    if editing:
        active_selected = "selected" if values.get("status", "active") == "active" else ""
        inactive_selected = "selected" if values.get("status") == "inactive" else ""
        status_field = f"""
        <label for="merchant_status">Статус</label>
        <select id="merchant_status" name="status" required>
            <option value="active" {active_selected}>Активен</option>
            <option value="inactive" {inactive_selected}>Деактивирован</option>
        </select>
        <div class="field-error">{escape(errors.get("status", ""))}</div>
        """
    else:
        status_field = """
        <label>Статус</label>
        <input type="text" value="Активен" disabled />
        <input type="hidden" name="status" value="active" />
        """
    hidden_filters = _merchant_hidden_filters(
        fio_query, last4_query, filter_tu, filter_status, sort
    )
    form_confirmation = (
        """ onsubmit="return document.getElementById('merchant_status').value !== 'inactive' || confirm('Деактивировать сотрудника? Он не сможет войти, но история сохранится.');\""""
        if editing
        else ""
    )

    audit_html = ""
    if editing:
        action_names = {
            "created": "Создание",
            "updated": "Изменение",
            "deactivated": "Деактивация",
            "restored": "Восстановление",
        }
        rendered = []
        for row in audit_rows or []:
            try:
                fields = ", ".join(json.loads(row.get("changed_fields") or "[]")) or "—"
            except (TypeError, ValueError):
                fields = "—"
            rendered.append(
                f"""
                <div class="audit-item">
                    <strong>{escape(action_names.get(row.get("action"), str(row.get("action") or "Действие")))}</strong>
                    · {escape(str(row.get("actor") or "admin"))}
                    <div>Поля: {escape(fields)}</div>
                    <div class="subtitle" style="margin:4px 0 0;">{_format_admin_date(row.get("created_at"))}</div>
                </div>
                """
            )
        audit_html = f"""
        <div class="detail-card" style="margin-top:18px;">
            <div class="detail-title">Аудит действий</div>
            <div class="audit-list">{''.join(rendered) if rendered else '<div class="hint">Записей аудита пока нет.</div>'}</div>
        </div>
        """

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{title}</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card" style="max-width:720px;">
            <div class="brand">ВкусВилл</div>
            <h1>{title}</h1>
            <div class="subtitle">Все изменения сохраняют прежний merchant_id и связанную историю.</div>
            {error_box}
            {duplicate_box}
            <form method="post" action="{action}"{form_confirmation}>
                <input type="hidden" name="csrf_token" value="{csrf_token}" />
                {hidden_filters}

                <label for="merchant_fio">ФИО</label>
                <input id="merchant_fio" name="fio" type="text" value="{escape(str(values.get('fio') or ''))}" autocomplete="name" required />
                <div class="field-error">{escape(errors.get("fio", ""))}</div>

                <label for="merchant_last4">Последние 4 цифры телефона</label>
                <input id="merchant_last4" name="last4" type="text" inputmode="numeric" pattern="[0-9]{{4}}" minlength="4" maxlength="4" value="{escape(str(values.get('last4') or ''))}" required />
                <div class="field-error">{escape(errors.get("last4", ""))}</div>

                <label for="merchant_tu">ТУ</label>
                <input id="merchant_tu" name="tu" type="text" list="merchant_tu_values" value="{escape(str(values.get('tu') or ''))}" required />
                <datalist id="merchant_tu_values">{tu_options}</datalist>
                <div class="field-error">{escape(errors.get("tu", ""))}</div>

                {status_field}
                {confirm_box}

                <button class="btn" type="submit">{"Сохранить изменения" if editing else "Добавить мерчендайзера"}</button>
            </form>
            <a class="back" href="/admin-merchants?{_merchant_return_query(fio_query, last4_query, filter_tu, filter_status, sort)}">← Вернуться к списку</a>
            {audit_html}
        </div>
    </div>
</body>
</html>
"""


@app.get("/admin-merchants", response_class=HTMLResponse)
def admin_merchants_page(
    fio_query: str = "",
    last4_query: str = "",
    tu: str = "",
    status: str = "",
    sort: str = "fio_asc",
    success: str = "",
    error: str = "",
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    sort = sort if sort in MERCHANT_SORTS else "fio_asc"
    rows = list_merchants(
        db,
        fio_query=fio_query,
        last4_query=last4_query,
        tu=tu,
        status=status,
        sort=sort,
    )
    tu_values = get_all_tu_values(db)
    csrf_token = get_admin_csrf_token(str(admin_auth))
    filters = {
        "fio_query": fio_query,
        "last4_query": last4_query,
        "filter_tu": tu,
        "filter_status": status,
        "sort": sort,
    }
    hidden_filters = _merchant_hidden_filters(**filters)
    info_box = ""
    if success:
        info_box += f"<div class='success-box'>{escape(success)}</div>"
    if error:
        info_box += f"<div class='error-box'>{escape(error)}</div>"

    table_rows = []
    cards = []
    for row in rows:
        active = bool(row.get("is_active"))
        status_text = "Активен" if active else "Деактивирован"
        status_class = "" if active else " inactive"
        next_active = "0" if active else "1"
        action_text = "Деактивировать" if active else "Восстановить"
        action_class = "btn-danger" if active else "btn-secondary"
        confirm_text = (
            "Деактивировать сотрудника? Он не сможет войти, но история сохранится."
            if active
            else "Восстановить сотрудника и разрешить вход?"
        )
        last4_display = escape(str(row.get("last4") or "Не сохранены"))
        edit_url = (
            f"/admin-merchants/{row['id']}/edit?"
            + _merchant_return_query(fio_query, last4_query, tu, status, sort)
        )
        status_form = f"""
        <form method="post" action="/admin-merchants/{row['id']}/status" onsubmit="return confirm('{confirm_text}');">
            <input type="hidden" name="csrf_token" value="{csrf_token}" />
            <input type="hidden" name="active" value="{next_active}" />
            {hidden_filters}
            <button class="btn {action_class} btn-inline" type="submit">{action_text}</button>
        </form>
        """
        table_rows.append(
            f"""
            <tr>
                <td><strong>{escape(str(row['fio']))}</strong></td>
                <td>{last4_display}</td>
                <td>{escape(str(row.get('tu') or '—'))}</td>
                <td><span class="status-pill{status_class}">{status_text}</span></td>
                <td>{_format_admin_date(row.get('created_at'))}</td>
                <td>{_format_admin_date(row.get('updated_at'))}</td>
                <td>
                    <div style="display:flex;gap:8px;flex-wrap:wrap;">
                        <a class="btn btn-secondary btn-inline" href="{edit_url}">Редактировать</a>
                        {status_form}
                    </div>
                </td>
            </tr>
            """
        )
        cards.append(
            f"""
            <div class="merchant-card">
                <div class="merchant-card-title">{escape(str(row['fio']))}</div>
                <div class="merchant-meta">
                    <div><strong>Последние 4:</strong> {last4_display}</div>
                    <div><strong>ТУ:</strong> {escape(str(row.get('tu') or '—'))}</div>
                    <div><strong>Создан:</strong> {_format_admin_date(row.get('created_at'))}</div>
                    <div><strong>Изменён:</strong> {_format_admin_date(row.get('updated_at'))}</div>
                </div>
                <div style="margin-top:12px;"><span class="status-pill{status_class}">{status_text}</span></div>
                <a class="btn btn-secondary btn-inline" href="{edit_url}">Редактировать</a>
                {status_form}
            </div>
            """
        )
    if not rows:
        table_rows.append(
            "<tr><td colspan='7'>Сотрудники по выбранным условиям не найдены.</td></tr>"
        )
        cards.append(
            "<div class='hint'>Сотрудники по выбранным условиям не найдены.</div>"
        )

    tu_options = "<option value=''>Все ТУ</option>" + "".join(
        f"<option value='{escape(item)}' {'selected' if item == tu else ''}>{escape(item)}</option>"
        for item in tu_values
    )
    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Мерчендайзеры</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card-wide">
            <div class="admin-actions">
                <div>
                    <div class="brand">ВкусВилл</div>
                    <h1>Мерчендайзеры</h1>
                    <div class="subtitle">Ручное управление сотрудниками без потери исторических данных.</div>
                </div>
                <div class="admin-export-buttons">
                    <a class="btn btn-inline" href="/admin-merchants/new?{_merchant_return_query(fio_query, last4_query, tu, status, sort)}">Добавить мерчендайзера</a>
                    <a class="btn btn-secondary btn-inline" href="/admin-data">Управление данными</a>
                    <a class="btn btn-secondary btn-inline" href="/admin-report">Отчёт</a>
                </div>
            </div>
            {info_box}
            <form method="get" action="/admin-merchants" class="merchant-filter-grid">
                <div>
                    <label for="merchant_fio_query">Поиск по ФИО</label>
                    <input id="merchant_fio_query" name="fio_query" type="search" value="{escape(fio_query)}" />
                </div>
                <div>
                    <label for="merchant_last4_query">Последние 4</label>
                    <input id="merchant_last4_query" name="last4_query" type="search" inputmode="numeric" value="{escape(last4_query)}" />
                </div>
                <div>
                    <label for="merchant_tu_filter">ТУ</label>
                    <select id="merchant_tu_filter" name="tu">{tu_options}</select>
                </div>
                <div>
                    <label for="merchant_status_filter">Статус</label>
                    <select id="merchant_status_filter" name="status">
                        <option value="" {'selected' if not status else ''}>Все</option>
                        <option value="active" {'selected' if status == 'active' else ''}>Активные</option>
                        <option value="inactive" {'selected' if status == 'inactive' else ''}>Деактивированные</option>
                    </select>
                </div>
                <div>
                    <label for="merchant_sort">Сортировка</label>
                    <select id="merchant_sort" name="sort">
                        <option value="fio_asc" {'selected' if sort == 'fio_asc' else ''}>ФИО: А—Я</option>
                        <option value="fio_desc" {'selected' if sort == 'fio_desc' else ''}>ФИО: Я—А</option>
                        <option value="updated_desc" {'selected' if sort == 'updated_desc' else ''}>Сначала изменённые</option>
                        <option value="created_desc" {'selected' if sort == 'created_desc' else ''}>Сначала новые</option>
                        <option value="tu_asc" {'selected' if sort == 'tu_asc' else ''}>По ТУ</option>
                    </select>
                </div>
                <button class="btn btn-inline" type="submit">Найти</button>
            </form>

            <div class="table-wrap merchant-table" style="margin-top:18px;">
                <table>
                    <thead><tr>
                        <th>ФИО</th><th>Последние 4</th><th>ТУ</th><th>Статус</th>
                        <th>Создан</th><th>Изменён</th><th>Действия</th>
                    </tr></thead>
                    <tbody>{''.join(table_rows)}</tbody>
                </table>
            </div>
            <div class="merchant-cards" style="margin-top:18px;">{''.join(cards)}</div>
        </div>
    </div>
</body>
</html>
"""


@app.get("/admin-merchants/new", response_class=HTMLResponse)
def admin_new_merchant_page(
    fio_query: str = "",
    last4_query: str = "",
    tu: str = "",
    status: str = "",
    sort: str = "fio_asc",
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    return render_merchant_form_page(
        admin_auth=str(admin_auth),
        tu_values=get_all_tu_values(db),
        values={"fio": "", "last4": "", "tu": "", "status": "active"},
        fio_query=fio_query,
        last4_query=last4_query,
        filter_tu=tu,
        filter_status=status,
        sort=sort,
    )


@app.get("/admin-merchants/{merchant_id}/edit", response_class=HTMLResponse)
def admin_edit_merchant_page(
    merchant_id: int,
    fio_query: str = "",
    last4_query: str = "",
    tu: str = "",
    status: str = "",
    sort: str = "fio_asc",
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    merchant = get_merchant_for_admin(db, merchant_id)
    if not merchant:
        raise HTTPException(status_code=404, detail="Сотрудник не найден")
    merchant["status"] = "active" if merchant.get("is_active") else "inactive"
    return render_merchant_form_page(
        admin_auth=str(admin_auth),
        tu_values=get_all_tu_values(db),
        values=merchant,
        merchant_id=merchant_id,
        audit_rows=list_merchant_audit(db, merchant_id),
        fio_query=fio_query,
        last4_query=last4_query,
        filter_tu=tu,
        filter_status=status,
        sort=sort,
    )


@app.post("/admin-merchants/{merchant_id}")
def admin_update_merchant(
    merchant_id: int,
    fio: str = Form(""),
    last4: str = Form(""),
    tu: str = Form(""),
    status: str = Form("active"),
    csrf_token: str = Form(""),
    confirm_same_name: str = Form(""),
    fio_query: str = Form(""),
    last4_query: str = Form(""),
    filter_tu: str = Form(""),
    filter_status: str = Form(""),
    sort: str = Form("fio_asc"),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")
    values = {"fio": fio, "last4": last4, "tu": tu, "status": status}
    try:
        update_merchant(
            db,
            merchant_id,
            fio,
            last4,
            tu,
            status,
            actor=ADMIN_LOGIN,
            fio_normalizer=fio_norm,
            last4_hasher=hash_last4,
            confirm_same_name=confirm_same_name == "1",
        )
        db.commit()
        return RedirectResponse(
            url=_merchant_redirect_url(
                "success",
                "Изменения сотрудника сохранены.",
                fio_query=fio_query,
                last4_query=last4_query,
                filter_tu=filter_tu,
                filter_status=filter_status,
                sort=sort,
            ),
            status_code=303,
        )
    except MerchantInputError as exc:
        db.rollback()
        return HTMLResponse(
            render_merchant_form_page(
                admin_auth=str(admin_auth),
                tu_values=get_all_tu_values(db),
                values=values,
                errors=exc.field_errors,
                message=exc.message,
                duplicate_id=exc.duplicate_id,
                same_name_id=exc.same_name_id,
                requires_confirmation=exc.requires_confirmation,
                merchant_id=merchant_id,
                audit_rows=list_merchant_audit(db, merchant_id),
                fio_query=fio_query,
                last4_query=last4_query,
                filter_tu=filter_tu,
                filter_status=filter_status,
                sort=sort,
            ),
            status_code=422,
        )
    except Exception:
        db.rollback()
        return HTMLResponse(
            render_merchant_form_page(
                admin_auth=str(admin_auth),
                tu_values=get_all_tu_values(db),
                values=values,
                message="Не удалось сохранить изменения. Повторите попытку.",
                merchant_id=merchant_id,
                audit_rows=list_merchant_audit(db, merchant_id),
                fio_query=fio_query,
                last4_query=last4_query,
                filter_tu=filter_tu,
                filter_status=filter_status,
                sort=sort,
            ),
            status_code=500,
        )


@app.post("/admin-merchants/{merchant_id}/status")
def admin_set_merchant_status(
    merchant_id: int,
    active: str = Form(...),
    csrf_token: str = Form(""),
    fio_query: str = Form(""),
    last4_query: str = Form(""),
    filter_tu: str = Form(""),
    filter_status: str = Form(""),
    sort: str = Form("fio_asc"),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")
    if active not in {"0", "1"}:
        raise HTTPException(status_code=422, detail="Некорректный статус")
    try:
        set_merchant_active(
            db, merchant_id, active == "1", actor=ADMIN_LOGIN
        )
        db.commit()
        message = "Сотрудник восстановлен." if active == "1" else "Сотрудник деактивирован."
        return RedirectResponse(
            url=_merchant_redirect_url(
                "success",
                message,
                fio_query=fio_query,
                last4_query=last4_query,
                filter_tu=filter_tu,
                filter_status=filter_status,
                sort=sort,
            ),
            status_code=303,
        )
    except MerchantInputError as exc:
        db.rollback()
        return RedirectResponse(
            url=_merchant_redirect_url(
                "error",
                exc.message,
                fio_query=fio_query,
                last4_query=last4_query,
                filter_tu=filter_tu,
                filter_status=filter_status,
                sort=sort,
            ),
            status_code=303,
        )


@app.get("/admin-report", response_class=HTMLResponse)
def admin_report(
    year: int | None = None,
    month: int | None = None,
    tu: str = "",
    status: str = "",
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)

    period = get_active_period()
    year = year or period["year"]
    month = month or period["month"]

    tu_filter = tu.strip() or None
    status_filter = status.strip() or None

    rows = get_admin_report_rows(db, year, month, tu_filter, status_filter)
    tu_values = get_all_tu_values(db)

    tu_options = "<option value=''>Все ТУ</option>"
    for item in tu_values:
        selected = "selected" if item == tu else ""
        tu_options += f"<option value='{escape(item)}' {selected}>{escape(item)}</option>"

    status_options = f"""
        <option value='' {'selected' if not status else ''}>Все статусы</option>
        <option value='не отправлено' {'selected' if status == 'не отправлено' else ''}>Не отправлено</option>
        <option value='draft' {'selected' if status == 'draft' else ''}>Черновик</option>
        <option value='submitted' {'selected' if status == 'submitted' else ''}>Отправлено</option>
    """

    rows_html = ""
    if rows:
        for r in rows:
            receipt_html = "—"
            if r["receipt_path"]:
                receipt_html = f"<a href='/{r['receipt_path']}' target='_blank'>Открыть</a>"

            rows_html += f"""
            <tr>
                <td>{escape(r["fio"])}</td>
                <td>{escape(r["tu"]) if r["tu"] else "—"}</td>
                <td>{escape(r["point_code"])}</td>
                <td>{month_title(year, month)}</td>
                <td>{r["cnt_supply"]} / {r["sum_supply"]} ₽</td>
                <td>{r["cnt_no_supply"]} / {r["sum_no_supply"]} ₽</td>
                <td><strong>{r.get("cnt_total_exits", r["cnt_supply"] + r["cnt_no_supply"])}</strong></td>
                <td>{r["cnt_full_inv"]} / {r["sum_inventory"]} ₽</td>
                <td>{r["coffee_cnt"]} × {r["coffee_rate"]} = {r["coffee_sum"]} ₽</td>
                <td>{r["note_amount"]} ₽<br>{escape(r["note_comment"]) if r["note_comment"] else "—"}</td>
                <td>{r["reimb_amount"]} ₽<br>{escape(r["reimb_comment"]) if r["reimb_comment"] else "—"}</td>
                <td><strong>{r["point_total"]} ₽</strong></td>
                <td>{escape(r["status"])}</td>
                <td>{render_receipt_links(r["reimb_receipt"], "Открыть")}</td>
                <td>{"<span style='color:#B91C1C;font-weight:900;'>Да</span>" if r.get("has_overlap") else "—"}</td>
                <td>{escape(r["comment"]) if r["comment"] else "—"}</td>
            </tr>
            """
    else:
        rows_html = """
        <tr>
            <td colspan="16">По выбранным фильтрам данных нет.</td>
        </tr>
        """

    month_options = ""
    for m in range(1, 13):
        selected = "selected" if m == month else ""
        month_options += f"<option value='{m}' {selected}>{m:02d}</option>"

    export_query = f"year={year}&month={month}&tu={escape(tu)}&status={escape(status)}"

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Админ-отчёт</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card-wide">
            <div class="admin-actions">
                <div>
                    <div class="brand">ВкусВилл</div>
                    <h1>Отчёт по сверкам</h1>
                    <div class="subtitle">Админ-панель</div>
                </div>
                <div class="admin-export-buttons">
                    <a class="btn btn-inline" href="/admin-merchants">Мерчендайзеры</a>
                    <a class="btn btn-secondary btn-inline" href="/admin-data">Управление данными</a>
                    <a class="btn btn-secondary btn-inline" href="/admin-logout">Выйти</a>
                </div>
            </div>

            <form method="get" action="/admin-report">
                <div class="filter-grid">
                    <div>
                        <label for="year">Год</label>
                        <input id="year" name="year" type="number" value="{year}" />
                    </div>

                    <div>
                        <label for="month">Месяц</label>
                        <select id="month" name="month">
                            {month_options}
                        </select>
                    </div>

                    <div>
                        <label for="tu">Территориальный управляющий</label>
                        <select id="tu" name="tu">
                            {tu_options}
                        </select>
                    </div>

                    <div>
                        <label for="status">Статус сверки</label>
                        <select id="status" name="status">
                            {status_options}
                        </select>
                    </div>
                </div>

                <button class="btn btn-inline" type="submit">Применить фильтр</button>
            </form>

            <div class="admin-export-buttons">
                <a class="btn btn-secondary btn-inline" href="/admin-export-check?{export_query}">Выгрузка для проверки</a>
                <a class="btn btn-secondary btn-inline" href="/admin-export-payroll?{export_query}">Выгрузка в ведомость</a>
                <a class="btn btn-secondary btn-inline" href="/admin-export-overlaps?{export_query}">Выгрузка пересечений</a>
            </div>

            <div class="hint">
                Период отчёта: {month_title(year, month)}. Всего строк: {len(rows)}.
            </div>

            <div class="table-wrap" style="margin-top:16px;">
                <table>
                    <thead>
                        <tr>
                            <th>ФИО</th>
                            <th>ТУ</th>
                            <th>Точка</th>
                            <th>Месяц</th>
                            <th>С поставкой</th>
                            <th>Без поставки</th>
                            <th>Выходов всего</th>
                            <th>Инвенты</th>
                            <th>Кофемашина</th>
                            <th>Примечание по точке</th>
                            <th>Возмещение по точке</th>
                            <th>Итог по точке</th>
                            <th>Статус</th>
                            <th>Чек по точке</th>
                            <th>Пересечение</th>
                            <th>Комментарий месяца</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</body>
</html>
"""


@app.get("/admin-data", response_class=HTMLResponse)
def admin_data_page(
    success: str = "",
    error: str = "",
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)

    period = get_active_period()
    admin_csrf = get_admin_csrf_token(str(admin_auth))

    info_box = ""
    if success:
        info_box += f"<div class='success-box'>{escape(success)}</div>"
    if error:
        info_box += f"<div class='error-box'>{escape(error)}</div>"

    try:
        merchant_delete_counts = count_merchant_owned_rows(db)
        merchant_delete_count_html = "".join(
            f"<li>{escape(label)}: <strong>{merchant_delete_counts[key]}</strong></li>"
            for key, label in (
                ("merchants", "Мерчендайзеры"),
                ("visits", "Выходы"),
                ("monthly_submissions", "Месячные сверки"),
                ("point_notes", "Примечания"),
                ("point_reimbursements", "Возмещения"),
                ("reimbursement_receipts", "Связи чеков возмещений"),
                ("receipt_files", "Файлы чеков"),
                ("point_adjustments", "Корректировки"),
                ("merchant_audit_log", "Audit-записи"),
            )
        )
        merchant_delete_preview = (
            "<div class='hint' style='margin-top:14px;'>"
            "Перед удалением будут обработаны:</div>"
            f"<ul>{merchant_delete_count_html}</ul>"
        )
    except Exception as exc:
        log_redacted_exception("merchant_delete_preview_failed", exc)
        merchant_delete_preview = (
            "<div class='error-box' style='margin-top:14px;'>"
            "Не удалось безопасно посчитать связанные записи. "
            "Удаление заблокировано.</div>"
        )

    special_inventory_days = get_special_inventory_days(db)
    if special_inventory_days:
        rows = []
        for inv_day in special_inventory_days:
            rows.append(f"""<form method='post' action='/admin-delete-special-inventory-day' style='margin-top:10px; display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>
                <input type='hidden' name='inv_date' value='{inv_day.isoformat()}' />
                <input type='hidden' name='csrf_token' value='{admin_csrf}' />
                <div class='mini-pill'>{inv_day.strftime('%d.%m.%Y')}</div>
                <button class='btn btn-danger btn-inline' type='submit'>Удалить</button>
            </form>""")
        special_inventory_html = ''.join(rows)
    else:
        special_inventory_html = "<div class='hint' style='margin-top:14px;'>Специальные даты пока не добавлены.</div>"

    calendar_status = get_calendar_status(db)
    if calendar_status:
        calendar_status_html = "".join(
            f"<div class='hint'><strong>{row['year']}</strong>: "
            f"{row['row_count']} дат, последнее обновление "
            f"{escape(str(row['last_success_at'] or '—'))}; "
            f"{escape(str(row['message'] or ''))}</div>"
            for row in calendar_status
        )
    else:
        calendar_status_html = (
            "<div class='error-box'>Официальный календарь ещё не синхронизирован. "
            "До синхронизации используется безопасное правило субботы/воскресенья.</div>"
        )

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Управление данными</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card-wide">
            <div class="admin-actions">
                <div>
                    <div class="brand">ВкусВилл</div>
                    <h1>Управление данными</h1>
                    <div class="subtitle">Загрузка файлов и очистка месяца</div>
                </div>
                <div class="admin-export-buttons">
                    <a class="btn btn-inline" href="/admin-merchants">Мерчендайзеры</a>
                    <a class="btn btn-secondary btn-inline" href="/admin-report">Назад к отчёту</a>
                    <a class="btn btn-secondary btn-inline" href="/admin-logout">Выйти</a>
                </div>
            </div>

            {info_box}

            <div class="data-grid">
                <div class="detail-card">
                    <div class="detail-title">Загрузка поставок</div>
                    <form method="post" action="/admin-upload-supplies" enctype="multipart/form-data">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="supplies_file">Файл поставок</label>
                        <input id="supplies_file" name="file" type="file" accept=".xlsx" required />
                        <button class="btn" type="submit">Загрузить поставки</button>
                    </form>
                </div>

                <div class="detail-card">
                    <div class="detail-title">Загрузка ставок</div>
                    <form method="post" action="/admin-upload-rates" enctype="multipart/form-data">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="rates_year">Год</label>
                        <input id="rates_year" name="year" type="number" value="{period["year"]}" required />

                        <label for="rates_month">Месяц</label>
                        <input id="rates_month" name="month" type="number" value="{period["month"]}" min="1" max="12" required />

                        <label for="rates_file">Файл ставок</label>
                        <input id="rates_file" name="file" type="file" accept=".xlsx" required />

                        <button class="btn" type="submit">Загрузить ставки</button>
                    </form>
                </div>

                <div class="detail-card">
                    <div class="detail-title">Загрузка мерчей</div>
                    <form method="post" action="/admin-upload-merchants" enctype="multipart/form-data">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="merchants_tu">Территориальный управляющий</label>
                        <input id="merchants_tu" name="tu" type="text" placeholder="Например: Хрупов" required />

                        <label for="merchants_file">Файл мерчей</label>
                        <input id="merchants_file" name="file" type="file" accept=".xlsx" required />

                        <button class="btn" type="submit">Загрузить мерчей</button>
                    </form>
                    {merchant_delete_preview}
                    <form method="post" action="/admin-delete-all-merchants" style="margin-top:18px;" onsubmit="return confirm('Будут безвозвратно удалены все мерчендайзеры и все связанные с ними сверки, выходы, примечания, возмещения и чеки. Продолжить?');">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="delete_all_merchants_confirmation">Для подтверждения введите: <strong>{DELETE_ALL_CONFIRMATION}</strong></label>
                        <input id="delete_all_merchants_confirmation" name="confirmation" type="text" autocomplete="off" required />
                        <button class="btn btn-danger" type="submit">Удалить всех мерчендайзеров и их данные</button>
                    </form>
                </div>

                <div class="detail-card">
                    <div class="detail-title">Управление мерчендайзерами</div>
                    <div class="hint">Добавление, редактирование, поиск, деактивация и восстановление доступны в отдельном разделе. Изменения не создают новый merchant_id и не отвязывают историю.</div>
                    <a class="btn" href="/admin-merchants">Открыть раздел «Мерчендайзеры»</a>
                </div>

                <div class="detail-card">
                    <div class="detail-title">Очистка месяца</div>
                    <form method="post" action="/admin-clear-month" onsubmit="return confirm('Безвозвратно очистить данные только выбранного месяца?');">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="clear_year">Год</label>
                        <input id="clear_year" name="year" type="number" value="{period["year"]}" required />

                        <label for="clear_month">Месяц</label>
                        <input id="clear_month" name="month" type="number" value="{period["month"]}" min="1" max="12" required />

                        <button class="btn btn-danger" type="submit">Очистить данные месяца</button>
                    </form>
                </div>

                <div class="detail-card">
                    <div class="detail-title">Специальные даты для инвента</div>
                    <form method="post" action="/admin-add-special-inventory-day">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="special_inventory_date">Дата</label>
                        <input id="special_inventory_date" name="inv_date" type="date" required />
                        <button class="btn" type="submit">Добавить дату</button>
                    </form>

                    <div class="hint" style="margin-top:14px;">Инвент будет доступен в пятницу, субботу и в датах из списка ниже.</div>

                    {special_inventory_html}
                </div>
                <div class="detail-card">
                    <div class="detail-title">Производственный календарь РФ</div>
                    {calendar_status_html}
                    <form method="post" action="/admin-sync-production-calendar" style="margin-top:14px;">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="calendar_sync_year">Утверждённый год (пусто — текущий и следующий)</label>
                        <input id="calendar_sync_year" name="year" type="number" min="2025" placeholder="Например: 2026" />
                        <button class="btn" type="submit">Обновить производственный календарь</button>
                    </form>
                    <div class="hint">Обновление запускается в фоне из проверенного официального набора и не блокирует страницу.</div>

                    <form method="post" action="/admin-calendar-override" style="margin-top:18px;">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="calendar_override_date">Ручная корректировка даты</label>
                        <input id="calendar_override_date" name="calendar_date_value" type="date" required />
                        <label for="calendar_override_kind">Статус дня</label>
                        <select id="calendar_override_kind" name="is_day_off" required>
                            <option value="1">Нерабочий</option>
                            <option value="0">Рабочий</option>
                        </select>
                        <label for="calendar_override_title">Название</label>
                        <input id="calendar_override_title" name="title" type="text" />
                        <label for="calendar_override_comment">Обязательный комментарий</label>
                        <input id="calendar_override_comment" name="comment" type="text" required />
                        <button class="btn btn-secondary" type="submit">Сохранить ручную корректировку</button>
                    </form>
                    <form method="post" action="/admin-calendar-reset" style="margin-top:14px;" onsubmit="return confirm('Вернуть официальное значение этой даты?');">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="calendar_reset_date">Вернуть к официальному значению</label>
                        <input id="calendar_reset_date" name="calendar_date_value" type="date" required />
                        <button class="btn btn-secondary" type="submit">Вернуть официальное значение</button>
                    </form>

                    <form method="post" action="/admin-upload-production-calendar" enctype="multipart/form-data">
                        <input type="hidden" name="csrf_token" value="{admin_csrf}" />
                        <label for="calendar_file">XLSX: дата, выходной день, название, источник, комментарий</label>
                        <input id="calendar_file" name="file" type="file" accept=".xlsx" required />
                        <button class="btn btn-secondary" type="submit">Аварийный XLSX-импорт</button>
                    </form>
                    <div class="hint">Ручная корректировка имеет приоритет и не затирается следующей официальной синхронизацией.</div>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""


@app.post("/admin-upload-supplies")
async def admin_upload_supplies(
    file: UploadFile = File(...),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")

    try:
        result = import_supplies_xlsx(db, file.file)
        msg = f"Поставки загружены: строк {result['loaded_rows']}, точек {result['loaded_points']}."
        return RedirectResponse(url=f"/admin-data?success={msg}", status_code=303)
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/admin-data?error={str(e)}", status_code=303)


@app.post("/admin-sync-production-calendar")
def admin_sync_production_calendar(
    year: int | None = Form(None),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")
    years = [year] if year is not None else None
    threading.Thread(
        target=_sync_calendar_background,
        kwargs={"years": years},
        name="production-calendar-admin-sync",
        daemon=True,
    ).start()
    return RedirectResponse(
        url="/admin-data?success=Обновление производственного календаря запущено в фоне.",
        status_code=303,
    )


@app.post("/admin-calendar-override")
def admin_calendar_override(
    calendar_date_value: str = Form(...),
    is_day_off: str = Form(...),
    title: str = Form(""),
    comment: str = Form(...),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")
    try:
        parsed = date.fromisoformat(calendar_date_value)
        if is_day_off not in {"0", "1"}:
            raise ValueError("Некорректный статус дня")
        set_manual_calendar_override(
            db,
            parsed,
            is_day_off == "1",
            title,
            comment,
            actor=ADMIN_LOGIN,
        )
        return RedirectResponse(
            url="/admin-data?success=Ручная корректировка календаря сохранена.",
            status_code=303,
        )
    except ValueError as exc:
        db.rollback()
        message = urlencode({"error": str(exc)}).split("=", 1)[1]
        return RedirectResponse(url=f"/admin-data?error={message}", status_code=303)


@app.post("/admin-calendar-reset")
def admin_calendar_reset(
    calendar_date_value: str = Form(...),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")
    try:
        reset_manual_calendar_override(
            db,
            date.fromisoformat(calendar_date_value),
            actor=ADMIN_LOGIN,
        )
        return RedirectResponse(
            url="/admin-data?success=Восстановлено официальное значение календаря.",
            status_code=303,
        )
    except ValueError as exc:
        db.rollback()
        message = urlencode({"error": str(exc)}).split("=", 1)[1]
        return RedirectResponse(url=f"/admin-data?error={message}", status_code=303)


@app.post("/admin-upload-production-calendar")
async def admin_upload_production_calendar(
    file: UploadFile = File(...),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")
    try:
        result = import_calendar_xlsx(db, file.file)
        msg = f"Производственный календарь загружен: строк {result['loaded_rows']}."
        return RedirectResponse(url=f"/admin-data?success={msg}", status_code=303)
    except Exception as exc:
        db.rollback()
        return RedirectResponse(url=f"/admin-data?error={str(exc)}", status_code=303)


@app.post("/admin-upload-rates")
async def admin_upload_rates(
    year: int = Form(...),
    month: int = Form(...),
    file: UploadFile = File(...),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")

    try:
        result = import_rates_xlsx(db, file.file, year, month)
        msg = f"Ставки загружены: строк {result['loaded_rows']}."
        return RedirectResponse(url=f"/admin-data?success={msg}", status_code=303)
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/admin-data?error={str(e)}", status_code=303)


@app.post("/admin-upload-merchants")
async def admin_upload_merchants(
    tu: str = Form(...),
    file: UploadFile = File(...),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")

    try:
        result = import_merchants_xlsx(db, file.file, tu, actor=ADMIN_LOGIN)
        msg = (
            f"Мерчендайзеры загружены: строк {result['loaded_rows']}, "
            f"создано {result['created']}, повторно активировано {result['reactivated']}."
        )
        return RedirectResponse(url=f"/admin-data?success={msg}", status_code=303)
    except ValueError as exc:
        db.rollback()
        return RedirectResponse(
            url=f"/admin-data?error={urlencode({'error': str(exc)}).split('=', 1)[1]}",
            status_code=303,
        )
    except Exception:
        db.rollback()
        return RedirectResponse(
            url="/admin-data?error=Не удалось загрузить файл мерчендайзеров.",
            status_code=303,
        )


@app.post("/admin-add-merchant")
def admin_add_merchant(
    fio: str = Form(""),
    last4: str = Form(""),
    tu: str = Form(""),
    csrf_token: str = Form(""),
    confirm_same_name: str = Form(""),
    fio_query: str = Form(""),
    last4_query: str = Form(""),
    filter_tu: str = Form(""),
    filter_status: str = Form(""),
    sort: str = Form("fio_asc"),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")
    values = {"fio": fio, "last4": last4, "tu": tu, "status": "active"}
    try:
        created = create_merchant(
            db,
            fio,
            last4,
            tu,
            actor=ADMIN_LOGIN,
            fio_normalizer=fio_norm,
            last4_hasher=hash_last4,
            confirm_same_name=confirm_same_name == "1",
        )
        db.commit()
        return RedirectResponse(
            url=_merchant_redirect_url(
                "success",
                f"Мерчендайзер {created['fio']} добавлен.",
                fio_query=fio_query,
                last4_query=last4_query,
                filter_tu=filter_tu,
                filter_status=filter_status,
                sort=sort,
            ),
            status_code=303,
        )
    except MerchantInputError as exc:
        db.rollback()
        return HTMLResponse(
            render_merchant_form_page(
                admin_auth=str(admin_auth),
                tu_values=get_all_tu_values(db),
                values=values,
                errors=exc.field_errors,
                message=exc.message,
                duplicate_id=exc.duplicate_id,
                same_name_id=exc.same_name_id,
                requires_confirmation=exc.requires_confirmation,
                fio_query=fio_query,
                last4_query=last4_query,
                filter_tu=filter_tu,
                filter_status=filter_status,
                sort=sort,
            ),
            status_code=422,
        )
    except Exception as exc:
        db.rollback()
        log_redacted_exception("admin_merchant_create_failed", exc)
        return HTMLResponse(
            render_merchant_form_page(
                admin_auth=str(admin_auth),
                tu_values=safe_admin_tu_values(db),
                values=values,
                message="Не удалось добавить сотрудника. Данные не сохранены; повторите попытку.",
                fio_query=fio_query,
                last4_query=last4_query,
                filter_tu=filter_tu,
                filter_status=filter_status,
                sort=sort,
            ),
            status_code=503,
        )


@app.post("/admin-clear-month")
def admin_clear_month(
    year: int = Form(...),
    month: int = Form(...),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")

    try:
        result = clear_month_data(db, year, month)
        msg = (
            f"Месяц очищен. Визиты: {result['deleted_visits']}, "
            f"поставки: {result['deleted_supplies']}, ставки: {result['deleted_rates']}, "
            f"месячные сверки: {result['deleted_monthly']}, корректировки по точкам: {result.get('deleted_point_adjustments', 0)}."
        )
        return RedirectResponse(url=f"/admin-data?success={msg}", status_code=303)
    except Exception as exc:
        db.rollback()
        log_redacted_exception("admin_clear_month_failed", exc)
        return RedirectResponse(
            url="/admin-data?error=Не удалось очистить месяц. Изменения отменены.",
            status_code=303,
        )


@app.post("/admin-clear-merchants")
def admin_clear_merchants(
    tu: str = Form(""),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")

    try:
        if tu.strip():
            deactivated = clear_merchants_by_tu(db, tu, actor=ADMIN_LOGIN)
        else:
            deactivated = clear_all_merchants(db, actor=ADMIN_LOGIN)
        msg = (
            f"Деактивировано мерчендайзеров: {deactivated}. "
            "Можно загружать новый список"
        )
        return RedirectResponse(url=f"/admin-data?success={msg}", status_code=303)
    except Exception as exc:
        db.rollback()
        log_redacted_exception("admin_clear_merchants_failed", exc)
        return RedirectResponse(
            url="/admin-data?error=Не удалось деактивировать мерчендайзеров. Изменения отменены.",
            status_code=303,
        )


@app.post("/admin-delete-all-merchants")
def admin_delete_all_merchants(
    confirmation: str = Form(""),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db),
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")
    if confirmation != DELETE_ALL_CONFIRMATION:
        return RedirectResponse(
            url="/admin-data?error="
            + urlencode(
                {
                    "error": (
                        "Удаление не выполнено: введите фразу подтверждения "
                        "точно так, как она указана."
                    )
                }
            ).split("=", 1)[1],
            status_code=303,
        )

    try:
        deleted = delete_all_merchants_and_data(db)
        db.commit()
        summary = ", ".join(
            f"{key}: {value}" for key, value in deleted.items()
        )
        message = (
            "Все мерчендайзеры и связанные с ними данные удалены. "
            f"Удалено строк — {summary}."
        )
        return RedirectResponse(
            url="/admin-data?success="
            + urlencode({"success": message}).split("=", 1)[1],
            status_code=303,
        )
    except Exception as exc:
        db.rollback()
        log_redacted_exception("admin_delete_all_merchants_failed", exc)
        return RedirectResponse(
            url="/admin-data?error="
            + urlencode(
                {
                    "error": (
                        "Не удалось удалить мерчендайзеров. "
                        "Все изменения отменены."
                    )
                }
            ).split("=", 1)[1],
            status_code=303,
        )


@app.post("/admin-add-special-inventory-day")
def admin_add_special_inventory_day(
    inv_date: str = Form(...),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")

    try:
        parsed_date = datetime.strptime(inv_date, "%Y-%m-%d").date()
        add_special_inventory_day(db, parsed_date)
        msg = f"Добавлена специальная дата для инвента: {parsed_date.strftime('%d.%m.%Y')}."
        return RedirectResponse(url=f"/admin-data?success={msg}", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/admin-data?error={str(e)}", status_code=303)


@app.post("/admin-delete-special-inventory-day")
def admin_delete_special_inventory_day(
    inv_date: str = Form(...),
    csrf_token: str = Form(""),
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)
    if not verify_admin_csrf(admin_auth, csrf_token):
        raise HTTPException(status_code=403, detail="Недействительный CSRF-токен")

    try:
        parsed_date = datetime.strptime(inv_date, "%Y-%m-%d").date()
        delete_special_inventory_day(db, parsed_date)
        msg = f"Удалена специальная дата для инвента: {parsed_date.strftime('%d.%m.%Y')}."
        return RedirectResponse(url=f"/admin-data?success={msg}", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/admin-data?error={str(e)}", status_code=303)


@app.get("/admin-export-check")
def admin_export_check(
    year: int,
    month: int,
    tu: str = "",
    status: str = "",
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)

    rows = get_admin_report_rows(
        db=db,
        y=year,
        m=month,
        tu=tu.strip() or None,
        status=status.strip() or None
    )

    wb = Workbook()
    ws = wb.active
    ws.title = "Проверка"

    ws.append([
        "ФИО",
        "ТУ",
        "Точка",
        "Месяц",
        "Выходы с поставкой (кол-во)",
        "Выходы с поставкой (сумма)",
        "Выходы без поставки (кол-во)",
        "Выходы без поставки (сумма)",
        "Выходов всего",
        "Полные инвенты (кол-во)",
        "Полные инвенты (сумма)",
        "Кофемашина (кол-во)",
        "Кофемашина (сумма)",
        "Примечание по точке (сумма)",
        "Примечание по точке (комментарий)",
        "Возмещение по точке (сумма)",
        "Возмещение по точке (комментарий)",
        "Чек по точке",
        "Пересечение",
        "Статус",
        "Комментарий месяца",
        "Итог по точке"
    ])

    for r in rows:
        ws.append([
            r["fio"],
            r["tu"],
            r["point_code"],
            month_title(year, month),
            r["cnt_supply"],
            r["sum_supply"],
            r["cnt_no_supply"],
            r["sum_no_supply"],
            r.get("cnt_total_exits", r["cnt_supply"] + r["cnt_no_supply"]),
            r["cnt_full_inv"],
            r["sum_inventory"],
            r["coffee_cnt"],
            r["coffee_sum"],
            r["note_amount"],
            r["note_comment"],
            r["reimb_amount"],
            r["reimb_comment"],
            r["reimb_receipt"] or "",
            "Да" if r.get("has_overlap") else "",
            r["status"],
            r["comment"],
            r["point_total"]
        ])

    style_sheet(ws)
    return build_excel_response(wb, f"proverka_{year}_{month:02d}.xlsx")


@app.get("/admin-export-payroll")
def admin_export_payroll(
    year: int,
    month: int,
    tu: str = "",
    status: str = "",
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)

    rows = get_admin_payroll_rows(
        db=db,
        y=year,
        m=month,
        tu=tu.strip() or None,
        status=status.strip() or None
    )

    wb = Workbook()
    ws = wb.active
    ws.title = "Ведомость"

    ws.append([
        "ФИО",
        "ТУ",
        "Сумма по мерчу",
        "Сумма в ведомость (/0.87, округление вверх)",
        "Статус"
    ])

    for r in rows:
        ws.append([
            r["fio"],
            r["tu"],
            r["clean_total"],
            r["payroll_total"],
            r["status"]
        ])

    style_sheet(ws)
    return build_excel_response(wb, f"vedomost_{year}_{month:02d}.xlsx")


@app.get("/admin-export-overlaps")
def admin_export_overlaps(
    year: int,
    month: int,
    tu: str = "",
    admin_auth: Optional[str] = Cookie(default=None),
    db: Session = Depends(get_db)
):
    if not is_admin_authenticated(admin_auth):
        return RedirectResponse(url="/admin-login", status_code=303)

    rows = get_intersections_rows(
        db=db,
        y=year,
        m=month,
        tu=tu.strip() or None
    )

    wb = Workbook()
    ws = wb.active
    ws.title = "Пересечения"

    ws.append([
        "Дата",
        "Точка",
        "Мерч 1",
        "ТУ 1",
        "Слот 1",
        "Мерч 2",
        "ТУ 2",
        "Слот 2"
    ])

    for r in rows:
        ws.append([
            r["visit_date"],
            r["point_code"],
            r["fio1"],
            r["tu1"],
            r["slot1"],
            r["fio2"],
            r["tu2"],
            r["slot2"],
        ])

    style_sheet(ws)
    return build_excel_response(wb, f"peresecheniya_{year}_{month:02d}.xlsx")
