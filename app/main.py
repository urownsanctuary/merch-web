import hmac
import os

from fastapi import FastAPI, Depends, HTTPException, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import SessionLocal, engine
from app.admin import router as admin_router
from app.records import router as records_router
from app.migrations import migrate
from app.security import (
    ADMIN_COOKIE,
    SESSION_COOKIE,
    cookie_secure,
    make_session,
    read_session,
    require_admin,
    require_merchant,
    safe,
    url,
    verify_csrf,
)
from app.services import (
    get_active_period,
    login_user,
    get_merchants_columns,
    normalize_point_code,
    point_has_any_supply_in_month,
    get_supply_boxes_map,
    get_visits_for_month,
    get_merchant_by_fio,
    get_merchant_by_id,
    toggle_day_visit,
    toggle_inventory_visit,
    compute_point_total,
    compute_overall_total,
    days_in_month,
    weekday_of,
    month_title,
    inventory_allowed,
    is_submitted,
    effective_has_supply,
)

app = FastAPI()
app.include_router(admin_router)
app.include_router(records_router)

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.on_event("startup")
def run_additive_migrations():
    migrate(engine)


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; form-action 'self'; frame-ancestors 'none'"
    )
    return response


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def root():
    return RedirectResponse(url="/login-page")


@app.get("/db-check")
def db_check():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok", "db": "connected"}


@app.get("/active-period")
def active_period():
    return get_active_period()


@app.get("/debug/merchants-columns")
def merchants_columns(request: Request, db: Session = Depends(get_db)):
    require_admin(request)
    cols = get_merchants_columns(db)
    return {"table": "merchants", "columns": cols}


@app.post("/login")
def login_api(response: Response, fio: str, last4: str, db: Session = Depends(get_db)):
    user = login_user(db, fio, last4)

    if not user:
        raise HTTPException(status_code=401, detail="Неверные данные")

    response.set_cookie(
        SESSION_COOKIE, make_session(str(user["id"]), "merchant"), httponly=True,
        secure=cookie_secure(), samesite="lax", max_age=12 * 60 * 60,
    )
    return {"status": "ok", "active_period": get_active_period(), "user": {"fio": user["fio"], "tu": user["tu"]}}


def current_merchant(request: Request, db: Session) -> tuple[dict, dict]:
    session = require_merchant(request)
    merchant = get_merchant_by_id(db, int(session["sub"]))
    if not merchant:
        raise HTTPException(status_code=401, detail="Merchant no longer exists")
    return merchant, session


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
            --shadow: 0 12px 32px rgba(0, 0, 0, 0.08);
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            background: var(--bg);
            color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
            min-height: 100vh;
            padding: 20px;
        }

        .page {
            max-width: 980px;
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
            max-width: 960px;
            background: var(--card);
            border-radius: 24px;
            padding: 32px 28px;
            box-shadow: var(--shadow);
        }

        .brand {
            font-family: 'Villula', -apple-system, sans-serif;
            font-size: 28px;
            line-height: 1;
            color: var(--green);
            margin-bottom: 10px;
        }

        h1 {
            font-family: 'Villula', -apple-system, sans-serif;
            font-size: 34px;
            line-height: 1.05;
            margin: 0 0 10px 0;
            color: var(--text);
        }

        .subtitle {
            color: var(--muted);
            font-size: 15px;
            line-height: 1.45;
            margin-bottom: 24px;
        }

        .muted {
            color: var(--muted);
        }

        label {
            display: block;
            margin: 14px 0 6px;
            font-size: 14px;
            font-weight: 700;
            color: var(--text);
        }

        input {
            width: 100%;
            padding: 14px 16px;
            border: 1px solid var(--line);
            border-radius: 14px;
            font-size: 16px;
            background: #fff;
        }

        input:focus {
            outline: none;
            border-color: var(--green);
            box-shadow: 0 0 0 3px rgba(46, 125, 50, 0.10);
        }

        .btn {
            display: inline-block;
            width: 100%;
            margin-top: 20px;
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

        .btn:hover {
            background: var(--green-dark);
        }

        .btn-secondary {
            background: var(--soft);
            color: var(--text);
        }

        .btn-secondary:hover {
            background: #e3ece3;
        }

        .btn-small {
            margin-top: 12px;
            padding: 12px 14px;
            font-size: 15px;
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

        .error-box {
            margin-top: 16px;
            background: #FEF2F2;
            color: var(--error);
            border-radius: 14px;
            padding: 14px;
            line-height: 1.45;
            font-weight: 700;
        }

        .top-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 14px;
            margin-bottom: 24px;
        }

        .info-box {
            background: var(--soft-2);
            border-radius: 16px;
            padding: 16px;
        }

        .info-label {
            color: var(--muted);
            font-size: 13px;
            margin-bottom: 6px;
        }

        .info-value {
            font-size: 18px;
            font-weight: 800;
        }

        .calendar-wrap {
            margin-top: 10px;
        }

        .weekdays,
        .calendar-grid {
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 10px;
        }

        .weekdays {
            margin-bottom: 10px;
        }

        .weekday {
            text-align: center;
            font-size: 13px;
            color: var(--muted);
            font-weight: 700;
            padding: 6px 0;
        }

        .day,
        .day-empty {
            min-height: 96px;
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
        }

        .day:hover {
            border-color: var(--green);
            box-shadow: 0 0 0 2px rgba(46, 125, 50, 0.06);
        }

        .day-empty {
            background: transparent;
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
            margin-top: 20px;
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
            margin-top: 18px;
            color: var(--muted);
            line-height: 1.5;
        }

        .action-list {
            display: grid;
            gap: 12px;
            margin-top: 20px;
        }

        .sum-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 14px;
            margin-bottom: 24px;
        }

        .sum-box {
            background: #F7FBF8;
            border: 1px solid #E5E7EB;
            border-radius: 16px;
            padding: 16px;
        }

        .sum-title {
            color: var(--muted);
            font-size: 13px;
            margin-bottom: 8px;
        }

        .sum-value {
            font-size: 22px;
            font-weight: 900;
        }

        @media (max-width: 760px) {
            .page {
                align-items: flex-start;
            }

            .card-wide {
                padding: 24px 16px;
            }

            .top-grid,
            .sum-grid {
                grid-template-columns: 1fr;
            }

            .weekdays,
            .calendar-grid {
                gap: 8px;
            }

            .day,
            .day-empty {
                min-height: 84px;
                border-radius: 14px;
                padding: 8px;
            }

            .day-number {
                font-size: 16px;
            }

            h1 {
                font-size: 30px;
            }

            .brand {
                font-size: 24px;
            }
        }
    </style>
    """


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
            <div class="subtitle">
                Введите ФИО и последние 4 цифры телефона
            </div>

            <form method="post" action="/login-page">
                <label for="fio">ФИО</label>
                <input
                    id="fio"
                    name="fio"
                    type="text"
                    placeholder="Иванов Иван Иванович"
                    required
                />

                <label for="last4">Последние 4 цифры телефона</label>
                <input
                    id="last4"
                    name="last4"
                    type="text"
                    inputmode="numeric"
                    maxlength="4"
                    placeholder="1234"
                    required
                />

                <button class="btn" type="submit">Войти</button>
            </form>

            <div class="hint">
                Сейчас открыт период за {month_title(period["year"], period["month"])}.
            </div>

            <div class="footer">
                Веб-версия сверок мерчендайзеров
            </div>
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
            <div class="error-box">
                Неверные данные. Проверьте ФИО и последние 4 цифры телефона.
            </div>
            <a class="back" href="/login-page">← Попробовать снова</a>
        </div>
    </div>
</body>
</html>
"""

    response = RedirectResponse(url="/menu-page", status_code=303)
    response.set_cookie(
        SESSION_COOKIE,
        make_session(str(user["id"]), "merchant"),
        httponly=True,
        secure=cookie_secure(),
        samesite="lax",
        max_age=12 * 60 * 60,
    )
    return response


@app.get("/menu-page", response_class=HTMLResponse)
def menu_page(request: Request, db: Session = Depends(get_db)):
    period = get_active_period()
    merchant, session = current_merchant(request, db)
    fio = safe(merchant["fio"])
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
            <div class="subtitle">{fio}</div>
            <div class="hint">
                Сейчас открыт период за {month_title(period["year"], period["month"])}.
            </div>

            <div class="sum-box" style="margin-top: 18px;">
                <div class="sum-title">Общая сумма за месяц</div>
                <div class="sum-value">{overall["total"]} ₽</div>
            </div>

            <a class="btn" href="/point-page">Заполнить сверку</a>
            <a class="btn btn-secondary" href="/summary-page">Моя сумма</a>
        </div>
    </div>
</body>
</html>
"""


@app.get("/point-page", response_class=HTMLResponse)
def point_page(request: Request, db: Session = Depends(get_db)):
    period = get_active_period()
    merchant, session = current_merchant(request, db)
    fio = safe(merchant["fio"])

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
            <div class="subtitle">{fio}</div>

            <div class="hint" style="margin-top: 0; margin-bottom: 18px;">
                Сверка заполняется за {month_title(period["year"], period["month"])}.
            </div>

            <form method="post" action="/point-page">
                <input type="hidden" name="csrf_token" value="{safe(session["csrf"])}" />

                <label for="point_code">Номер точки</label>
                <input id="point_code" name="point_code" type="text" placeholder="2674" required />

                <button class="btn" type="submit">Продолжить</button>
            </form>

            <a class="back" href="/menu-page">← Назад</a>
        </div>
    </div>
</body>
</html>
"""


@app.post("/point-page", response_class=HTMLResponse)
def point_submit(
    request: Request,
    point_code: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    merchant, session = current_merchant(request, db)
    verify_csrf(session, csrf_token)
    fio = safe(merchant["fio"])
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
            <a class="back" href="/point-page">← Назад</a>
        </div>
    </div>
</body>
</html>
"""

    has_supply = point_has_any_supply_in_month(
        db=db,
        point_code=point_code,
        y=period["year"],
        m=period["month"]
    )

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
                В периоде {month_title(period["year"], period["month"])} по точке {point_code} нет поставок.
                <br><br>
                Проверьте номер точки или обратитесь к управляющему.
            </div>
            <a class="back" href="/point-page">← Попробовать снова</a>
        </div>
    </div>
</body>
</html>
"""

    return RedirectResponse(
        url=url("/calendar-page", point_code=point_code),
        status_code=303
    )


def build_calendar_html(point_code: str, y: int, m: int, boxes_map: dict[int, int], visits: dict[int, set[str]], pay_lt5: bool) -> str:
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

        if "DAY" in day_visits:
            badges += '<span class="badge badge-day">В</span>'

        if "FULL_INVENT" in day_visits:
            badges += '<span class="badge badge-inv">И</span>'

        html += f"""
        <a class="day" href="{safe(url("/day-action-page", point_code=point_code, day=day))}">
            <div class="day-number">{day}</div>
            <div class="day-badges">{badges}</div>
        </a>
        """

    html += '</div>'
    return html


def build_records_html(db: Session, merchant_id: int, point_code: str, y: int, m: int, csrf: str, locked: bool) -> str:
    notes = db.execute(text("""
        SELECT id, amount, comment, kind, adjustment_date FROM point_notes
        WHERE merchant_id=:merchant_id AND point_code=:point_code AND year=:year AND month=:month
        ORDER BY created_at, id
    """), {"merchant_id": merchant_id, "point_code": point_code, "year": y, "month": m}).mappings().all()
    reimbursements = db.execute(text("""
        SELECT r.id, r.amount, r.comment, rr.id AS receipt_id, rr.original_name
        FROM point_reimbursements r
        LEFT JOIN reimbursement_receipts rr ON rr.reimbursement_id=r.id
        WHERE r.merchant_id=:merchant_id AND r.point_code=:point_code AND r.year=:year AND r.month=:month
        ORDER BY r.created_at, r.id, rr.id
    """), {"merchant_id": merchant_id, "point_code": point_code, "year": y, "month": m}).mappings().all()
    note_rows = "".join(f"""
        <div class="hint">{safe(row["amount"])} ₽ — {safe(row["comment"])}
        <form method="post" action="/notes/{row["id"]}/delete">
          <input type="hidden" name="csrf_token" value="{safe(csrf)}">
          <button class="btn btn-secondary btn-small" {'disabled' if locked else ''}>Удалить</button>
        </form></div>""" for row in notes)
    grouped: dict[int, dict] = {}
    for row in reimbursements:
        grouped.setdefault(row["id"], {"amount": row["amount"], "comment": row["comment"], "receipts": []})
        if row["receipt_id"]:
            grouped[row["id"]]["receipts"].append(
                f'<a href="/receipts/{row["receipt_id"]}">{safe(row["original_name"])}</a>'
            )
    reimbursement_rows = "".join(f"""
        <div class="hint">{safe(item["amount"])} ₽ — {safe(item["comment"])}<br>{"; ".join(item["receipts"])}
        <form method="post" action="/reimbursements/{record_id}/delete">
          <input type="hidden" name="csrf_token" value="{safe(csrf)}">
          <button class="btn btn-secondary btn-small" {'disabled' if locked else ''}>Удалить</button>
        </form></div>""" for record_id, item in grouped.items())
    disabled = "disabled" if locked else ""
    return f"""
      <h2>Примечания</h2>
      {note_rows or '<div class="hint">Примечаний пока нет.</div>'}
      <form method="post" action="/notes">
        <input type="hidden" name="csrf_token" value="{safe(csrf)}">
        <input type="hidden" name="point_code" value="{safe(point_code)}">
        <label>Сумма (может быть отрицательной)</label><input name="amount" required {disabled}>
        <label>Комментарий</label><input name="comment" required {disabled}>
        <button class="btn btn-small" {disabled}>Добавить примечание</button>
      </form>
      <h2>Возмещения</h2>
      {reimbursement_rows or '<div class="hint">Возмещений пока нет.</div>'}
      <form method="post" action="/reimbursements" enctype="multipart/form-data">
        <input type="hidden" name="csrf_token" value="{safe(csrf)}">
        <input type="hidden" name="point_code" value="{safe(point_code)}">
        <label>Сумма</label><input name="amount" required {disabled}>
        <label>Комментарий</label><input name="comment" required {disabled}>
        <label>Чеки (PDF, PNG или JPEG, можно несколько)</label>
        <input name="receipts" type="file" accept=".pdf,.png,.jpg,.jpeg" multiple required {disabled}>
        <button class="btn btn-small" {disabled}>Добавить возмещение</button>
      </form>
    """


@app.get("/calendar-page", response_class=HTMLResponse)
def calendar_page(
    request: Request,
    point_code: str,
    db: Session = Depends(get_db)
):
    period = get_active_period()
    y = period["year"]
    m = period["month"]

    merchant, session = current_merchant(request, db)
    fio = safe(merchant["fio"])
    point_code = normalize_point_code(point_code)

    boxes_map = get_supply_boxes_map(db, point_code, y, m)
    visits = get_visits_for_month(db, merchant["id"], point_code, y, m)
    point_total = compute_point_total(db, merchant["id"], point_code, y, m)
    overall = compute_overall_total(db, merchant["id"], y, m)
    calendar_html = build_calendar_html(point_code, y, m, boxes_map, visits, point_total["pay_lt5"])
    locked = is_submitted(db, merchant["id"], y, m)
    records_html = build_records_html(db, merchant["id"], point_code, y, m, session["csrf"], locked)

    coffee_html = ""
    if point_total["coffee_enabled"]:
        coffee_html = f"""
                <div class="info-box">
                    <div class="info-label">Кофемашина</div>
                    <div class="info-value">Да, {point_total["coffee_rate"]} ₽</div>
                </div>
        """
    rates_warning = "" if point_total["rates_configured"] else (
        '<div class="error-box">Для точки не настроены ставки. Итог временно рассчитан с нулевыми ставками.</div>'
    )

    return f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Календарь</title>
    {base_css()}
</head>
<body>
    <div class="page">
        <div class="card-wide">
            <div class="brand">ВкусВилл</div>
            <h1>Сверка точки</h1>
            <div class="subtitle">
                Нажмите на нужный день.
                Для пятницы и субботы можно добавить полный инвент.
            </div>

            <div class="top-grid">
                <div class="info-box">
                    <div class="info-label">ФИО</div>
                    <div class="info-value">{fio}</div>
                </div>

                <div class="info-box">
                    <div class="info-label">Точка</div>
                    <div class="info-value">{point_code}</div>
                </div>

                <div class="info-box">
                    <div class="info-label">Расчётный месяц</div>
                    <div class="info-value">{month_title(y, m)}</div>
                </div>

                {coffee_html}

                <div class="info-box">
                    <div class="info-label">Правило поставок</div>
                    <div class="info-value">До 5 коробок не оплачивается</div>
                </div>
            </div>
            {rates_warning}

            <div class="sum-grid">
                <div class="sum-box">
                    <div class="sum-title">Сумма по точке</div>
                    <div class="sum-value">{point_total["total"]} ₽</div>
                </div>

                <div class="sum-box">
                    <div class="sum-title">Общая сумма за месяц</div>
                    <div class="sum-value">{overall["total"]} ₽</div>
                </div>

                {f'''<div class="sum-box">
                    <div class="sum-title">Начислено за кофемашину</div>
                    <div class="sum-value">{point_total["coffee_sum"]} ₽</div>
                </div>''' if point_total["coffee_enabled"] else ''}
            </div>

            <div class="calendar-wrap">
                {calendar_html}
            </div>

            <div class="legend">
                <div class="legend-item">П — в этот день была поставка</div>
                <div class="legend-item">В — отмечен выход</div>
                <div class="legend-item">И — отмечен полный инвент</div>
            </div>

            <div class="calendar-note">
                Выход считается по ставке «с поставкой» или «без поставки» автоматически.
                Кофемашина начисляется автоматически за каждый дневной выход.
            </div>

            <div class="calendar-note">
                Поставки до 5 коробок не оплачиваются.
            </div>
            {records_html}

            <a class="back" href="/point-page">← Выбрать другую точку</a>
        </div>
    </div>
</body>
</html>
"""


@app.get("/day-action-page", response_class=HTMLResponse)
def day_action_page(
    request: Request,
    point_code: str,
    day: int,
    db: Session = Depends(get_db)
):
    period = get_active_period()
    y = period["year"]
    m = period["month"]

    merchant, session = current_merchant(request, db)
    point_code = normalize_point_code(point_code)
    if not point_code:
        raise HTTPException(status_code=400, detail="Invalid point code")

    if day < 1 or day > days_in_month(y, m):
        return RedirectResponse(url=url("/calendar-page", point_code=point_code), status_code=303)

    visits = get_visits_for_month(db, merchant["id"], point_code, y, m)
    day_visits = visits.get(day, set())

    can_inventory = inventory_allowed(db, y, m, day)
    locked = is_submitted(db, merchant["id"], y, m)

    day_btn_text = "Убрать выход" if "DAY" in day_visits else "Добавить выход"
    inv_btn_text = "Убрать полный инвент" if "FULL_INVENT" in day_visits else "Добавить полный инвент"

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
            <h1>Выбор действия</h1>
            <div class="subtitle">
                Точка: {point_code}<br>
                Дата: {day:02d}.{m:02d}.{y}
            </div>

            <div class="action-list">
                <form method="post" action="/toggle-day">
                    <input type="hidden" name="csrf_token" value="{safe(session["csrf"])}">
                    <input type="hidden" name="point_code" value="{safe(point_code)}">
                    <input type="hidden" name="day" value="{day}">
                    <button class="btn btn-small" type="submit" {'disabled' if locked else ''}>
                    {day_btn_text}
                    </button>
                </form>

                {f'''
                <form method="post" action="/toggle-inventory">
                    <input type="hidden" name="csrf_token" value="{safe(session["csrf"])}">
                    <input type="hidden" name="point_code" value="{safe(point_code)}">
                    <input type="hidden" name="day" value="{day}">
                    <button class="btn btn-secondary btn-small" type="submit" {'disabled' if locked else ''}>
                    {inv_btn_text}
                    </button>
                </form>
                ''' if can_inventory else ''}
            </div>

            {'<div class="error-box">Сверка отправлена и заблокирована для изменений.</div>' if locked else ''}
            <a class="back" href="{safe(url("/calendar-page", point_code=point_code))}">← Назад к календарю</a>
        </div>
    </div>
</body>
</html>
"""


@app.post("/toggle-day")
def toggle_day(
    request: Request,
    point_code: str = Form(...),
    day: int = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    y = period["year"]
    m = period["month"]

    merchant, session = current_merchant(request, db)
    verify_csrf(session, csrf_token)
    point_code = normalize_point_code(point_code)
    if is_submitted(db, merchant["id"], y, m):
        raise HTTPException(status_code=409, detail="Reconciliation is submitted")

    if 1 <= day <= days_in_month(y, m):
        toggle_day_visit(db, merchant["id"], point_code, y, m, day)

    return RedirectResponse(
        url=url("/calendar-page", point_code=point_code),
        status_code=303
    )


@app.post("/toggle-inventory")
def toggle_inventory(
    request: Request,
    point_code: str = Form(...),
    day: int = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db)
):
    period = get_active_period()
    y = period["year"]
    m = period["month"]

    merchant, session = current_merchant(request, db)
    verify_csrf(session, csrf_token)
    point_code = normalize_point_code(point_code)
    if is_submitted(db, merchant["id"], y, m):
        raise HTTPException(status_code=409, detail="Reconciliation is submitted")

    if 1 <= day <= days_in_month(y, m):
        if inventory_allowed(db, y, m, day):
            toggle_inventory_visit(db, merchant["id"], point_code, y, m, day)

    return RedirectResponse(
        url=url("/calendar-page", point_code=point_code),
        status_code=303
    )


@app.get("/summary-page", response_class=HTMLResponse)
def summary_page(request: Request, db: Session = Depends(get_db)):
    period = get_active_period()
    merchant, session = current_merchant(request, db)
    fio = safe(merchant["fio"])
    overall = compute_overall_total(db, merchant["id"], period["year"], period["month"])
    locked = is_submitted(db, merchant["id"], period["year"], period["month"])

    point_lines = ""
    if overall["per_point"]:
        for p, total in overall["per_point"].items():
            point_lines += f"<div class='hint' style='margin-top:10px'>{p} — {total} ₽</div>"
    else:
        point_lines = "<div class='hint' style='margin-top:10px'>Пока нет отмеченных точек за этот месяц.</div>"

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
            <div class="subtitle">{fio}</div>

            <div class="sum-box">
                <div class="sum-title">Общая сумма за месяц</div>
                <div class="sum-value">{overall["total"]} ₽</div>
            </div>

            {point_lines}

            <div class="hint">
                Сейчас открыт период за {month_title(period["year"], period["month"])}.
            </div>
            {f'<div class="hint">Сверка отправлена. Изменения заблокированы.</div>' if locked else f'''
            <form method="post" action="/submit-reconciliation">
                <input type="hidden" name="csrf_token" value="{safe(session["csrf"])}">
                <button class="btn" type="submit">Отправить сверку по всем точкам — {overall["total"]} ₽</button>
            </form>'''}

            <a class="back" href="/menu-page">← Назад</a>
        </div>
    </div>
</body>
</html>
"""
