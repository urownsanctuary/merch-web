from fastapi import FastAPI, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import SessionLocal, engine
from app.services import (
    get_active_period,
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
)

app = FastAPI()

app.mount("/static", StaticFiles(directory="app/static"), name="static")


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
def merchants_columns(db: Session = Depends(get_db)):
    cols = get_merchants_columns(db)
    return {"table": "merchants", "columns": cols}


@app.post("/login")
def login_api(fio: str, last4: str, db: Session = Depends(get_db)):
    user = login_user(db, fio, last4)

    if not user:
        raise HTTPException(status_code=401, detail="Неверные данные")

    return {"status": "ok", "active_period": get_active_period(), "user": user}


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

        .detail-line {
            font-size: 15px;
            font-weight: 700;
            line-height: 1.5;
        }

        .calendar-wrap {
            margin-top: 4px;
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

        .action-list {
            display: grid;
            gap: 12px;
            margin-top: 20px;
        }

        @media (max-width: 760px) {
            .page {
                align-items: flex-start;
            }

            .card-wide {
                padding: 18px 14px 24px;
            }

            .sum-strip,
            .details-grid {
                grid-template-columns: 1fr;
            }

            .weekdays,
            .calendar-grid {
                gap: 8px;
            }

            .day,
            .day-empty {
                min-height: 80px;
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

            .calendar-month {
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

    fio_safe = user["fio"]
    return RedirectResponse(
        url=f"/menu-page?fio={fio_safe}",
        status_code=303
    )


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
            <div class="subtitle">{fio}</div>
            <div class="hint">
                Сейчас открыт период за {month_title(period["year"], period["month"])}.
            </div>

            <div class="sum-card" style="margin-top: 18px;">
                <div class="sum-title">Общая сумма за месяц</div>
                <div class="sum-value">{overall["total"]} ₽</div>
            </div>

            <a class="btn" href="/point-page?fio={fio}">Заполнить сверку</a>
            <a class="btn btn-secondary" href="/summary-page?fio={fio}">Моя сумма</a>
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
            <div class="subtitle">{fio}</div>

            <div class="hint" style="margin-top: 0; margin-bottom: 18px;">
                Сверка заполняется за {month_title(period["year"], period["month"])}.
            </div>

            <form method="post" action="/point-page">
                <input type="hidden" name="fio" value="{fio}" />

                <label for="point_code">Номер точки</label>
                <input id="point_code" name="point_code" type="text" placeholder="2674" required />

                <button class="btn" type="submit">Продолжить</button>
            </form>

            <a class="back" href="/menu-page?fio={fio}">← Назад</a>
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
            <a class="back" href="/point-page?fio={fio}">← Назад</a>
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
            <a class="back" href="/point-page?fio={fio}">← Попробовать снова</a>
        </div>
    </div>
</body>
</html>
"""

    return RedirectResponse(
        url=f"/calendar-page?fio={fio}&point_code={point_code}",
        status_code=303
    )


def build_day_href(fio: str, point_code: str, y: int, m: int, day: int) -> str:
    wd = weekday_of(y, m, day)
    if wd in (4, 5):
        return f"/day-action-page?fio={fio}&point_code={point_code}&day={day}"
    return f"/toggle-day?fio={fio}&point_code={point_code}&day={day}"


def build_calendar_html(fio: str, point_code: str, y: int, m: int, boxes_map: dict[int, int], visits: dict[int, set[str]]) -> str:
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

        if boxes > 0:
            badges += '<span class="badge badge-supply">П</span>'

        if "DAY" in day_visits:
            badges += '<span class="badge badge-day">В</span>'

        if "FULL_INVENT" in day_visits:
            badges += '<span class="badge badge-inv">И</span>'

        href = build_day_href(fio, point_code, y, m, day)

        html += f"""
        <a class="day" href="{href}">
            <div class="day-number">{day}</div>
            <div class="day-badges">{badges}</div>
        </a>
        """

    html += '</div>'
    return html


@app.get("/calendar-page", response_class=HTMLResponse)
def calendar_page(
    fio: str,
    point_code: str,
    db: Session = Depends(get_db)
):
    period = get_active_period()
    y = period["year"]
    m = period["month"]

    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)

    boxes_map = get_supply_boxes_map(db, point_code, y, m)
    visits = get_visits_for_month(db, merchant["id"], point_code, y, m)
    point_total = compute_point_total(db, merchant["id"], point_code, y, m)
    overall = compute_overall_total(db, merchant["id"], y, m)
    calendar_html = build_calendar_html(fio, point_code, y, m, boxes_map, visits)

    coffee_text = "Да" if point_total["coffee_enabled"] else "Нет"

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
            <div class="calendar-head">
                <div>
                    <div class="brand">ВкусВилл</div>
                    <div class="calendar-month">{month_title(y, m)}</div>
                </div>

                <div class="calendar-meta">
                    <div class="mini-pill">Точка: {point_code}</div>
                    <div class="mini-pill">{fio}</div>
                    <div class="mini-pill">КМ: {coffee_text}</div>
                </div>
            </div>

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

                <div class="detail-card">
                    <div class="detail-title">Кофемашина</div>
                    <div class="detail-line">{point_total["coffee_cnt"]} × {point_total["coffee_rate"]} ₽ = {point_total["coffee_sum"]} ₽</div>
                </div>
            </div>

            <div class="calendar-wrap">
                {calendar_html}
            </div>

            <div class="legend">
                <div class="legend-item">П — была поставка</div>
                <div class="legend-item">В — отмечен выход</div>
                <div class="legend-item">И — полный инвент</div>
            </div>

            <div class="calendar-note">
                В обычные дни нажатие по дню сразу ставит или убирает выход.
                В пятницу и субботу открывается выбор: выход или полный инвент.
            </div>

            <div class="calendar-note">
                Поставки до 5 коробок не оплачиваются.
            </div>

            <a class="back" href="/point-page?fio={fio}">← Выбрать другую точку</a>
        </div>
    </div>
</body>
</html>
"""


@app.get("/day-action-page", response_class=HTMLResponse)
def day_action_page(
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

    if day < 1 or day > days_in_month(y, m):
        return RedirectResponse(url=f"/calendar-page?fio={fio}&point_code={point_code}", status_code=303)

    visits = get_visits_for_month(db, merchant["id"], point_code, y, m)
    day_visits = visits.get(day, set())

    wd = weekday_of(y, m, day)
    is_fri_or_sat = wd in (4, 5)

    day_btn_text = "Убрать выход" if "DAY" in day_visits else "Добавить выход"
    inv_btn_text = "Убрать полный инвент" if "FULL_INVENT" in day_visits else "Добавить полный инвент"

    if not is_fri_or_sat:
        return RedirectResponse(url=f"/toggle-day?fio={fio}&point_code={point_code}&day={day}", status_code=303)

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
                <a class="btn btn-small" href="/toggle-day?fio={fio}&point_code={point_code}&day={day}">
                    {day_btn_text}
                </a>

                <a class="btn btn-secondary btn-small" href="/toggle-inventory?fio={fio}&point_code={point_code}&day={day}">
                    {inv_btn_text}
                </a>
            </div>

            <a class="back" href="/calendar-page?fio={fio}&point_code={point_code}">← Назад к календарю</a>
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
    period = get_active_period()
    y = period["year"]
    m = period["month"]

    merchant = get_merchant_by_fio(db, fio)
    if not merchant:
        return RedirectResponse(url="/login-page", status_code=303)

    if 1 <= day <= days_in_month(y, m):
        toggle_day_visit(db, merchant["id"], point_code, y, m, day)

    return RedirectResponse(
        url=f"/calendar-page?fio={fio}&point_code={point_code}",
        status_code=303
    )


@app.get("/toggle-inventory")
def toggle_inventory(
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

    if 1 <= day <= days_in_month(y, m):
        wd = weekday_of(y, m, day)
        if wd in (4, 5):
            toggle_inventory_visit(db, merchant["id"], point_code, y, m, day)

    return RedirectResponse(
        url=f"/calendar-page?fio={fio}&point_code={point_code}",
        status_code=303
    )


@app.get("/summary-page", response_class=HTMLResponse)
def summary_page(fio: str = "", db: Session = Depends(get_db)):
    period = get_active_period()
    merchant = get_merchant_by_fio(db, fio)
    overall = {"total": 0, "per_point": {}}

    if merchant:
        overall = compute_overall_total(db, merchant["id"], period["year"], period["month"])

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

            <div class="sum-card">
                <div class="sum-title">Общая сумма за месяц</div>
                <div class="sum-value">{overall["total"]} ₽</div>
            </div>

            {point_lines}

            <div class="hint">
                Сейчас открыт период за {month_title(period["year"], period["month"])}.
            </div>

            <a class="back" href="/menu-page?fio={fio}">← Назад</a>
        </div>
    </div>
</body>
</html>
"""
