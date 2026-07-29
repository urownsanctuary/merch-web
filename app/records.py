import hashlib
import os
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import PurePath

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import RedirectResponse, Response
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.security import read_session, require_merchant, url, verify_csrf, ADMIN_COOKIE
from app.services import (
    effective_has_supply,
    get_active_period,
    get_merchant_by_id,
    get_point_rates,
    get_supply_boxes_map,
    is_submitted,
    normalize_point_code,
)


router = APIRouter()
MAX_RECEIPT_BYTES = int(os.getenv("MAX_RECEIPT_BYTES", str(5 * 1024 * 1024)))
ALLOWED_RECEIPTS = {
    "application/pdf": (b"%PDF-",),
    "image/png": (b"\x89PNG\r\n\x1a\n",),
    "image/jpeg": (b"\xff\xd8\xff",),
}


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _owner(request: Request, db: Session) -> tuple[dict, dict, dict]:
    session = require_merchant(request)
    merchant = get_merchant_by_id(db, int(session["sub"]))
    if not merchant:
        raise HTTPException(status_code=401, detail="Merchant no longer exists")
    return merchant, session, get_active_period()


def _amount(raw: str, positive: bool = False) -> Decimal:
    try:
        amount = Decimal(raw.replace(",", "."))
    except (InvalidOperation, AttributeError):
        raise HTTPException(status_code=422, detail="Invalid amount")
    if (positive and amount <= 0) or (not positive and amount == 0):
        raise HTTPException(status_code=422, detail="Amount is outside the allowed range")
    return amount.quantize(Decimal("0.01"))


def _point(raw: str) -> str:
    point = normalize_point_code(raw)
    if not point:
        raise HTTPException(status_code=422, detail="Invalid point code")
    return point


def _assert_editable(db: Session, merchant_id: int, period: dict) -> None:
    if is_submitted(db, merchant_id, period["year"], period["month"]):
        raise HTTPException(status_code=409, detail="Reconciliation is submitted")


@router.post("/notes")
def add_note(
    request: Request,
    point_code: str = Form(...),
    amount: str = Form(...),
    comment: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    merchant, session, period = _owner(request, db)
    verify_csrf(session, csrf_token)
    _assert_editable(db, merchant["id"], period)
    comment = comment.strip()
    if not comment:
        raise HTTPException(status_code=422, detail="Comment is required")
    db.execute(text("""
        INSERT INTO point_notes (merchant_id, point_code, year, month, amount, comment)
        VALUES (:merchant_id, :point_code, :year, :month, :amount, :comment)
    """), {
        "merchant_id": merchant["id"], "point_code": _point(point_code),
        "year": period["year"], "month": period["month"],
        "amount": _amount(amount), "comment": comment,
    })
    db.commit()
    return RedirectResponse(url=url("/calendar-page", point_code=_point(point_code)), status_code=303)


@router.post("/notes/{note_id}/delete")
def delete_note(note_id: int, request: Request, csrf_token: str = Form(...), db: Session = Depends(get_db)):
    merchant, session, period = _owner(request, db)
    verify_csrf(session, csrf_token)
    _assert_editable(db, merchant["id"], period)
    point = db.execute(text("""
        DELETE FROM point_notes WHERE id=:id AND merchant_id=:merchant_id
        AND year=:year AND month=:month RETURNING point_code
    """), {"id": note_id, "merchant_id": merchant["id"], "year": period["year"], "month": period["month"]}).scalar()
    if not point:
        raise HTTPException(status_code=404, detail="Note not found")
    db.commit()
    return RedirectResponse(url=url("/calendar-page", point_code=point), status_code=303)


async def _validated_receipt(upload: UploadFile) -> tuple[str, str, bytes, str]:
    content_type = (upload.content_type or "").lower()
    if content_type not in ALLOWED_RECEIPTS:
        raise HTTPException(status_code=415, detail="Only PDF, PNG and JPEG receipts are allowed")
    data = await upload.read(MAX_RECEIPT_BYTES + 1)
    if not data or len(data) > MAX_RECEIPT_BYTES:
        raise HTTPException(status_code=413, detail="Receipt is empty or too large")
    if not any(data.startswith(magic) for magic in ALLOWED_RECEIPTS[content_type]):
        raise HTTPException(status_code=415, detail="Receipt content does not match its MIME type")
    name = PurePath(upload.filename or "receipt").name[:255]
    allowed_extensions = {
        "application/pdf": {".pdf"},
        "image/png": {".png"},
        "image/jpeg": {".jpg", ".jpeg"},
    }
    if PurePath(name).suffix.lower() not in allowed_extensions[content_type]:
        raise HTTPException(status_code=415, detail="Receipt filename extension does not match its MIME type")
    return name, content_type, data, hashlib.sha256(data).hexdigest()


@router.post("/reimbursements")
async def add_reimbursement(
    request: Request,
    point_code: str = Form(...),
    amount: str = Form(...),
    comment: str = Form(...),
    csrf_token: str = Form(...),
    receipts: list[UploadFile] = File(...),
    db: Session = Depends(get_db),
):
    merchant, session, period = _owner(request, db)
    verify_csrf(session, csrf_token)
    _assert_editable(db, merchant["id"], period)
    comment = comment.strip()
    if not comment or not receipts:
        raise HTTPException(status_code=422, detail="Comment and at least one receipt are required")
    if len(receipts) > 10:
        raise HTTPException(status_code=413, detail="At most 10 receipts are allowed per reimbursement")
    validated = [await _validated_receipt(receipt) for receipt in receipts]
    if sum(len(item[2]) for item in validated) > MAX_RECEIPT_BYTES * 5:
        raise HTTPException(status_code=413, detail="Combined receipt size is too large")
    try:
        reimbursement_id = db.execute(text("""
            INSERT INTO point_reimbursements (merchant_id, point_code, year, month, amount, comment)
            VALUES (:merchant_id, :point_code, :year, :month, :amount, :comment)
            RETURNING id
        """), {
            "merchant_id": merchant["id"], "point_code": _point(point_code),
            "year": period["year"], "month": period["month"],
            "amount": _amount(amount, positive=True), "comment": comment,
        }).scalar_one()
        for name, content_type, data, digest in validated:
            db.execute(text("""
                INSERT INTO reimbursement_receipts
                    (reimbursement_id, original_name, content_type, byte_size, sha256, content)
                VALUES (:reimbursement_id, :name, :content_type, :size, :digest, :content)
            """), {
                "reimbursement_id": reimbursement_id, "name": name, "content_type": content_type,
                "size": len(data), "digest": digest, "content": data,
            })
        db.commit()
    except Exception:
        db.rollback()
        raise
    return RedirectResponse(url=url("/calendar-page", point_code=_point(point_code)), status_code=303)


@router.post("/reimbursements/{reimbursement_id}/delete")
def delete_reimbursement(
    reimbursement_id: int, request: Request, csrf_token: str = Form(...), db: Session = Depends(get_db)
):
    merchant, session, period = _owner(request, db)
    verify_csrf(session, csrf_token)
    _assert_editable(db, merchant["id"], period)
    point = db.execute(text("""
        DELETE FROM point_reimbursements WHERE id=:id AND merchant_id=:merchant_id
        AND year=:year AND month=:month RETURNING point_code
    """), {
        "id": reimbursement_id, "merchant_id": merchant["id"],
        "year": period["year"], "month": period["month"],
    }).scalar()
    if not point:
        raise HTTPException(status_code=404, detail="Reimbursement not found")
    db.commit()
    return RedirectResponse(url=url("/calendar-page", point_code=point), status_code=303)


@router.get("/receipts/{receipt_id}")
def get_receipt(receipt_id: int, request: Request, db: Session = Depends(get_db)):
    merchant_session = read_session(request.cookies.get("merch_session"), "merchant")
    admin_session = read_session(request.cookies.get(ADMIN_COOKIE), "admin")
    if not merchant_session and not admin_session:
        raise HTTPException(status_code=401, detail="Authentication required")
    params = {"receipt_id": receipt_id}
    owner_clause = ""
    if not admin_session:
        owner_clause = "AND r.merchant_id=:merchant_id"
        params["merchant_id"] = int(merchant_session["sub"])
    row = db.execute(text(f"""
        SELECT rr.original_name, rr.content_type, rr.content
        FROM reimbursement_receipts rr
        JOIN point_reimbursements r ON r.id=rr.reimbursement_id
        WHERE rr.id=:receipt_id {owner_clause}
    """), params).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Receipt not found")
    return Response(
        content=bytes(row["content"]),
        media_type=row["content_type"],
        headers={"Content-Disposition": f'inline; filename="receipt-{receipt_id}"', "Cache-Control": "private, no-store"},
    )


@router.post("/submit-reconciliation")
def submit_reconciliation(request: Request, csrf_token: str = Form(...), db: Session = Depends(get_db)):
    merchant, session, period = _owner(request, db)
    verify_csrf(session, csrf_token)
    db.execute(text("""
        INSERT INTO reconciliation_submissions (merchant_id, year, month)
        VALUES (:merchant_id, :year, :month)
        ON CONFLICT (merchant_id, year, month)
        DO UPDATE SET submitted_at=CURRENT_TIMESTAMP, reopened_at=NULL
    """), {"merchant_id": merchant["id"], "year": period["year"], "month": period["month"]})
    db.commit()
    return RedirectResponse(url="/summary-page", status_code=303)


@router.post("/supply-corrections")
def add_no_supply_correction(
    request: Request,
    point_code: str = Form(...),
    day: int = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    merchant, session, period = _owner(request, db)
    verify_csrf(session, csrf_token)
    _assert_editable(db, merchant["id"], period)
    point = _point(point_code)
    try:
        target = date(period["year"], period["month"], day)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid date")
    rates = get_point_rates(db, point, period["year"], period["month"])
    if not rates["rates_configured"]:
        raise HTTPException(status_code=409, detail="Point rates are not configured")
    boxes = get_supply_boxes_map(db, point, period["year"], period["month"]).get(day, 0)
    if not effective_has_supply(boxes, rates["pay_lt5"]):
        raise HTTPException(status_code=422, detail="The selected date is not a paid supply date")
    amount = Decimal(rates["rate_no_supply"] - rates["rate_supply"])
    try:
        db.execute(text("""
            INSERT INTO point_notes
                (merchant_id, point_code, year, month, amount, comment, kind, adjustment_date)
            VALUES (:merchant_id, :point_code, :year, :month, :amount, :comment, 'no_supply_taken', :target)
        """), {
            "merchant_id": merchant["id"], "point_code": point,
            "year": period["year"], "month": period["month"], "amount": amount,
            "comment": "Не принимал поставку в день с поставкой", "target": target,
        })
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="This date has already been corrected")
    return RedirectResponse(url=url("/calendar-page", point_code=point), status_code=303)
