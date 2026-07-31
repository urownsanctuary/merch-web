"""Safe merchant administration with stable identities and audit history."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session


MERCHANT_STATUSES = frozenset({"active", "inactive"})
MERCHANT_SORTS = {
    "fio_asc": "fio_norm ASC, id ASC",
    "fio_desc": "fio_norm DESC, id DESC",
    "created_desc": "created_at DESC, id DESC",
    "created_asc": "created_at ASC, id ASC",
    "updated_desc": "updated_at DESC, id DESC",
    "updated_asc": "updated_at ASC, id ASC",
    "tu_asc": "tu ASC, fio_norm ASC, id ASC",
}


@dataclass
class MerchantInputError(ValueError):
    field_errors: dict[str, str] = field(default_factory=dict)
    message: str = "Проверьте введённые данные."
    duplicate_id: int | None = None
    same_name_id: int | None = None
    requires_confirmation: bool = False

    def __str__(self) -> str:
        return self.message


def normalize_fio_display(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u00a0", " ")).strip()


def normalize_fio_search(value: Any) -> str:
    normalized = normalize_fio_display(value).lower().replace("ё", "е")
    normalized = re.sub(r"[^а-яa-z\s]", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def normalize_tu(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u00a0", " ")).strip()


def validate_merchant_values(
    fio: Any,
    last4: Any,
    tu: Any,
    *,
    fio_normalizer,
) -> dict[str, str]:
    fio_clean = normalize_fio_display(fio)
    last4_raw = str(last4 if last4 is not None else "")
    tu_clean = normalize_tu(tu)
    errors: dict[str, str] = {}
    if not fio_clean or not fio_normalizer(fio_clean):
        errors["fio"] = "Укажите ФИО сотрудника."
    if not re.fullmatch(r"\d{4}", last4_raw):
        errors["last4"] = "Введите ровно 4 цифры без пробелов и других символов."
    if not tu_clean:
        errors["tu"] = "Выберите или укажите ТУ."
    if errors:
        raise MerchantInputError(field_errors=errors)
    return {
        "fio": fio_clean,
        "fio_norm": fio_normalizer(fio_clean),
        "last4": last4_raw,
        "tu": tu_clean,
    }


def _merchant_columns(db: Session) -> set[str]:
    inspector = inspect(db.get_bind())
    if not inspector.has_table("merchants"):
        return set()
    return {column["name"] for column in inspector.get_columns("merchants")}


def ensure_merchant_admin_schema(db: Session) -> None:
    """Apply only additive, idempotent merchant administration schema changes."""
    columns = _merchant_columns(db)
    if not columns:
        return
    additions = (
        ("last4", "TEXT"),
        ("is_active", "BOOLEAN NOT NULL DEFAULT TRUE"),
        ("created_at", "TIMESTAMP"),
        ("updated_at", "TIMESTAMP"),
    )
    dialect = db.get_bind().dialect.name
    for column, definition in additions:
        if dialect == "postgresql":
            db.execute(
                text(
                    f"ALTER TABLE merchants ADD COLUMN IF NOT EXISTS {column} {definition}"
                )
            )
        elif column not in columns:
            db.execute(text(f"ALTER TABLE merchants ADD COLUMN {column} {definition}"))
    db.execute(
        text(
            """
            UPDATE merchants
            SET is_active = COALESCE(is_active, TRUE),
                created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
                updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)
            WHERE is_active IS NULL OR created_at IS NULL OR updated_at IS NULL
            """
        )
    )
    if dialect == "postgresql":
        # Production's legacy schema enforces one row per FIO. Replace only
        # those known legacy constraints so the new (FIO, last4) identity can
        # represent namesakes without touching merchant rows.
        for legacy_name in ("merchants_fio_key", "merchants_fio_norm_uq"):
            db.execute(
                text(
                    f"ALTER TABLE merchants DROP CONSTRAINT IF EXISTS {legacy_name}"
                )
            )
            db.execute(text(f"DROP INDEX IF EXISTS {legacy_name}"))
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS merchant_audit_log (
                id TEXT PRIMARY KEY,
                merchant_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                changed_fields TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_merchants_fio_norm_last4
            ON merchants (fio_norm, last4)
            WHERE last4 IS NOT NULL
            """
        )
    )
    db.commit()


def _audit(
    db: Session,
    merchant_id: int,
    action: str,
    actor: str,
    changed_fields: list[str],
) -> None:
    db.execute(
        text(
            """
            INSERT INTO merchant_audit_log
                (id, merchant_id, action, actor, changed_fields)
            VALUES
                (:id, :merchant_id, :action, :actor, :changed_fields)
            """
        ),
        {
            "id": uuid.uuid4().hex,
            "merchant_id": merchant_id,
            "action": action,
            "actor": str(actor or "admin")[:200],
            "changed_fields": json.dumps(
                sorted(set(changed_fields)), ensure_ascii=False, separators=(",", ":")
            ),
        },
    )


def _same_name_rows(
    db: Session,
    fio_normalized: str,
    *,
    exclude_id: int | None = None,
) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT id, fio, fio_norm, pass_hash, last4, tu, is_active
            FROM merchants
            WHERE fio_norm = :fio_norm
              AND (:exclude_id IS NULL OR id <> :exclude_id)
            ORDER BY id
            """
        ),
        {"fio_norm": fio_normalized, "exclude_id": exclude_id},
    ).mappings().all()
    return [dict(row) for row in rows]


def _check_duplicates(
    db: Session,
    values: dict[str, str],
    *,
    password_hash: str,
    exclude_id: int | None = None,
    confirm_same_name: bool = False,
) -> None:
    same_name_rows = _same_name_rows(
        db, values["fio_norm"], exclude_id=exclude_id
    )
    for row in same_name_rows:
        exact_last4 = row.get("last4") == values["last4"]
        legacy_exact = not row.get("last4") and row.get("pass_hash") == password_hash
        if exact_last4 or legacy_exact:
            raise MerchantInputError(
                message="Сотрудник с таким ФИО и последними четырьмя цифрами уже существует.",
                duplicate_id=int(row["id"]),
            )
    if same_name_rows and not confirm_same_name:
        raise MerchantInputError(
            message=(
                "Сотрудник с таким ФИО уже существует, но последние четыре цифры отличаются. "
                "Убедитесь, что это другой человек."
            ),
            same_name_id=int(same_name_rows[0]["id"]),
            requires_confirmation=True,
        )


def create_merchant(
    db: Session,
    fio: Any,
    last4: Any,
    tu: Any,
    *,
    actor: str,
    fio_normalizer,
    last4_hasher,
    confirm_same_name: bool = False,
) -> dict[str, Any]:
    values = validate_merchant_values(
        fio, last4, tu, fio_normalizer=fio_normalizer
    )
    password_hash = last4_hasher(values["last4"])
    _check_duplicates(
        db,
        values,
        password_hash=password_hash,
        confirm_same_name=confirm_same_name,
    )
    result = db.execute(
        text(
            """
            INSERT INTO merchants
                (fio, fio_norm, pass_hash, last4, tu, is_active, updated_at)
            VALUES
                (:fio, :fio_norm, :pass_hash, :last4, :tu, TRUE, CURRENT_TIMESTAMP)
            RETURNING id
            """
        ),
        {**values, "pass_hash": password_hash},
    )
    merchant_id = int(result.scalar_one())
    _audit(
        db,
        merchant_id,
        "created",
        actor,
        ["fio", "last4", "tu", "status"],
    )
    return {"id": merchant_id, **values, "status": "active"}


def get_merchant_for_admin(db: Session, merchant_id: int) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT id, fio, fio_norm, last4, tu, is_active, created_at, updated_at
            FROM merchants
            WHERE id = :merchant_id
            """
        ),
        {"merchant_id": merchant_id},
    ).mappings().first()
    return dict(row) if row else None


def update_merchant(
    db: Session,
    merchant_id: int,
    fio: Any,
    last4: Any,
    tu: Any,
    status: str,
    *,
    actor: str,
    fio_normalizer,
    last4_hasher,
    confirm_same_name: bool = False,
) -> dict[str, Any]:
    existing = get_merchant_for_admin(db, merchant_id)
    if not existing:
        raise MerchantInputError(message="Сотрудник не найден.")
    values = validate_merchant_values(
        fio, last4, tu, fio_normalizer=fio_normalizer
    )
    status = str(status or "").strip().lower()
    if status not in MERCHANT_STATUSES:
        raise MerchantInputError(
            field_errors={"status": "Выберите корректный статус."}
        )
    password_hash = last4_hasher(values["last4"])
    _check_duplicates(
        db,
        values,
        password_hash=password_hash,
        exclude_id=merchant_id,
        confirm_same_name=confirm_same_name,
    )
    is_active = status == "active"
    changed_fields = [
        field_name
        for field_name, new_value in (
            ("fio", values["fio"]),
            ("last4", values["last4"]),
            ("tu", values["tu"]),
            ("status", is_active),
        )
        if existing.get(
            "is_active" if field_name == "status" else field_name
        )
        != new_value
    ]
    if changed_fields:
        db.execute(
            text(
                """
                UPDATE merchants
                SET fio = :fio,
                    fio_norm = :fio_norm,
                    pass_hash = :pass_hash,
                    last4 = :last4,
                    tu = :tu,
                    is_active = :is_active,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :merchant_id
                """
            ),
            {
                **values,
                "pass_hash": password_hash,
                "is_active": is_active,
                "merchant_id": merchant_id,
            },
        )
        _audit(db, merchant_id, "updated", actor, changed_fields)
    return {"id": merchant_id, **values, "status": status}


def set_merchant_active(
    db: Session,
    merchant_id: int,
    active: bool,
    *,
    actor: str,
) -> bool:
    existing = get_merchant_for_admin(db, merchant_id)
    if not existing:
        raise MerchantInputError(message="Сотрудник не найден.")
    if bool(existing["is_active"]) == bool(active):
        return False
    db.execute(
        text(
            """
            UPDATE merchants
            SET is_active = :is_active, updated_at = CURRENT_TIMESTAMP
            WHERE id = :merchant_id
            """
        ),
        {"is_active": bool(active), "merchant_id": merchant_id},
    )
    _audit(
        db,
        merchant_id,
        "restored" if active else "deactivated",
        actor,
        ["status"],
    )
    return True


def deactivate_merchants_by_tu(
    db: Session,
    tu: str,
    *,
    actor: str,
) -> int:
    rows = db.execute(
        text(
            """
            SELECT id
            FROM merchants
            WHERE tu = :tu AND COALESCE(is_active, TRUE) = TRUE
            ORDER BY id
            """
        ),
        {"tu": normalize_tu(tu)},
    ).all()
    for row in rows:
        set_merchant_active(db, int(row[0]), False, actor=actor)
    return len(rows)


def deactivate_all_merchants(
    db: Session,
    *,
    actor: str,
) -> int:
    rows = db.execute(
        text(
            """
            SELECT id
            FROM merchants
            WHERE COALESCE(is_active, TRUE) = TRUE
            ORDER BY id
            """
        )
    ).all()
    for row in rows:
        set_merchant_active(db, int(row[0]), False, actor=actor)
    return len(rows)


def import_or_reactivate_merchant(
    db: Session,
    fio: Any,
    last4: Any,
    tu: Any,
    *,
    actor: str,
    fio_normalizer,
    last4_hasher,
) -> dict[str, Any]:
    """Match imports only by normalized FIO plus last4, preserving merchant_id."""
    values = validate_merchant_values(
        fio, last4, tu, fio_normalizer=fio_normalizer
    )
    existing_id = db.execute(
        text(
            """
            SELECT id
            FROM merchants
            WHERE fio_norm = :fio_norm AND last4 = :last4
            ORDER BY id
            LIMIT 1
            """
        ),
        {"fio_norm": values["fio_norm"], "last4": values["last4"]},
    ).scalar()
    if existing_id is None:
        created = create_merchant(
            db,
            values["fio"],
            values["last4"],
            values["tu"],
            actor=actor,
            fio_normalizer=fio_normalizer,
            last4_hasher=last4_hasher,
            confirm_same_name=True,
        )
        return {**created, "created": True, "reactivated": False}

    existing = get_merchant_for_admin(db, int(existing_id))
    update_merchant(
        db,
        int(existing_id),
        values["fio"],
        values["last4"],
        values["tu"],
        "active",
        actor=actor,
        fio_normalizer=fio_normalizer,
        last4_hasher=last4_hasher,
        confirm_same_name=True,
    )
    return {
        "id": int(existing_id),
        **values,
        "status": "active",
        "created": False,
        "reactivated": not bool(existing and existing.get("is_active")),
    }


def list_merchants(
    db: Session,
    *,
    fio_query: str = "",
    last4_query: str = "",
    tu: str = "",
    status: str = "",
    sort: str = "fio_asc",
) -> list[dict[str, Any]]:
    conditions = ["1 = 1"]
    params: dict[str, Any] = {}
    fio_query = normalize_fio_search(fio_query)
    last4_query = str(last4_query or "").strip()
    tu = normalize_tu(tu)
    status = str(status or "").strip().lower()
    if fio_query:
        conditions.append("fio_norm LIKE :fio_query")
        params["fio_query"] = f"%{fio_query}%"
    if last4_query:
        conditions.append("last4 LIKE :last4_query")
        params["last4_query"] = f"%{last4_query}%"
    if tu:
        conditions.append("tu = :tu")
        params["tu"] = tu
    if status == "active":
        conditions.append("COALESCE(is_active, TRUE) = TRUE")
    elif status == "inactive":
        conditions.append("COALESCE(is_active, TRUE) = FALSE")
    order_by = MERCHANT_SORTS.get(sort, MERCHANT_SORTS["fio_asc"])
    rows = db.execute(
        text(
            f"""
            SELECT id, fio, fio_norm, last4, tu, is_active, created_at, updated_at
            FROM merchants
            WHERE {' AND '.join(conditions)}
            ORDER BY {order_by}
            """
        ),
        params,
    ).mappings().all()
    return [dict(row) for row in rows]


def list_merchant_audit(db: Session, merchant_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT action, actor, changed_fields, created_at
            FROM merchant_audit_log
            WHERE merchant_id = :merchant_id
            ORDER BY created_at DESC, id DESC
            """
        ),
        {"merchant_id": merchant_id},
    ).mappings().all()
    return [dict(row) for row in rows]
