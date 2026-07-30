"""Create a test-only, anonymized snapshot for legacy migration verification.

The source connection is forced into a PostgreSQL REPEATABLE READ, READ ONLY
transaction before any query is issued. The generated SQL contains only the
minimal legacy-migration dataset and refuses to load unless an explicit psql
test-only variable is supplied.

Render Shell usage:
    python -m app.legacy_snapshot --output /tmp/merch-web-legacy-snapshot.sql
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from psycopg2 import extensions, sql
from psycopg2.extras import RealDictCursor


REQUIRED_POINT_ADJUSTMENT_COLUMNS = (
    "id",
    "merchant_id",
    "point_code",
    "month_key",
    "note_amount",
    "note_comment",
    "reimb_amount",
    "reimb_comment",
    "reimb_receipt",
)
NORMALIZED_COLUMNS = {
    "point_notes": (
        "id",
        "merchant_id",
        "point_code",
        "month_key",
        "amount",
        "comment",
        "legacy_key",
        "created_at",
    ),
    "point_reimbursements": (
        "id",
        "merchant_id",
        "point_code",
        "month_key",
        "amount",
        "comment",
        "legacy_key",
        "created_at",
    ),
    "reimbursement_receipts": (
        "id",
        "reimbursement_id",
        "legacy_path",
        "legacy_key",
        "created_at",
    ),
}
EXCLUDED_FIELDS = (
    "merchants.* (including fio, fio_norm, phone/last4, pass_hash, tu and email)",
    "receipt_files.data",
    "receipt_files.original_filename (only a safe extension is retained)",
    "all application tables unrelated to legacy point_adjustments migration",
)
URL_RE = re.compile(r"(?i)\b(?:https?|ftp)://[^\s|]+")
EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
CONNECTION_RE = re.compile(r"(?i)\b(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?)://[^\s'\"]+")
SECRET_RE = re.compile(
    r"(?i)\b(?:password|passwd|secret|token|api[_-]?key|authorization)\b\s*[:=]\s*[^\s|,;]+"
)
PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d[ \t().-]*){10,15}(?!\d)")
LONG_DIGITS_RE = re.compile(r"\d{7,}")
LETTER_RE = re.compile(r"[^\W\d_]", re.UNICODE)
SAFE_RECEIPT_EXTENSIONS = {".jpg", ".jpeg", ".png", ".pdf", ".webp"}


@dataclass
class SourceData:
    point_adjustments: list[dict[str, Any]]
    point_notes: list[dict[str, Any]] = field(default_factory=list)
    point_reimbursements: list[dict[str, Any]] = field(default_factory=list)
    reimbursement_receipts: list[dict[str, Any]] = field(default_factory=list)
    receipt_metadata: list[dict[str, Any]] = field(default_factory=list)
    source_columns: dict[str, list[str]] = field(default_factory=dict)


def _stable_map(values: Iterable[Any], prefix: str, *, start: int = 1) -> dict[Any, str]:
    distinct = sorted({value for value in values if value is not None}, key=lambda item: str(item))
    width = max(6, len(str(start + len(distinct))))
    return {
        value: f"{prefix}{index:0{width}d}"
        for index, value in enumerate(distinct, start=start)
    }


def _integer_map(values: Iterable[Any], *, start: int = 100001) -> dict[Any, int]:
    distinct = sorted({value for value in values if value is not None}, key=lambda item: str(item))
    return {value: index for index, value in enumerate(distinct, start=start)}


def sanitize_structured_text(value: Any) -> str | None:
    """Remove payload text while retaining migration-relevant separators."""
    if value is None:
        return None
    result = str(value)
    result = CONNECTION_RE.sub("<CONNECTION>", result)
    result = URL_RE.sub("<URL>", result)
    result = EMAIL_RE.sub("<EMAIL>", result)
    result = SECRET_RE.sub("<SECRET>", result)
    result = PHONE_RE.sub("<PHONE>", result)
    result = LONG_DIGITS_RE.sub("<NUMBER>", result)
    return LETTER_RE.sub("x", result)


def _receipt_extension(path: str) -> str:
    without_query = re.split(r"[?#]", path, maxsplit=1)[0]
    suffix = Path(without_query).suffix.lower()
    return suffix if suffix in SAFE_RECEIPT_EXTENSIONS else ".bin"


def _extension_for_content_type(content_type: Any) -> str:
    return {
        "application/pdf": ".pdf",
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/webp": ".webp",
    }.get(str(content_type or "").lower(), ".bin")


class ReceiptPathSanitizer:
    def __init__(self, file_ids: Iterable[Any]):
        self.file_id_map = _stable_map(file_ids, "receipt_")
        self.path_map: dict[str, str] = {}

    def _token_for_path(self, raw_path: str) -> str:
        if raw_path in self.path_map:
            return self.path_map[raw_path]
        token = None
        normalized = raw_path.replace("\\", "/")
        match = re.search(r"(?:^|/)receipts/([^/]+)/", normalized, re.IGNORECASE)
        if match:
            token = self.file_id_map.get(match.group(1))
        if token is None:
            token = f"receipt_{len(self.file_id_map) + len(self.path_map) + 1:06d}"
        safe_path = f"receipts/{token}/receipt{_receipt_extension(raw_path)}"
        self.path_map[raw_path] = safe_path
        return safe_path

    def sanitize(self, value: Any) -> str | None:
        if value is None:
            return None
        chunks = re.split(r"(\|)", str(value))
        sanitized: list[str] = []
        for chunk in chunks:
            if chunk == "|":
                sanitized.append(chunk)
                continue
            leading = chunk[: len(chunk) - len(chunk.lstrip())]
            trailing = chunk[len(chunk.rstrip()) :]
            payload = chunk.strip()
            sanitized.append(
                f"{leading}{self._token_for_path(payload) if payload else ''}{trailing}"
            )
        return "".join(sanitized)


def _legacy_key(row_id: Any, kind: str, index: int, payload: str) -> str:
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"point_adjustments:{row_id}:{kind}:{index}:{digest}"


def _legacy_key_mapping(
    point_adjustments: list[dict[str, Any]],
    adjustment_id_map: dict[Any, int],
    receipt_paths: ReceiptPathSanitizer,
) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in point_adjustments:
        new_id = adjustment_id_map[row["id"]]
        for kind, field_name in (
            ("note", "note_comment"),
            ("reimbursement", "reimb_comment"),
        ):
            original_lines = [
                line.strip()
                for line in str(row.get(field_name) or "").splitlines()
                if line.strip()
            ]
            sanitized_lines = [
                line.strip()
                for line in str(sanitize_structured_text(row.get(field_name)) or "").splitlines()
                if line.strip()
            ]
            for index, (original, sanitized) in enumerate(
                zip(original_lines, sanitized_lines, strict=True)
            ):
                result[_legacy_key(row["id"], kind, index, original)] = _legacy_key(
                    new_id, kind, index, sanitized
                )
        original_paths = [
            part.strip()
            for part in str(row.get("reimb_receipt") or "").split("|")
            if part.strip()
        ]
        sanitized_paths = [
            part.strip()
            for part in str(receipt_paths.sanitize(row.get("reimb_receipt")) or "").split("|")
            if part.strip()
        ]
        for index, (original, sanitized) in enumerate(
            zip(original_paths, sanitized_paths, strict=True)
        ):
            result[_legacy_key(row["id"], "receipt", index, original)] = _legacy_key(
                new_id, "receipt", index, sanitized
            )
    return result


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (date, datetime)):
        value = value.isoformat()
    return "'" + str(value).replace("'", "''") + "'"


def _insert(table: str, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> str:
    if not rows:
        return f"-- {table}: 0 rows\n"
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    values = ",\n".join(
        "    (" + ", ".join(_sql_literal(row.get(column)) for column in columns) + ")"
        for row in rows
    )
    return f'INSERT INTO "{table}" ({quoted_columns}) VALUES\n{values};\n'


def build_snapshot_sql(source: SourceData) -> tuple[str, dict[str, int]]:
    merchant_values: list[Any] = []
    point_values: list[Any] = []
    for table in (
        source.point_adjustments,
        source.point_notes,
        source.point_reimbursements,
        source.receipt_metadata,
    ):
        merchant_values.extend(row.get("merchant_id") for row in table)
    for table in (
        source.point_adjustments,
        source.point_notes,
        source.point_reimbursements,
    ):
        point_values.extend(row.get("point_code") for row in table)

    merchant_map = _integer_map(merchant_values)
    point_map = _stable_map(point_values, "POINT_")
    adjustment_id_map = {
        row["id"]: index
        for index, row in enumerate(
            sorted(source.point_adjustments, key=lambda row: str(row["id"])), start=1
        )
    }
    note_id_map = _stable_map((row.get("id") for row in source.point_notes), "note_")
    reimbursement_id_map = _stable_map(
        (row.get("id") for row in source.point_reimbursements), "reimbursement_"
    )
    reimbursement_receipt_id_map = _stable_map(
        (row.get("id") for row in source.reimbursement_receipts), "receipt_link_"
    )
    receipt_paths = ReceiptPathSanitizer(
        row.get("file_id") for row in source.receipt_metadata
    )
    known_legacy_keys = _legacy_key_mapping(
        source.point_adjustments, adjustment_id_map, receipt_paths
    )
    unknown_legacy_values = [
        row.get("legacy_key")
        for table in (
            source.point_notes,
            source.point_reimbursements,
            source.reimbursement_receipts,
        )
        for row in table
        if row.get("legacy_key") is not None
        and row.get("legacy_key") not in known_legacy_keys
    ]
    unknown_legacy_map = _stable_map(unknown_legacy_values, "legacy_existing_")

    def mapped_legacy_key(value: Any) -> str | None:
        if value is None:
            return None
        return known_legacy_keys.get(value) or unknown_legacy_map[value]

    point_adjustments = []
    for row in sorted(source.point_adjustments, key=lambda item: str(item["id"])):
        point_adjustments.append(
            {
                "id": adjustment_id_map[row["id"]],
                "merchant_id": merchant_map[row["merchant_id"]],
                "point_code": point_map[row["point_code"]],
                "month_key": row["month_key"],
                "note_amount": row.get("note_amount") or 0,
                "note_comment": sanitize_structured_text(row.get("note_comment")),
                "reimb_amount": row.get("reimb_amount") or 0,
                "reimb_comment": sanitize_structured_text(row.get("reimb_comment")),
                "reimb_receipt": receipt_paths.sanitize(row.get("reimb_receipt")),
            }
        )

    def normalized_rows(
        rows: list[dict[str, Any]], id_map: dict[Any, str]
    ) -> list[dict[str, Any]]:
        result = []
        for row in rows:
            result.append(
                {
                    "id": id_map[row["id"]],
                    "merchant_id": merchant_map[row["merchant_id"]],
                    "point_code": point_map[row["point_code"]],
                    "month_key": row["month_key"],
                    "amount": row["amount"],
                    "comment": sanitize_structured_text(row.get("comment")) or "x",
                    "legacy_key": mapped_legacy_key(row.get("legacy_key")),
                    "created_at": row.get("created_at"),
                }
            )
        return result

    point_notes = normalized_rows(source.point_notes, note_id_map)
    point_reimbursements = normalized_rows(
        source.point_reimbursements, reimbursement_id_map
    )
    reimbursement_receipts = []
    for row in source.reimbursement_receipts:
        reimbursement_receipts.append(
            {
                "id": reimbursement_receipt_id_map[row["id"]],
                "reimbursement_id": reimbursement_id_map[row["reimbursement_id"]],
                "legacy_path": receipt_paths.sanitize(row.get("legacy_path")) or "",
                "legacy_key": mapped_legacy_key(row.get("legacy_key")),
                "created_at": row.get("created_at"),
            }
        )

    receipt_metadata = []
    for row in source.receipt_metadata:
        file_id = receipt_paths.file_id_map[row["file_id"]]
        extension = _extension_for_content_type(row.get("content_type"))
        receipt_metadata.append(
            {
                "file_id": file_id,
                "safe_extension": extension,
                "content_type": sanitize_structured_text(row.get("content_type")),
                "byte_size": row.get("byte_size"),
                "merchant_id": merchant_map.get(row.get("merchant_id")),
                "created_at": row.get("created_at"),
            }
        )

    counts = {
        "point_adjustments": len(point_adjustments),
        "point_notes": len(point_notes),
        "point_reimbursements": len(point_reimbursements),
        "reimbursement_receipts": len(reimbursement_receipts),
        "receipt_metadata": len(receipt_metadata),
    }
    source_structure = json.dumps(
        source.source_columns, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    header = f"""\\set ON_ERROR_STOP on
\\if :{{?LEGACY_SNAPSHOT_TEST_ONLY}}
\\else
\\echo 'Refusing to load: pass -v LEGACY_SNAPSHOT_TEST_ONLY=on to psql for an isolated test database.'
\\quit
\\endif
\\if :LEGACY_SNAPSHOT_TEST_ONLY
\\else
\\echo 'Refusing to load: LEGACY_SNAPSHOT_TEST_ONLY must be on.'
\\quit
\\endif

-- Anonymized legacy-migration snapshot. Never load into production.
-- Relevant source columns: {source_structure}
-- Excluded: {'; '.join(EXCLUDED_FIELDS)}
BEGIN;

CREATE TABLE point_adjustments (
    id INTEGER PRIMARY KEY,
    merchant_id INTEGER NOT NULL,
    point_code TEXT NOT NULL,
    month_key DATE NOT NULL,
    note_amount INTEGER NOT NULL DEFAULT 0,
    note_comment TEXT,
    reimb_amount INTEGER NOT NULL DEFAULT 0,
    reimb_comment TEXT,
    reimb_receipt TEXT,
    UNIQUE (merchant_id, point_code, month_key)
);

CREATE TABLE point_notes (
    id TEXT PRIMARY KEY,
    merchant_id INTEGER NOT NULL,
    point_code TEXT NOT NULL,
    month_key DATE NOT NULL,
    amount INTEGER NOT NULL CHECK (amount <> 0),
    comment TEXT NOT NULL CHECK (length(trim(comment)) > 0),
    legacy_key TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE point_reimbursements (
    id TEXT PRIMARY KEY,
    merchant_id INTEGER NOT NULL,
    point_code TEXT NOT NULL,
    month_key DATE NOT NULL,
    amount INTEGER NOT NULL CHECK (amount > 0),
    comment TEXT NOT NULL CHECK (length(trim(comment)) > 0),
    legacy_key TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE reimbursement_receipts (
    id TEXT PRIMARY KEY,
    reimbursement_id TEXT NOT NULL REFERENCES point_reimbursements(id) ON DELETE CASCADE,
    legacy_path TEXT NOT NULL,
    legacy_key TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Metadata only: there is intentionally no BYTEA column or original filename.
CREATE TABLE receipt_metadata (
    file_id TEXT PRIMARY KEY,
    safe_extension TEXT,
    content_type TEXT,
    byte_size BIGINT,
    merchant_id INTEGER,
    created_at TIMESTAMPTZ
);
"""
    body = "\n".join(
        (
            _insert("point_adjustments", REQUIRED_POINT_ADJUSTMENT_COLUMNS, point_adjustments),
            _insert("point_notes", NORMALIZED_COLUMNS["point_notes"], point_notes),
            _insert(
                "point_reimbursements",
                NORMALIZED_COLUMNS["point_reimbursements"],
                point_reimbursements,
            ),
            _insert(
                "reimbursement_receipts",
                NORMALIZED_COLUMNS["reimbursement_receipts"],
                reimbursement_receipts,
            ),
            _insert(
                "receipt_metadata",
                (
                    "file_id",
                    "safe_extension",
                    "content_type",
                    "byte_size",
                    "merchant_id",
                    "created_at",
                ),
                receipt_metadata,
            ),
        )
    )
    return header + "\n" + body + "\nCOMMIT;\n", counts


def _table_columns(cursor: RealDictCursor, table: str) -> list[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return [row["column_name"] for row in cursor.fetchall()]


def _read_rows(
    cursor: RealDictCursor,
    table: str,
    columns: tuple[str, ...],
    *,
    required: bool = False,
) -> tuple[list[dict[str, Any]], list[str]]:
    available = _table_columns(cursor, table)
    if not available:
        if required:
            raise RuntimeError(f"Required table is missing: {table}")
        return [], []
    missing = [column for column in columns if column not in available]
    if missing:
        raise RuntimeError(f"{table} is missing required columns: {', '.join(missing)}")
    query = sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.Identifier(table),
        sql.Identifier(columns[0]),
    )
    cursor.execute(query)
    return [dict(row) for row in cursor.fetchall()], available


def read_source(database_url: str) -> SourceData:
    connection = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
    try:
        connection.set_session(
            isolation_level=extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            readonly=True,
            autocommit=False,
        )
        with connection.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            if cursor.fetchone()["transaction_read_only"] != "on":
                raise RuntimeError("PostgreSQL did not enable a read-only transaction")

            point_adjustments, point_columns = _read_rows(
                cursor,
                "point_adjustments",
                REQUIRED_POINT_ADJUSTMENT_COLUMNS,
                required=True,
            )
            normalized: dict[str, list[dict[str, Any]]] = {}
            source_columns = {"point_adjustments": point_columns}
            for table, columns in NORMALIZED_COLUMNS.items():
                rows, available = _read_rows(cursor, table, columns)
                normalized[table] = rows
                source_columns[table] = available

            receipt_columns = _table_columns(cursor, "receipt_files")
            receipt_metadata: list[dict[str, Any]] = []
            needed_receipt_columns = {"file_id", "content_type", "data", "created_at"}
            if receipt_columns:
                missing_receipt_columns = needed_receipt_columns.difference(receipt_columns)
                if missing_receipt_columns:
                    raise RuntimeError(
                        "receipt_files is missing required metadata columns: "
                        + ", ".join(sorted(missing_receipt_columns))
                    )
                merchant_expression = (
                    sql.Identifier("merchant_id")
                    if "merchant_id" in receipt_columns
                    else sql.SQL("NULL::INTEGER")
                )
                cursor.execute(
                    sql.SQL(
                        """
                        SELECT file_id, content_type, octet_length(data) AS byte_size,
                               {} AS merchant_id, created_at
                        FROM receipt_files
                        ORDER BY file_id
                        """
                    ).format(merchant_expression)
                )
                receipt_metadata = [dict(row) for row in cursor.fetchall()]
            source_columns["receipt_files"] = receipt_columns
        connection.rollback()
        return SourceData(
            point_adjustments=point_adjustments,
            point_notes=normalized["point_notes"],
            point_reimbursements=normalized["point_reimbursements"],
            reimbursement_receipts=normalized["reimbursement_receipts"],
            receipt_metadata=receipt_metadata,
            source_columns=source_columns,
        )
    finally:
        connection.close()


def privacy_findings(snapshot: str) -> dict[str, int]:
    patterns = {
        "connection_strings": CONNECTION_RE,
        "emails": EMAIL_RE,
        "urls": URL_RE,
        "phone_like_values": PHONE_RE,
        "secret_assignments": SECRET_RE,
        "postgres_bytea_hex": re.compile(r"(?i)\\\\x[0-9a-f]{16,}"),
        "pdf_or_image_signatures": re.compile(r"%PDF-|PNG\\r?\\n|JFIF|Exif|RIFF.{0,8}WEBP"),
        # All source free text is reduced to ASCII "x" placeholders. Any surviving
        # Cyrillic word therefore indicates an unsanitized name or comment.
        "real_name_or_free_text": re.compile(r"[А-Яа-яЁё]{2,}"),
    }
    return {name: len(pattern.findall(snapshot)) for name, pattern in patterns.items()}


def write_snapshot(source: SourceData, output: Path) -> dict[str, Any]:
    snapshot, counts = build_snapshot_sql(source)
    findings = privacy_findings(snapshot)
    # SQL keywords and fixed anonymized labels are ASCII; the name detector ignores
    # the placeholder-only letter "x" but catches any surviving natural-language text.
    disallowed = {name: count for name, count in findings.items() if count}
    if disallowed:
        raise RuntimeError(f"Privacy scan failed: {json.dumps(disallowed, sort_keys=True)}")

    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".legacy-snapshot-{os.getpid()}.tmp")
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            stat.S_IRUSR | stat.S_IWUSR,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(snapshot)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
        os.chmod(output, stat.S_IRUSR | stat.S_IWUSR)
    finally:
        if temporary.exists():
            temporary.unlink()

    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    return {
        "mode": "source-read-only",
        "output": str(output),
        "sha256": digest,
        "row_counts": counts,
        "excluded_fields": list(EXCLUDED_FIELDS),
        "privacy_scan": findings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export an anonymized, test-only legacy migration snapshot"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/tmp/merch-web-legacy-snapshot.sql"),
    )
    args = parser.parse_args()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is not set")
    report = write_snapshot(read_source(database_url), args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
