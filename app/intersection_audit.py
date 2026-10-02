"""Read-only intersection comparison. No schema initialization or write mode."""
import argparse
import json
import os
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.services import (
    _calendar_intersection_candidates,
    _valid_intersection_candidates,
    month_end_exclusive,
    month_start,
)


def _key(row):
    return (str(row["point_code"]), str(row["visit_date"]))


def _summary(rows):
    keys = {_key(row) for row in rows}
    return {
        "points": len({point for point, _ in keys}),
        "point_dates": len(keys),
        "calendar_dates": len({day for _, day in keys}),
    }


def build_report(db: Session, year: int, month: int) -> dict:
    """Only SELECTs; call within a read-only snapshot for production data."""
    before = _valid_intersection_candidates(db, year, month)
    calendar = _calendar_intersection_candidates(db, year, month)
    after = before + calendar
    before_pairs = {(*_key(row), row["merchant_id1"], row["merchant_id2"]) for row in before}
    missed = [row for row in calendar
              if (*_key(row), row["merchant_id1"], row["merchant_id2"]) not in before_pairs]
    duplicates = db.execute(text("""
        SELECT merchant_id, point_code, visit_date, slot, COUNT(*) AS row_count
        FROM visits
        WHERE visit_date >= :start AND visit_date < :end
        GROUP BY merchant_id, point_code, visit_date, slot HAVING COUNT(*) > 1
        ORDER BY point_code, visit_date, merchant_id, slot
    """), {"start": month_start(year, month), "end": month_end_exclusive(year, month)}).mappings().all()
    control = [row for row in after if str(row["point_code"]) == "3284"]
    return {
        "mode": "read-only", "period": f"{year}-{month:02d}",
        "before": _summary(before), "after": _summary(after),
        "calendar_exit_intersections": _summary(calendar),
        "slot_pairs_before": len(before), "slot_pairs_after": len(before),
        "slot_point_date_count": len({(*_key(row), row["slot1"]) for row in before}),
        "previously_missed_pairs": missed,
        "new_point_dates": [list(key) for key in sorted({_key(row) for row in after} - {_key(row) for row in before})],
        "preserved_slot_intersections": before,
        "duplicate_groups": [dict(row) for row in duplicates],
        "potential_duplicate_rows": sum(row["row_count"] - 1 for row in duplicates),
        "self_intersections": sum(row["merchant_id1"] == row["merchant_id2"] for row in after),
        "control_3284": {"summary": _summary(control),
                         "dates": sorted({str(row["visit_date"]) for row in control}), "rows": control},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Always enabled; there is no write mode")
    parser.add_argument("--output", type=Path, required=True, help="New local JSON file (not in Git)")
    args = parser.parse_args()
    engine = None
    try:
        month_start(args.year, args.month)
        engine = create_engine(os.environ["DATABASE_URL"], isolation_level="REPEATABLE READ")
        if engine.dialect.name != "postgresql":
            raise ValueError("Production audit requires PostgreSQL")
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.execute(text("SET TRANSACTION READ ONLY"))
                with Session(bind=connection, autoflush=False) as db:
                    report = build_report(db, args.year, args.month)
            finally:
                transaction.rollback()
        with args.output.open("x", encoding="utf-8") as output:
            json.dump(report, output, ensure_ascii=False, indent=2, default=str)
            output.write("\n")
    except Exception:
        # Driver exceptions can contain connection details; never print them.
        parser.exit(1, "Audit failed. Check connection, period and output path; no database writes were requested.\n")
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()
