"""Explicit existing-schema maintenance; not a legacy financial migration."""
import argparse
import sys

from app.runtime import maintenance_mode_enabled


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", required=True,
        help="Explicitly authorize existing schema setup and merchant metadata backfill",
    )
    parser.parse_args(argv)
    if maintenance_mode_enabled():
        print("Schema setup refused: maintenance/read-only mode is enabled.", file=sys.stderr)
        return 1
    try:
        # Lazy import: --help and missing --apply never initialize the app/engine.
        from app.main import initialize_application_schema

        initialize_application_schema()
    except Exception:
        # Database exceptions may contain SQL, connection details or credentials.
        print("Schema setup failed. Check the target schema and permissions securely.", file=sys.stderr)
        return 1
    print("SCHEMA_SETUP_COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
