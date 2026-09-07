import os


def maintenance_mode_enabled() -> bool:
    return os.getenv("MAINTENANCE_MODE", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
