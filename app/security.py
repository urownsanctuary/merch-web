import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from contextvars import ContextVar


MERCHANT_COOKIE = "merchant_session"
_request_merchant: ContextVar[str | None] = ContextVar("request_merchant", default=None)


def _secret() -> bytes:
    value = os.getenv("SESSION_SECRET") or os.getenv("SECRET_SALT")
    if not value or len(value) < 24:
        raise RuntimeError("SESSION_SECRET (or SECRET_SALT) must contain at least 24 characters")
    return value.encode()


def create_merchant_session(fio_norm: str) -> str:
    payload = {
        "sub": fio_norm,
        "csrf": secrets.token_urlsafe(24),
        "exp": int(time.time()) + 12 * 60 * 60,
    }
    encoded = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).rstrip(b"=").decode()
    signature = hmac.new(_secret(), encoded.encode(), hashlib.sha256).hexdigest()
    return f"{encoded}.{signature}"


def read_merchant_session(token: str | None) -> dict | None:
    try:
        encoded, signature = str(token or "").rsplit(".", 1)
        expected = hmac.new(_secret(), encoded.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return None
        payload = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        if int(payload.get("exp", 0)) < int(time.time()) or not payload.get("sub"):
            return None
        return payload
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def set_request_merchant(fio_norm: str | None):
    return _request_merchant.set(fio_norm)


def reset_request_merchant(token) -> None:
    _request_merchant.reset(token)


def request_merchant_matches(fio_norm: str) -> bool:
    current = _request_merchant.get()
    return bool(current) and hmac.compare_digest((current or "").encode(), fio_norm.encode())


def verify_csrf(session: dict | None, value: str) -> bool:
    return bool(session and value) and hmac.compare_digest(str(session.get("csrf", "")), value)


def secure_cookie() -> bool:
    return os.getenv("ENVIRONMENT", "production").lower() not in {"dev", "development", "test", "local"}
