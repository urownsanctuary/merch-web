import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass
from html import escape
from urllib.parse import urlencode

from fastapi import HTTPException, Request


SESSION_COOKIE = "merch_session"
ADMIN_COOKIE = "merch_admin_session"
SESSION_TTL_SECONDS = 12 * 60 * 60


def _secret() -> bytes:
    value = os.getenv("SESSION_SECRET") or os.getenv("SECRET_SALT")
    if not value or len(value) < 24:
        raise RuntimeError("SESSION_SECRET (or SECRET_SALT) must contain at least 24 characters")
    return value.encode("utf-8")


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _sign(encoded_payload: str) -> str:
    return hmac.new(_secret(), encoded_payload.encode("ascii"), hashlib.sha256).hexdigest()


def make_session(subject: str, role: str) -> str:
    payload = {
        "sub": subject,
        "role": role,
        "exp": int(time.time()) + SESSION_TTL_SECONDS,
        "csrf": secrets.token_urlsafe(24),
    }
    encoded = _b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    return f"{encoded}.{_sign(encoded)}"


def read_session(token: str | None, expected_role: str) -> dict | None:
    if not token:
        return None
    try:
        encoded, signature = token.rsplit(".", 1)
        if not hmac.compare_digest(signature, _sign(encoded)):
            return None
        payload = json.loads(_b64decode(encoded))
        if payload.get("role") != expected_role or int(payload.get("exp", 0)) < int(time.time()):
            return None
        if not payload.get("sub") or not payload.get("csrf"):
            return None
        return payload
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def require_merchant(request: Request) -> dict:
    session = read_session(request.cookies.get(SESSION_COOKIE), "merchant")
    if not session:
        raise HTTPException(status_code=401, detail="Authentication required")
    return session


def require_admin(request: Request) -> dict:
    session = read_session(request.cookies.get(ADMIN_COOKIE), "admin")
    if not session:
        raise HTTPException(status_code=401, detail="Administrator authentication required")
    return session


def verify_csrf(session: dict, token: str) -> None:
    if not token or not hmac.compare_digest(str(session.get("csrf", "")), token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


def cookie_secure() -> bool:
    return os.getenv("ENVIRONMENT", "production").lower() not in {"development", "dev", "test", "local"}


def safe(value: object) -> str:
    return escape(str(value), quote=True)


def url(path: str, **params: object) -> str:
    return f"{path}?{urlencode({key: str(value) for key, value in params.items()})}" if params else path
