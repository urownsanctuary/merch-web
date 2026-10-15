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
_request_merchant_id: ContextVar[int | None] = ContextVar("request_merchant_id", default=None)
_request_session_time: ContextVar[float] = ContextVar("request_session_time", default=float("inf"))
_request_auth_state: ContextVar[dict | None] = ContextVar("request_auth_state", default=None)


def _secret() -> bytes:
    value = os.getenv("SESSION_SECRET") or os.getenv("SECRET_SALT")
    if not value or len(value) < 24:
        raise RuntimeError("SESSION_SECRET (or SECRET_SALT) must contain at least 24 characters")
    return value.encode()


def create_merchant_session(fio_norm: str, merchant_id: int) -> str:
    if type(merchant_id) is not int or merchant_id <= 0:
        raise ValueError('A positive merchant_id is required')
    payload = {
        "sub": fio_norm,
        "merchant_id": merchant_id,
        "iat": time.time(),
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
        if type(payload.get('merchant_id')) is not int or payload['merchant_id'] <= 0:
            return None
        if int(payload.get("exp", 0)) < int(time.time()) or not payload.get("sub"):
            return None
        return payload
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def set_request_merchant(fio_norm: str | None, issued_at: float = float("inf"), merchant_id: int | None = None):
    return (_request_merchant.set(fio_norm), _request_session_time.set(issued_at), _request_merchant_id.set(merchant_id), _request_auth_state.set({}))


def reset_request_merchant(token) -> None:
    _request_merchant.reset(token[0])
    _request_session_time.reset(token[1])
    _request_merchant_id.reset(token[2])
    _request_auth_state.reset(token[3])


def invalidate_merchant_session():
    state = _request_auth_state.get()
    if state is not None:
        state['invalid'] = True


def merchant_session_invalid():
    return bool((_request_auth_state.get() or {}).get('invalid'))


def request_session_issued_at() -> float:
    return _request_session_time.get()


def request_merchant_matches(fio_norm: str) -> bool:
    current = _request_merchant.get()
    return bool(current) and hmac.compare_digest((current or "").encode(), fio_norm.encode())


def verify_csrf(session: dict | None, value: str) -> bool:
    return bool(session and value) and hmac.compare_digest(str(session.get("csrf", "")), value)


def secure_cookie() -> bool:
    return os.getenv("ENVIRONMENT", "production").lower() not in {"dev", "development", "test", "local"}


def request_merchant_id():
    return _request_merchant_id.get()


def sign_visit_action(payload: dict) -> str:
    payload = {**payload, "expires": int(time.time()) + 3600}
    encoded = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()
    signature = hmac.new(_secret(), ("visit:" + encoded).encode(), hashlib.sha256).hexdigest()
    return encoded + "." + signature


def read_visit_action(token: str, csrf: str) -> dict | None:
    try:
        encoded, signature = token.rsplit(".", 1)
        expected = hmac.new(_secret(), ("visit:" + encoded).encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return None
        payload = json.loads(base64.urlsafe_b64decode(encoded))
        if payload["expires"] < time.time() or not hmac.compare_digest(payload["csrf"], csrf):
            return None
        return payload
    except (ValueError, TypeError, KeyError):
        return None
