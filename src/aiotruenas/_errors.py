"""Classification of low-level connection exceptions into typed errors.

Kept separate from client.py so the (string-matching) fallback heuristics can
be unit-tested in isolation from the WebSocket I/O.
"""

from __future__ import annotations

import errno
import socket
import ssl
from typing import Any

from websockets.exceptions import InvalidStatus

from .exceptions import (
    TrueNASCertificateVerificationError,
    TrueNASConnectionError,
    TrueNASConnectionRefusedError,
    TrueNASEndpointNotFoundError,
    TrueNASHandshakeTimeoutError,
    TrueNASHostUnknownError,
    TrueNASHttpSchemeError,
    TrueNASProxyInterceptedError,
    TrueNASUnknownError,
    TrueNASUnsupportedTlsVersionError,
    TrueNASWebSocketUnsupportedError,
)

_REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}
_AUTH_REJECTED_STATUS_CODES = {401, 403}


def _status_code(exc: BaseException) -> int | None:
    """Extract an HTTP status code from a ``websockets`` handshake failure."""
    if isinstance(exc, InvalidStatus):
        return exc.response.status_code
    return None


def _classify_by_type(exc: Exception) -> TrueNASConnectionError | None:
    if isinstance(exc, ssl.SSLCertVerificationError):
        return TrueNASCertificateVerificationError(str(exc))

    if isinstance(exc, socket.gaierror):
        if exc.errno in {socket.EAI_NONAME, getattr(socket, "EAI_NODATA", None)}:
            return TrueNASHostUnknownError(str(exc))
        return None

    if isinstance(exc, ConnectionRefusedError):
        return TrueNASConnectionRefusedError(str(exc))

    if isinstance(exc, TimeoutError):
        return TrueNASHandshakeTimeoutError(str(exc))

    if isinstance(exc, OSError) and exc.errno is not None:
        if exc.errno == errno.ECONNREFUSED:
            return TrueNASConnectionRefusedError(str(exc))
        if exc.errno == errno.ETIMEDOUT:
            return TrueNASHandshakeTimeoutError(str(exc))
        if exc.errno == getattr(errno, "EHOSTUNREACH", None):
            return TrueNASConnectionRefusedError(str(exc))

    return None


def _classify_by_status_code(exc: Exception) -> TrueNASConnectionError | None:
    status_code = _status_code(exc)
    is_redirect_or_denied = (
        status_code in _REDIRECT_STATUS_CODES
        or status_code in _AUTH_REJECTED_STATUS_CODES
    )
    if is_redirect_or_denied:
        # A redirect or an auth rejection at the handshake means something
        # other than TrueNAS answered: typically a reverse proxy / SSO portal
        # (e.g. Cloudflare Access) that intercepts the request before it ever
        # reaches TrueNAS. TrueNAS itself upgrades the WebSocket regardless of
        # API key validity and only checks the key afterwards during login.
        return TrueNASProxyInterceptedError(str(exc))
    if status_code == 404:
        return TrueNASEndpointNotFoundError(str(exc))
    return None


# WARNING: deliberately bound to specific text fragments from the websockets/
# ssl/socket stacks as a last-resort fallback when no structured information
# (exception type, HTTP status code) is available. Changes in library
# versions or message localization can break this matching; prefer adding a
# type- or status-code-based rule above when possible.
_TEXT_FRAGMENT_RULES: tuple[tuple[str, type[TrueNASConnectionError]], ...] = (
    ("certificate_verify_failed", TrueNASCertificateVerificationError),
    ("plain http request was sent to https port", TrueNASHttpSchemeError),
    ("tlsv1_unrecognized_name", TrueNASUnsupportedTlsVersionError),
    ("no websocket upgrade", TrueNASWebSocketUnsupportedError),
    ("connection refused", TrueNASConnectionRefusedError),
    ("no route to host", TrueNASConnectionRefusedError),
    ("timed out while waiting for handshake response", TrueNASHandshakeTimeoutError),
)


def _classify_by_text(exc: Exception) -> TrueNASConnectionError | None:
    normalized = str(exc).strip().lower()
    for fragment, error_cls in _TEXT_FRAGMENT_RULES:
        if fragment in normalized:
            return error_cls(str(exc))
    return None


def classify_connect_exception(exc: Exception) -> TrueNASConnectionError:
    """Classify a connection-setup exception into a typed aiotruenas error."""
    for classifier in (_classify_by_type, _classify_by_status_code, _classify_by_text):
        error = classifier(exc)
        if error is not None:
            return error
    return TrueNASUnknownError(str(exc) or type(exc).__name__)


def build_call_error(error: Any) -> Exception:
    """Build a `TrueNASCallError` from a JSON-RPC ``error`` object."""
    from .exceptions import TrueNASCallError

    if not isinstance(error, dict):
        return TrueNASCallError(str(error))

    code = error.get("code")
    message = error.get("message")
    data = error.get("data")
    errname = reason = None
    if isinstance(data, dict):
        errname = data.get("errname")
        reason = data.get("reason")

    text = reason or message or "TrueNAS returned an RPC error"
    return TrueNASCallError(text, code=code, errname=errname, reason=reason, data=data)
