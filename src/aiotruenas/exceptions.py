"""Typed exception hierarchy for aiotruenas.

Every failure mode that the client can distinguish is represented as its own
exception type (rather than a string error code), so callers can use
``except TrueNASAuthenticationError`` / ``except TrueNASConnectionError`` etc.
without inspecting error strings.
"""

from __future__ import annotations

from typing import Any, Literal

__all__ = [
    "TrueNASError",
    "TrueNASConnectionError",
    "TrueNASCertificateVerificationError",
    "TrueNASHttpSchemeError",
    "TrueNASUnsupportedTlsVersionError",
    "TrueNASWebSocketUnsupportedError",
    "TrueNASHostUnknownError",
    "TrueNASConnectionRefusedError",
    "TrueNASProxyInterceptedError",
    "TrueNASEndpointNotFoundError",
    "TrueNASTimeoutError",
    "TrueNASHandshakeTimeoutError",
    "TrueNASCallTimeoutError",
    "TrueNASAuthenticationError",
    "TrueNASConnectionClosedError",
    "TrueNASMalformedResponseError",
    "TrueNASCallError",
    "TrueNASUnknownError",
]


class TrueNASError(Exception):
    """Base class for all errors raised by aiotruenas."""


class TrueNASConnectionError(TrueNASError):
    """The WebSocket connection to TrueNAS could not be established or was lost."""


class TrueNASCertificateVerificationError(TrueNASConnectionError):
    """TLS certificate verification failed (see ``verify_ssl``)."""


class TrueNASHttpSchemeError(TrueNASConnectionError):
    """A plain HTTP request was sent to a TLS (``wss://``) port."""


class TrueNASUnsupportedTlsVersionError(TrueNASConnectionError):
    """The server does not support the TLS version offered by the client."""


class TrueNASWebSocketUnsupportedError(TrueNASConnectionError):
    """The server did not upgrade the connection to a WebSocket."""


class TrueNASHostUnknownError(TrueNASConnectionError):
    """DNS resolution for the configured host failed."""


class TrueNASConnectionRefusedError(TrueNASConnectionError):
    """The TCP connection was actively refused (or the host is unreachable)."""


class TrueNASProxyInterceptedError(TrueNASConnectionError):
    """A reverse proxy / SSO portal intercepted the WebSocket handshake.

    Detected via an HTTP redirect (301/302/303/307/308) or an auth rejection
    (401/403) at the handshake, before TrueNAS itself ever saw the request
    (e.g. Cloudflare Access redirecting to a login page).
    """


class TrueNASEndpointNotFoundError(TrueNASConnectionError):
    """The WebSocket endpoint path returned HTTP 404 (wrong path/API version)."""


class TrueNASTimeoutError(TrueNASError):
    """Base class for timeouts (handshake or call)."""


class TrueNASHandshakeTimeoutError(TrueNASTimeoutError, TrueNASConnectionError):
    """The WebSocket handshake did not complete in time.

    TrueNAS may briefly hold a connection slot open after a clean disconnect
    (e.g. during an integration reload); the client already retries once
    after a short delay before raising this.
    """


class TrueNASCallTimeoutError(TrueNASTimeoutError):
    """A call (including login) did not receive a matching response in time."""


class TrueNASAuthenticationError(TrueNASError):
    """The API key was rejected by TrueNAS (handshake succeeded, login did not)."""


class TrueNASConnectionClosedError(TrueNASConnectionError):
    """The WebSocket connection was closed while a login or call was in flight."""

    def __init__(self, message: str, *, phase: Literal["login", "call"]) -> None:
        super().__init__(message)
        self.phase = phase


class TrueNASMalformedResponseError(TrueNASError):
    """The server sent a response that did not match the expected shape."""


class TrueNASCallError(TrueNASError):
    """TrueNAS returned a JSON-RPC ``error`` object for a call.

    Attributes mirror the JSON-RPC error object
    (``{"code": ..., "message": ..., "data": {"error": ..., "errname": ...,
    "reason": ...}}``); ``errname``/``reason`` come from ``data`` when present.
    """

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        errname: str | None = None,
        reason: str | None = None,
        data: Any = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.errname = errname
        self.reason = reason
        self.data = data


class TrueNASUnknownError(TrueNASError):
    """Fallback for connection failures that could not be classified."""
