"""Unit tests for connection-exception classification (aiotruenas._errors)."""

from __future__ import annotations

import errno
import socket
import ssl

import pytest
from websockets.exceptions import InvalidStatus

from aiotruenas._errors import build_call_error, classify_connect_exception
from aiotruenas.exceptions import (
    TrueNASCallError,
    TrueNASCertificateVerificationError,
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


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _FakeInvalidStatus(InvalidStatus):
    def __init__(self, status_code: int) -> None:
        self.response = _FakeResponse(status_code)

    def __str__(self) -> str:
        return f"HTTP {self.response.status_code}"


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (
            ssl.SSLCertVerificationError("certificate verify failed"),
            TrueNASCertificateVerificationError,
        ),
        (
            socket.gaierror(socket.EAI_NONAME, "Name or service not known"),
            TrueNASHostUnknownError,
        ),
        (
            ConnectionRefusedError(errno.ECONNREFUSED, "refused"),
            TrueNASConnectionRefusedError,
        ),
        (TimeoutError("timed out"), TrueNASHandshakeTimeoutError),
        (OSError(errno.ETIMEDOUT, "timed out"), TrueNASHandshakeTimeoutError),
        (_FakeInvalidStatus(302), TrueNASProxyInterceptedError),
        (_FakeInvalidStatus(401), TrueNASProxyInterceptedError),
        (_FakeInvalidStatus(404), TrueNASEndpointNotFoundError),
        (
            RuntimeError("Plain HTTP request was sent to HTTPS port"),
            TrueNASHttpSchemeError,
        ),
        (RuntimeError("TLSV1_UNRECOGNIZED_NAME"), TrueNASUnsupportedTlsVersionError),
        (RuntimeError("No WebSocket upgrade"), TrueNASWebSocketUnsupportedError),
        (RuntimeError("something completely unrelated"), TrueNASUnknownError),
    ],
)
def test_classify_connect_exception(exc: Exception, expected: type) -> None:
    assert isinstance(classify_connect_exception(exc), expected)


def test_build_call_error_extracts_reason_from_data() -> None:
    error = build_call_error(
        {
            "code": -32000,
            "message": "Call error",
            "data": {
                "error": 22,
                "errname": "EINVAL",
                "reason": "Invalid dataset name",
            },
        }
    )
    assert isinstance(error, TrueNASCallError)
    assert str(error) == "Invalid dataset name"
    assert error.code == -32000
    assert error.errname == "EINVAL"
    assert error.reason == "Invalid dataset name"


def test_build_call_error_falls_back_to_message() -> None:
    error = build_call_error({"code": -32601, "message": "Method not found"})
    assert str(error) == "Method not found"


def test_build_call_error_handles_non_dict_error() -> None:
    error = build_call_error("boom")
    assert str(error) == "boom"
