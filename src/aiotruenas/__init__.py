"""aiotruenas: asyncio-native Python client for the TrueNAS JSON-RPC WebSocket API."""

from .client import DEFAULT_QUERY_TIMEOUT, TrueNASClient
from .domain.state import TrueNASState
from .exceptions import (
    TrueNASAuthenticationError,
    TrueNASCallError,
    TrueNASCallTimeoutError,
    TrueNASCertificateVerificationError,
    TrueNASConnectionClosedError,
    TrueNASConnectionError,
    TrueNASConnectionRefusedError,
    TrueNASEndpointNotFoundError,
    TrueNASError,
    TrueNASHandshakeTimeoutError,
    TrueNASHostUnknownError,
    TrueNASHttpSchemeError,
    TrueNASMalformedResponseError,
    TrueNASProxyInterceptedError,
    TrueNASTimeoutError,
    TrueNASUnknownError,
    TrueNASUnsupportedTlsVersionError,
    TrueNASWebSocketUnsupportedError,
)

__version__ = "1.4.1"

__all__ = [
    "DEFAULT_QUERY_TIMEOUT",
    "TrueNASClient",
    "TrueNASState",
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
