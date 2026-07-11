"""aiotruenas: asyncio-native Python client for the TrueNAS JSON-RPC WebSocket API."""

from .client import DEFAULT_QUERY_TIMEOUT, TrueNASClient
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

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_QUERY_TIMEOUT",
    "TrueNASClient",
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
