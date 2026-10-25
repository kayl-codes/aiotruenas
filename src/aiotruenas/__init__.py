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
from .models import InterfaceTraffic, NetworkInterface, SystemInfo, SystemStats
from .system import TrueNASSystem

__version__ = "1.7.0"

__all__ = [
    "DEFAULT_QUERY_TIMEOUT",
    "TrueNASClient",
    "TrueNASState",
    "TrueNASSystem",
    "SystemInfo",
    "SystemStats",
    "NetworkInterface",
    "InterfaceTraffic",
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
