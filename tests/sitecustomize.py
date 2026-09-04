"""Propagate pytest loopback DNS isolation into child interpreters.

Product ingest credentials reject localhost and literal loopback IPs, so the
real-child HTTP regressions resolve a dedicated test hostname to 127.0.0.1
without touching the machine hosts file or leaving the loopback adapter.
"""

from __future__ import annotations

import os
import socket


_LOOPBACK_HOST_ENV = "KMTECH_TEST_LOOPBACK_HOST"


def _install_loopback_host_alias() -> None:
    alias = str(os.environ.get(_LOOPBACK_HOST_ENV) or "").strip().rstrip(".").lower()
    if not alias or getattr(socket, "__kmtech_test_loopback_host_isolated__", False):
        return
    original_getaddrinfo = socket.getaddrinfo

    def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        selected = str(host or "").strip().rstrip(".").lower()
        if selected == alias:
            return original_getaddrinfo(
                "127.0.0.1",
                port,
                socket.AF_INET,
                type,
                proto,
                flags,
            )
        return original_getaddrinfo(host, port, family, type, proto, flags)

    socket.getaddrinfo = getaddrinfo
    socket.__kmtech_test_loopback_host_isolated__ = True


if os.environ.get(_LOOPBACK_HOST_ENV, "").strip():
    _install_loopback_host_alias()
