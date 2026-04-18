"""Async webhook delivery for lead capture and notifications.

Posts JSON payloads to the LEAD_WEBHOOK_URL environment variable.
If the variable is not set, events are logged and silently skipped.

SSRF hardening: the webhook URL is validated once, at import time,
against an allowlist of schemes + hosts. An attacker who gained a way
to mutate LEAD_WEBHOOK_URL at runtime could otherwise point it at
internal services like http://169.254.169.254 (cloud metadata) or
http://localhost:5432. We refuse those targets up front.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import socket
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Allowlisted schemes. No http:// in production; HTTPS only.
_ALLOWED_SCHEMES = {"https"}
# Optional allowlist of fully-qualified hostnames. When set, the webhook
# host must match one of these. Comma-separated in the env.
_HOST_ALLOWLIST = {
    h.strip().lower()
    for h in os.getenv("WEBHOOK_HOST_ALLOWLIST", "").split(",")
    if h.strip()
}
# Escape hatch for dev + tests: set WEBHOOK_ALLOW_HTTP=true to accept
# http:// URLs. Production deployments must leave this unset.
if os.getenv("WEBHOOK_ALLOW_HTTP", "").lower() in ("1", "true", "yes"):
    _ALLOWED_SCHEMES = {"http", "https"}


def _is_internal_ip(host: str) -> bool:
    """True for loopback, link-local, private, or metadata-service IPs.

    Resolves the hostname to its A/AAAA records and classifies every
    result; any internal-looking answer fails the check. This closes
    the DNS-rebind + explicit-IP variants at once.
    """
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        # If DNS fails we let the HTTP stack surface the error; do not
        # block on a transient lookup miss.
        return False
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if (
            ip.is_loopback
            or ip.is_link_local
            or ip.is_private
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            return True
        # AWS / GCP / Azure instance metadata service
        if str(ip) in {"169.254.169.254", "fd00:ec2::254"}:
            return True
    return False


def _validate_webhook_url(url: str) -> Optional[str]:
    """Return None if the URL is safe to post to, else a reason string."""
    if not url:
        return "empty URL"
    try:
        parsed = urlparse(url)
    except ValueError:
        return "unparseable URL"
    if parsed.scheme.lower() not in _ALLOWED_SCHEMES:
        return f"scheme {parsed.scheme!r} not allowed"
    host = (parsed.hostname or "").lower()
    if not host:
        return "missing host"
    if _HOST_ALLOWLIST and host not in _HOST_ALLOWLIST:
        return f"host {host!r} not in WEBHOOK_HOST_ALLOWLIST"
    if _is_internal_ip(host):
        return f"host {host!r} resolves to an internal address"
    return None


LEAD_WEBHOOK_URL = os.environ.get("LEAD_WEBHOOK_URL", "")
_WEBHOOK_REJECTION_REASON = _validate_webhook_url(LEAD_WEBHOOK_URL) if LEAD_WEBHOOK_URL else None
if _WEBHOOK_REJECTION_REASON:
    logger.warning(
        "LEAD_WEBHOOK_URL rejected at import: %s. Webhook delivery disabled.",
        _WEBHOOK_REJECTION_REASON,
    )
    LEAD_WEBHOOK_URL = ""


async def send_webhook(event: str, data: dict[str, Any]) -> bool:
    """Post an event to the configured webhook URL.

    Returns True if delivery succeeded, False otherwise.
    Never raises - failures are logged and swallowed.
    """
    if not LEAD_WEBHOOK_URL:
        logger.debug("LEAD_WEBHOOK_URL not set, skipping webhook for event=%s", event)
        return False

    payload = {
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }

    try:
        import httpx

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                LEAD_WEBHOOK_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code < 300:
                logger.info("Webhook delivered: event=%s status=%d", event, resp.status_code)
                return True
            else:
                logger.warning(
                    "Webhook delivery failed: event=%s status=%d body=%s",
                    event, resp.status_code, resp.text[:200],
                )
                return False
    except ImportError:
        # httpx not installed - fall back to urllib
        import json
        import urllib.request
        import urllib.error

        try:
            req = urllib.request.Request(
                LEAD_WEBHOOK_URL,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.info("Webhook delivered (urllib): event=%s status=%d", event, resp.status)
                return resp.status < 300
        except (urllib.error.URLError, OSError) as e:
            logger.warning("Webhook delivery failed (urllib): event=%s error=%s", event, e)
            return False
    except Exception as e:
        logger.warning("Webhook delivery failed: event=%s error=%s", event, e)
        return False
