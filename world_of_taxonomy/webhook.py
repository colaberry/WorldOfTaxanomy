"""Async webhook delivery for lead capture and notifications.

Posts JSON payloads to the LEAD_WEBHOOK_URL environment variable.
If the variable is not set, events are logged and silently skipped.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

LEAD_WEBHOOK_URL = os.environ.get("LEAD_WEBHOOK_URL", "")


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
