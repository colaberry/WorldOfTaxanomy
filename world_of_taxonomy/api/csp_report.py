"""CSP violation report sink.

Accepts POSTs from browsers that enforced our Content-Security-Policy
(or Content-Security-Policy-Report-Only) header. The frontend starts in
report-only mode so we can watch what would be blocked before flipping
to enforcement. Each report increments a bounded-cardinality Prometheus
counter keyed on the violated directive so spikes are visible without
letting an attacker blow up the label set.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Request
from fastapi.responses import Response
from prometheus_client import Counter

_logger = logging.getLogger(__name__)

CSP_REPORTS = Counter(
    "wot_csp_reports_total",
    "CSP violation reports received, labelled by violated directive.",
    ["directive"],
)

# Directives we are willing to label on. Anything else is bucketed as
# "other" so a crafted report can not expand the label cardinality.
_KNOWN_DIRECTIVES = {
    "default-src",
    "script-src",
    "script-src-elem",
    "script-src-attr",
    "style-src",
    "style-src-elem",
    "style-src-attr",
    "img-src",
    "font-src",
    "connect-src",
    "frame-src",
    "frame-ancestors",
    "object-src",
    "media-src",
    "worker-src",
    "manifest-src",
    "base-uri",
    "form-action",
}


def _extract_directive(body: Any) -> str:
    """Pull the effective directive from either CSP reporting format.

    Legacy `application/csp-report` wraps the fields under "csp-report".
    The newer Reporting API (`application/reports+json`) is a list of
    {"type": "csp-violation", "body": {...}} entries.
    """
    directive = ""
    if isinstance(body, dict):
        report = body.get("csp-report")
        if isinstance(report, dict):
            directive = (
                report.get("effective-directive")
                or report.get("violated-directive", "")
            )
    elif isinstance(body, list) and body:
        first = body[0]
        if isinstance(first, dict):
            rb = first.get("body", {})
            if isinstance(rb, dict):
                directive = (
                    rb.get("effectiveDirective")
                    or rb.get("violatedDirective", "")
                )
    # Directives may include a source expression like "script-src 'self'".
    bucket = (directive or "").split(" ", 1)[0].lower().strip()
    return bucket if bucket in _KNOWN_DIRECTIVES else "other"


router = APIRouter(tags=["security"])


@router.post("/api/v1/csp-report", include_in_schema=False)
async def csp_report(request: Request) -> Response:
    """Collect a CSP violation report. Always returns 204."""
    raw: Optional[bytes] = None
    try:
        raw = await request.body()
    except Exception:
        return Response(status_code=204)

    directive = "other"
    if raw:
        try:
            body = json.loads(raw.decode("utf-8", errors="replace"))
            if isinstance(body, (dict, list)):
                directive = _extract_directive(body)
        except (ValueError, TypeError):
            pass

    CSP_REPORTS.labels(directive=directive).inc()
    # Truncate the log body so a broken client can not fill disk.
    preview = (raw or b"")[:1024].decode("utf-8", errors="replace")
    _logger.info("csp-report directive=%s body=%s", directive, preview)
    return Response(status_code=204)
