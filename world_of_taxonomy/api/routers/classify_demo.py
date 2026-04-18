"""POST /api/v1/classify/demo - Public email-gated classify endpoint.

This is the web-facing classify surface. Anonymous users submit an
email plus a business description; we persist the email as a lead and
return a limited classify result set. The full /api/v1/classify
endpoint remains Pro-tier gated.

The gate will migrate to Zitadel hosted login once the central IdP is
provisioned; until then, an email-only gate is the stopgap.
"""

from __future__ import annotations

import re
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

# RFC 5322-lite: good enough for lead capture without the email-validator dep.
_EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

from world_of_taxonomy.api.deps import get_conn
from world_of_taxonomy.api.text_guard import TextGuardError, guard
from world_of_taxonomy.classify import classify_text

router = APIRouter(prefix="/api/v1", tags=["classify"])

# Demo surface is intentionally narrower than the paid endpoint: five
# high-signal systems, three results each, no cross-system crosswalks.
# Pro/Enterprise users get the full surface via POST /api/v1/classify.
DEMO_SYSTEMS = [
    "naics_2022",
    "isic_rev4",
    "sic_1987",
    "nace_rev2",
    "soc_2018",
]
DEMO_RESULTS_PER_SYSTEM = 3


class ClassifyDemoRequest(BaseModel):
    email: str = Field(..., min_length=3, max_length=320, description="Lead email for follow-up")
    text: str = Field(
        ...,
        min_length=2,
        max_length=500,
        description="Business/product/occupation description",
    )

    @field_validator("email")
    @classmethod
    def _email_is_plausible(cls, v: str) -> str:
        if not _EMAIL_RX.match(v):
            raise ValueError("email must be a valid address")
        return v.strip().lower()


class ClassifyDemoResponse(BaseModel):
    query: str
    matches: list
    disclaimer: str
    report_issue_url: str
    demo: bool = True
    upgrade_cta: str


async def classify_demo_handler(
    body: ClassifyDemoRequest,
    *,
    conn,
    ip_address: Optional[str],
    user_agent: Optional[str],
    referrer: Optional[str],
) -> dict:
    """Insert a lead row, then run classify limited to the demo surface.

    Split from the route handler so tests can exercise the behaviour
    without spinning up a TestClient.
    """
    try:
        clean_text, _ = guard(body.text, max_length=500)
    except TextGuardError as exc:
        raise HTTPException(status_code=400, detail=exc.public_message) from exc

    await conn.execute(
        """
        INSERT INTO classify_lead (email, query_text, ip_address, user_agent, referrer)
        VALUES ($1, $2, $3, $4, $5)
        """,
        body.email,
        clean_text,
        ip_address,
        user_agent,
        referrer,
    )

    result = await classify_text(
        conn,
        text=clean_text,
        system_ids=DEMO_SYSTEMS,
        limit=DEMO_RESULTS_PER_SYSTEM,
    )

    return {
        "query": result["query"],
        "matches": result["matches"],
        "disclaimer": result["disclaimer"],
        "report_issue_url": result["report_issue_url"],
        "demo": True,
        "upgrade_cta": (
            "Want all 1000 systems, cross-system crosswalks, and "
            "programmatic API access? Upgrade to Pro at /pricing."
        ),
    }


@router.post("/classify/demo", response_model=ClassifyDemoResponse)
async def classify_demo(
    body: ClassifyDemoRequest,
    request: Request,
    conn=Depends(get_conn),
):
    """Public classify endpoint gated by email only.

    Returns a limited result set (5 systems, 3 results each). Records
    the email as a lead.
    """
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent")
    ref = request.headers.get("referer")

    return await classify_demo_handler(
        body,
        conn=conn,
        ip_address=ip,
        user_agent=ua,
        referrer=ref,
    )
