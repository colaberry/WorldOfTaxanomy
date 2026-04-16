"""POST /api/v1/classify - Classify free-text against taxonomy systems."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from world_of_taxonomy.api.deps import get_conn, get_current_user
from world_of_taxonomy.classify import classify_text

router = APIRouter(prefix="/api/v1", tags=["classify"])


class ClassifyRequest(BaseModel):
    text: str = Field(..., min_length=2, max_length=500, description="Free-text to classify")
    systems: Optional[list[str]] = Field(
        None,
        description="Optional list of system IDs to search. Default: major systems.",
    )
    limit: int = Field(5, ge=1, le=20, description="Max matches per system")


class ClassifyResult(BaseModel):
    code: str
    title: str
    score: float
    level: int


class ClassifySystemMatch(BaseModel):
    system_id: str
    system_name: str
    results: list[ClassifyResult]


class CrosswalkEdge(BaseModel):
    from_: str = Field(..., alias="from")
    to: str
    match_type: str

    model_config = {"populate_by_name": True}


class ClassifyResponse(BaseModel):
    query: str
    matches: list[ClassifySystemMatch]
    crosswalks: list[CrosswalkEdge]
    disclaimer: str
    report_issue_url: str


@router.post("/classify", response_model=ClassifyResponse)
async def classify_business(
    body: ClassifyRequest,
    user: dict = Depends(get_current_user),
    conn=Depends(get_conn),
):
    """Classify a business/product/occupation description against taxonomy systems.

    Requires Pro or Enterprise tier.
    """
    if user.get("tier") not in ("pro", "enterprise"):
        raise HTTPException(
            status_code=403,
            detail=(
                "The classify endpoint requires a Pro or Enterprise tier account. "
                "See /developers for pricing information."
            ),
        )

    result = await classify_text(
        conn,
        text=body.text,
        system_ids=body.systems,
        limit=body.limit,
    )
    return result
