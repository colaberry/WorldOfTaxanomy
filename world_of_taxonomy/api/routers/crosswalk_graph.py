"""Crosswalk graph router - graph visualization data for system pairs."""

from typing import Optional

from fastapi import APIRouter, Depends, Query

from world_of_taxonomy.api.deps import get_conn
from world_of_taxonomy.api.schemas import CrosswalkGraphResponse, CrosswalkSectionsResponse
from world_of_taxonomy.query.equivalence import get_crosswalk_graph, get_crosswalk_sections

router = APIRouter(prefix="/api/v1/systems", tags=["crosswalk-graph"])


@router.get(
    "/{source}/crosswalk/{target}/graph",
    response_model=CrosswalkGraphResponse,
)
async def crosswalk_graph(
    source: str,
    target: str,
    limit: int = Query(500, ge=1, le=5000, description="Max edges to return"),
    section: Optional[str] = Query(None, description="Filter to edges within this section code"),
    conn=Depends(get_conn),
):
    """Get a graph of crosswalk edges between two systems for visualization."""
    data = await get_crosswalk_graph(conn, source, target, limit, section=section)
    return CrosswalkGraphResponse(**data)


@router.get(
    "/{source}/crosswalk/{target}/sections",
    response_model=CrosswalkSectionsResponse,
)
async def crosswalk_sections(
    source: str,
    target: str,
    conn=Depends(get_conn),
):
    """Get a section-level summary of crosswalk edges between two systems.

    Returns top-level groupings with edge counts for progressive drill-down.
    """
    data = await get_crosswalk_sections(conn, source, target)
    return CrosswalkSectionsResponse(**data)
