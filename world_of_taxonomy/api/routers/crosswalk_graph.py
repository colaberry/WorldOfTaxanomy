"""Crosswalk graph router - graph visualization data for system pairs."""

from fastapi import APIRouter, Depends, Query

from world_of_taxonomy.api.deps import get_conn
from world_of_taxonomy.api.schemas import CrosswalkGraphResponse
from world_of_taxonomy.query.equivalence import get_crosswalk_graph

router = APIRouter(prefix="/api/v1/systems", tags=["crosswalk-graph"])


@router.get(
    "/{source}/crosswalk/{target}/graph",
    response_model=CrosswalkGraphResponse,
)
async def crosswalk_graph(
    source: str,
    target: str,
    limit: int = Query(500, ge=1, le=5000, description="Max edges to return"),
    conn=Depends(get_conn),
):
    """Get a graph of crosswalk edges between two systems for visualization."""
    data = await get_crosswalk_graph(conn, source, target, limit)
    return CrosswalkGraphResponse(**data)
