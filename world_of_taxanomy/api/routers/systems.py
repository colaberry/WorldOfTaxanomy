"""Systems router - /api/v1/systems endpoints."""

from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from world_of_taxanomy.api.deps import get_conn
from world_of_taxanomy.api.schemas import SystemResponse, SystemDetailResponse, NodeResponse
from world_of_taxanomy.exceptions import SystemNotFoundError
from world_of_taxanomy.query.browse import get_systems, get_system, get_roots, get_systems_for_country

router = APIRouter(prefix="/api/v1/systems", tags=["systems"])


@router.get("")
async def list_systems(
    group_by: Optional[str] = Query(None, description="Group results (e.g. 'region')"),
    country: Optional[str] = Query(None, description="Filter by ISO 3166-1 alpha-2 country code (e.g. DE, PK, MX)"),
    conn=Depends(get_conn),
):
    """List all classification systems, optionally filtered by country or grouped."""
    if country:
        rows = await get_systems_for_country(conn, country)
        return rows

    systems = await get_systems(conn)
    if group_by == "region":
        grouped: Dict[str, List[SystemResponse]] = {}
        for s in systems:
            grouped.setdefault(s.region or "Unknown", []).append(SystemResponse(**s.__dict__))
        return grouped
    return [SystemResponse(**s.__dict__) for s in systems]


@router.get("/{system_id}", response_model=SystemDetailResponse)
async def get_system_detail(system_id: str, conn=Depends(get_conn)):
    """Get a classification system with its root nodes."""
    try:
        system = await get_system(conn, system_id)
    except SystemNotFoundError:
        raise HTTPException(status_code=404, detail=f"System '{system_id}' not found")

    roots = await get_roots(conn, system_id)
    root_responses = [NodeResponse(**r.__dict__) for r in roots]

    return SystemDetailResponse(**system.__dict__, roots=root_responses)
