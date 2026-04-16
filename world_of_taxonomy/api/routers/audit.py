"""Audit router - /api/v1/audit endpoints for data provenance review."""

from fastapi import APIRouter, Depends

from world_of_taxonomy.api.deps import get_conn
from world_of_taxonomy.api.schemas import AuditProvenanceResponse, SystemResponse
from world_of_taxonomy.query.provenance import get_audit_report
from world_of_taxonomy.query.browse import _row_to_system

router = APIRouter(prefix="/api/v1/audit", tags=["audit"])


@router.get("/provenance", response_model=AuditProvenanceResponse)
async def audit_provenance(conn=Depends(get_conn)):
    """Aggregate audit report for data trustworthiness review.

    Returns provenance tier breakdown, systems missing file hashes,
    structural derivation accounting, and skeleton system detection.
    """
    report = await get_audit_report(conn)
    return AuditProvenanceResponse(
        total_systems=report["total_systems"],
        total_nodes=report["total_nodes"],
        provenance_tiers=report["provenance_tiers"],
        official_missing_hash=[
            SystemResponse(**_row_to_system(r).__dict__)
            for r in report["official_missing_hash"]
        ],
        structural_derivation_count=report["structural_derivation_count"],
        structural_derivation_nodes=report["structural_derivation_nodes"],
        skeleton_systems=[
            SystemResponse(**_row_to_system(r).__dict__)
            for r in report["skeleton_systems"]
        ],
    )
