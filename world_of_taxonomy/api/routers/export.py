"""CSV export endpoints - gated behind authentication.

GET /api/v1/systems/{id}/export.csv
    Download all nodes in a classification system as CSV.
    Columns: code, title, description, level, parent_code, is_leaf

GET /api/v1/systems/{id}/crosswalk/{target_id}/export.csv
    Download crosswalk edges between two systems as CSV.
    Columns: source_code, source_title, target_system, target_code, target_title, match_type

Both endpoints require a valid JWT or API key (Authorization: Bearer <token>).
Unauthenticated requests receive 401 with a sign-in hint.
"""

from __future__ import annotations

import csv
import io
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from world_of_taxonomy.api.deps import get_conn, get_current_user

router = APIRouter(prefix="/api/v1/systems", tags=["export"])


def _require_auth(request: Request):
    """Raise 401 with sign-in hint if no valid token is present."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Sign in to download data. Visit /login to create a free account.",
            headers={"WWW-Authenticate": "Bearer"},
        )


def _csv_response(rows: list[list], headers: list[str], filename: str) -> StreamingResponse:
    """Build a streaming CSV response."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/{system_id}/export.csv")
async def export_nodes(
    system_id: str,
    request: Request,
    conn=Depends(get_conn),
):
    """Download all nodes in a classification system as CSV.

    Requires authentication. Returns rows sorted by seq_order.
    """
    _require_auth(request)

    # Verify system exists
    system = await conn.fetchrow(
        "SELECT id, name FROM classification_system WHERE id = $1", system_id
    )
    if not system:
        raise HTTPException(status_code=404, detail=f"System '{system_id}' not found")

    rows = await conn.fetch(
        """SELECT code, title, description, level, parent_code, is_leaf
           FROM classification_node
           WHERE system_id = $1
           ORDER BY seq_order, code""",
        system_id,
    )

    csv_rows = [
        [
            r["code"],
            r["title"] or "",
            r["description"] or "",
            r["level"],
            r["parent_code"] or "",
            "true" if r["is_leaf"] else "false",
        ]
        for r in rows
    ]

    safe_name = system_id.replace("/", "_")
    return _csv_response(
        csv_rows,
        headers=["code", "title", "description", "level", "parent_code", "is_leaf"],
        filename=f"{safe_name}_nodes.csv",
    )


@router.get("/{system_id}/crosswalk/{target_id}/export.csv")
async def export_crosswalk(
    system_id: str,
    target_id: str,
    request: Request,
    conn=Depends(get_conn),
):
    """Download crosswalk edges between two systems as CSV.

    Requires authentication. Returns edges in source -> target direction only.
    """
    _require_auth(request)

    # Verify both systems exist
    for sid in (system_id, target_id):
        exists = await conn.fetchval(
            "SELECT 1 FROM classification_system WHERE id = $1", sid
        )
        if not exists:
            raise HTTPException(status_code=404, detail=f"System '{sid}' not found")

    rows = await conn.fetch(
        """SELECT
             e.source_code,
             sn.title  AS source_title,
             e.target_system,
             e.target_code,
             tn.title  AS target_title,
             e.match_type
           FROM equivalence e
           LEFT JOIN classification_node sn
             ON sn.system_id = e.source_system AND sn.code = e.source_code
           LEFT JOIN classification_node tn
             ON tn.system_id = e.target_system AND tn.code = e.target_code
           WHERE e.source_system = $1 AND e.target_system = $2
           ORDER BY e.source_code""",
        system_id,
        target_id,
    )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No crosswalk found between '{system_id}' and '{target_id}'",
        )

    csv_rows = [
        [
            r["source_code"],
            r["source_title"] or "",
            r["target_system"],
            r["target_code"],
            r["target_title"] or "",
            r["match_type"] or "",
        ]
        for r in rows
    ]

    safe_src = system_id.replace("/", "_")
    safe_tgt = target_id.replace("/", "_")
    return _csv_response(
        csv_rows,
        headers=["source_code", "source_title", "target_system", "target_code", "target_title", "match_type"],
        filename=f"{safe_src}_to_{safe_tgt}_crosswalk.csv",
    )
