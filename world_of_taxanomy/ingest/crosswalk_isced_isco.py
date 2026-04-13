"""ISCED 2011 / ISCO-08 crosswalk.

Maps ISCED 2011 education levels to ISCO-08 major groups that typically
require or correspond to those education levels.

Based on UNESCO/ILO joint guidance on education-occupation relationships.
~25 hand-coded edges.

(isced_code, isco_code, match_type, note)
match_type: 'broad' = education level broadly required by this occupation group
"""
from __future__ import annotations

from typing import Optional

# (isced_code, isco_code, match_type, note)
ISCED_ISCO_EDGES: list[tuple[str, str, str, Optional[str]]] = [
    # ISCED 8 (Doctoral) -> ISCO Major Group 2 (Professionals)
    ("ISCED8", "2", "broad", "Doctoral level feeds into professional occupations"),

    # ISCED 7 (Master) -> ISCO Major Group 2 (Professionals)
    ("ISCED7", "2", "broad", "Master level required for many professional roles"),

    # ISCED 7 (Master) -> ISCO Major Group 1 (Managers)
    ("ISCED7", "1", "broad", "Senior management typically requires graduate education"),

    # ISCED 6 (Bachelor) -> ISCO Major Group 2 (Professionals)
    ("ISCED6", "2", "broad", "Bachelor degree entry point for professional occupations"),

    # ISCED 6 (Bachelor) -> ISCO Major Group 1 (Managers)
    ("ISCED6", "1", "broad", "Bachelor level common for managerial positions"),

    # ISCED 5 (Short-cycle Tertiary) -> ISCO Major Group 3 (Technicians)
    ("ISCED5", "3", "broad", "Associate/short-cycle degrees feed into technical roles"),

    # ISCED 5a (Academic short-cycle) -> ISCO Major Group 2
    ("ISCED5a", "2", "narrow", "Academic-oriented short-cycle leads to some professional roles"),

    # ISCED 5b (Professional short-cycle) -> ISCO Major Group 3
    ("ISCED5b", "3", "narrow", "Professionally-oriented short-cycle aligns with technician group"),

    # ISCED 4 (Post-secondary non-tertiary) -> ISCO Major Group 3 (Technicians)
    ("ISCED4", "3", "broad", "Post-secondary vocational feeds into technician and associate roles"),

    # ISCED 4 -> ISCO Major Group 4 (Clerical support)
    ("ISCED4", "4", "broad", "Post-secondary training relevant to clerical/admin occupations"),

    # ISCED 3 (Upper secondary) -> ISCO Major Group 4 (Clerical support)
    ("ISCED3", "4", "broad", "Upper secondary education common for clerical roles"),

    # ISCED 3 -> ISCO Major Group 5 (Service and sales)
    ("ISCED3", "5", "broad", "Upper secondary education common for service and sales roles"),

    # ISCED 3a (General upper secondary) -> ISCO Major Group 3
    ("ISCED3a", "3", "narrow", "General upper secondary can lead to technical roles with further training"),

    # ISCED 3b (Vocational upper secondary) -> ISCO Major Group 7 (Craft/trades)
    ("ISCED3b", "7", "broad", "Vocational upper secondary feeds into craft and related trades"),

    # ISCED 3b -> ISCO Major Group 8 (Plant and machine operators)
    ("ISCED3b", "8", "broad", "Vocational training aligns with plant/machine operator roles"),

    # ISCED 3c (Pre-vocational upper secondary) -> ISCO Major Group 9 (Elementary)
    ("ISCED3c", "9", "broad", "Pre-vocational aligns with elementary occupations entry path"),

    # ISCED 2 (Lower secondary) -> ISCO Major Group 5 (Service and sales)
    ("ISCED2", "5", "broad", "Lower secondary education entry for service/sales occupations"),

    # ISCED 2 -> ISCO Major Group 6 (Skilled agricultural)
    ("ISCED2", "6", "broad", "Lower secondary common for skilled agricultural workers"),

    # ISCED 2 -> ISCO Major Group 9 (Elementary)
    ("ISCED2", "9", "broad", "Lower secondary common for elementary occupation workers"),

    # ISCED 1 (Primary) -> ISCO Major Group 9 (Elementary)
    ("ISCED1", "9", "broad", "Primary education entry for elementary occupations"),

    # ISCED 0 (Early childhood) -> no direct ISCO link (pre-labor market)
    # ISCED 4b (Post-secondary vocational) -> ISCO Major Group 7
    ("ISCED4b", "7", "narrow", "Post-secondary vocational feeds into craft/trade occupations"),

    # ISCED 5b -> ISCO Major Group 7
    ("ISCED5b", "7", "narrow", "Professionally-oriented short-cycle also relevant to trades"),

    # ISCED 6 -> ISCO Major Group 3 (some bachelor-level technical roles)
    ("ISCED6", "3", "narrow", "Bachelor programs in technology lead to some technician roles"),

    # ISCED 8 -> ISCO Major Group 1 (Research and academic leadership)
    ("ISCED8", "1", "narrow", "Doctoral level researchers may move into management roles"),

    # ISCED 2b (Vocational lower secondary) -> ISCO Major Group 8
    ("ISCED2b", "8", "narrow", "Vocational lower secondary aligns with operator occupations"),
]


async def ingest_crosswalk_isced_isco(conn) -> int:
    """Ingest ISCED 2011 / ISCO-08 crosswalk edges.

    Hand-coded based on UNESCO/ILO guidance on education-occupation relationships.
    Returns count of edges inserted.
    """
    rows = [
        ("isced_2011", isced_code, "isco_08", isco_code, match_type, note or "")
        for isced_code, isco_code, match_type, note in ISCED_ISCO_EDGES
    ]

    await conn.executemany(
        """INSERT INTO equivalence
               (source_system, source_code, target_system, target_code, match_type, notes)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
        rows,
    )

    return len(rows)
