"""Classification engine - match free-text against taxonomy systems.

Phase 1: PostgreSQL full-text search using the existing search_vector
(tsvector) on classification_node. Fast, deterministic, zero cost per query.
"""

from __future__ import annotations

from typing import Optional

# Default systems to search when none specified
DEFAULT_SYSTEMS = [
    'naics_2022', 'isic_rev4', 'nace_rev2', 'hs_2022',
    'soc_2018', 'isco_08', 'cpc_v21', 'icd10cm', 'icd_11',
    'unspsc_v24', 'esco_occupations', 'esco_skills',
    'sic_1987', 'anzsic_2006', 'anzsco_2022',
]

DISCLAIMER = (
    "Results are informational only and not guaranteed to be accurate "
    "or complete. Use at your own risk. For authoritative codes, "
    "consult the official source."
)

REPORT_ISSUE_URL = (
    "https://github.com/colaberry/WorldOfTaxonomy/issues/new"
    "?template=data_issue.yml&labels=data-issue"
)


async def classify_text(
    conn,
    text: str,
    system_ids: Optional[list[str]] = None,
    limit: int = 5,
) -> dict:
    """Classify free-text against classification systems using full-text search.

    Returns top matches per system with relevance scores.
    """
    target_systems = system_ids or DEFAULT_SYSTEMS

    # Validate limit
    limit = max(1, min(limit, 20))

    results = []
    for sys_id in target_systems:
        rows = await conn.fetch(
            """
            SELECT code, title, level,
                   ts_rank_cd(search_vector, plainto_tsquery('english', $2)) AS score
            FROM classification_node
            WHERE system_id = $1
              AND search_vector @@ plainto_tsquery('english', $2)
            ORDER BY score DESC
            LIMIT $3
            """,
            sys_id,
            text,
            limit,
        )

        if rows:
            # Fetch system name
            sys_row = await conn.fetchrow(
                "SELECT name FROM classification_system WHERE id = $1",
                sys_id,
            )
            sys_name = sys_row["name"] if sys_row else sys_id

            results.append({
                "system_id": sys_id,
                "system_name": sys_name,
                "results": [
                    {
                        "code": r["code"],
                        "title": r["title"],
                        "score": round(float(r["score"]), 4),
                        "level": r["level"],
                    }
                    for r in rows
                ],
            })

    # Fetch crosswalk edges between top results
    crosswalks = []
    if len(results) >= 2:
        # Collect all top codes per system
        code_map: dict[str, list[str]] = {}
        for match in results:
            code_map[match["system_id"]] = [
                r["code"] for r in match["results"][:3]
            ]

        sys_ids = list(code_map.keys())
        for i, sys_a in enumerate(sys_ids):
            for sys_b in sys_ids[i + 1:]:
                codes_a = code_map[sys_a]
                codes_b = code_map[sys_b]
                edges = await conn.fetch(
                    """
                    SELECT source_system, source_code, target_system,
                           target_code, match_type
                    FROM equivalence
                    WHERE (source_system = $1 AND source_code = ANY($2::text[])
                           AND target_system = $3 AND target_code = ANY($4::text[]))
                       OR (source_system = $3 AND source_code = ANY($4::text[])
                           AND target_system = $1 AND target_code = ANY($2::text[]))
                    LIMIT 10
                    """,
                    sys_a,
                    codes_a,
                    sys_b,
                    codes_b,
                )
                for e in edges:
                    crosswalks.append({
                        "from": f"{e['source_system']}:{e['source_code']}",
                        "to": f"{e['target_system']}:{e['target_code']}",
                        "match_type": e["match_type"],
                    })

    return {
        "query": text,
        "matches": results,
        "crosswalks": crosswalks,
        "disclaimer": DISCLAIMER,
        "report_issue_url": REPORT_ISSUE_URL,
    }
