"""Ingest Equator Principles (EP4) - Financial Industry Framework for Environmental and Social Risk."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_equator",
    "Equator Principles",
    "Equator Principles (EP4) - Financial Industry Framework for Environmental and Social Risk",
    "2020",
    "Global",
    "Equator Principles Association",
)
_SOURCE_URL = "https://equator-principles.com/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ep_1", "Principle 1 - Review and Categorisation", 1, None),
    ("ep_2", "Principle 2 - Environmental and Social Assessment", 1, None),
    ("ep_3", "Principle 3 - Applicable Environmental and Social Standards", 1, None),
    ("ep_4", "Principle 4 - Environmental and Social Management System and Action Plan", 1, None),
    ("ep_5", "Principle 5 - Stakeholder Engagement", 1, None),
    ("ep_6", "Principle 6 - Grievance Mechanism", 1, None),
    ("ep_7", "Principle 7 - Independent Review", 1, None),
    ("ep_8", "Principle 8 - Covenants", 1, None),
    ("ep_9", "Principle 9 - Independent Monitoring and Reporting", 1, None),
    ("ep_10", "Principle 10 - Reporting and Transparency", 1, None),
    ("cat_a", "Category A - Projects with potentially significant adverse impacts", 2, "ep_1"),
    ("cat_b", "Category B - Projects with limited adverse impacts", 2, "ep_1"),
    ("cat_c", "Category C - Projects with minimal or no adverse impacts", 2, "ep_1"),
    ("scope", "Scope of Application", 1, None),
    ("s_1", "Project Finance (>USD 10M)", 2, "scope"),
    ("s_2", "Project-Related Corporate Loans (>USD 100M)", 2, "scope"),
    ("s_3", "Bridge Loans", 2, "scope"),
    ("s_4", "Project-Related Refinance and Acquisition Finance", 2, "scope"),
]


async def ingest_reg_equator(conn) -> int:
    """Insert or update Equator Principles system and its nodes. Returns node count."""
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0, source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance, license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute(
        "DELETE FROM classification_node WHERE system_id = $1", "reg_equator"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_equator", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_equator",
    )
    return count
