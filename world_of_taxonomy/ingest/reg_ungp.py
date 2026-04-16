"""Ingest UN Guiding Principles on Business and Human Rights (Ruggie Framework)."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_ungp",
    "UN Guiding Principles",
    "UN Guiding Principles on Business and Human Rights (Ruggie Framework)",
    "2011",
    "Global",
    "United Nations Human Rights Council",
)
_SOURCE_URL = "https://www.ohchr.org/documents/publications/guidingprinciplesbusinesshr_en.pdf"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (UN)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pillar_1", "Pillar I - State Duty to Protect Human Rights", 1, None),
    ("pillar_2", "Pillar II - Corporate Responsibility to Respect", 1, None),
    ("pillar_3", "Pillar III - Access to Remedy", 1, None),
    ("gp_1", "GP 1 - States must protect against human rights abuse", 2, "pillar_1"),
    ("gp_2", "GP 2 - States should set clear expectations for businesses", 2, "pillar_1"),
    ("gp_3", "GP 3 - Regulatory and policy functions", 2, "pillar_1"),
    ("gp_4", "GP 4 - State-business nexus", 2, "pillar_1"),
    ("gp_7", "GP 7 - Conflict-affected areas", 2, "pillar_1"),
    ("gp_10", "GP 10 - Multilateral institutions", 2, "pillar_1"),
    ("gp_11", "GP 11 - Corporate responsibility to respect human rights", 2, "pillar_2"),
    ("gp_13", "GP 13 - Responsibility to avoid causing or contributing to adverse impacts", 2, "pillar_2"),
    ("gp_15", "GP 15 - Policy commitment", 2, "pillar_2"),
    ("gp_17", "GP 17 - Human rights due diligence", 2, "pillar_2"),
    ("gp_18", "GP 18 - Assessing actual and potential human rights impacts", 2, "pillar_2"),
    ("gp_20", "GP 20 - Tracking effectiveness of responses", 2, "pillar_2"),
    ("gp_21", "GP 21 - Communication", 2, "pillar_2"),
    ("gp_22", "GP 22 - Remediation", 2, "pillar_2"),
    ("gp_25", "GP 25 - State-based judicial mechanisms", 2, "pillar_3"),
    ("gp_26", "GP 26 - State-based non-judicial mechanisms", 2, "pillar_3"),
    ("gp_27", "GP 27 - Non-State-based mechanisms", 2, "pillar_3"),
    ("gp_28", "GP 28 - Operational-level grievance mechanisms", 2, "pillar_3"),
    ("gp_31", "GP 31 - Effectiveness criteria for non-judicial mechanisms", 2, "pillar_3"),
]


async def ingest_reg_ungp(conn) -> int:
    """Insert or update UN Guiding Principles system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_ungp"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_ungp", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_ungp",
    )
    return count
