"""Ingest ISO 14040:2006 Environmental Management - Life Cycle Assessment - Principles and Framework."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_14040",
    "ISO 14040",
    "ISO 14040:2006 Environmental Management - Life Cycle Assessment - Principles and Framework",
    "2006",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/37456.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cl_4", "Clause 4 - LCA Framework", 1, None),
    ("cl_5", "Clause 5 - Goal and Scope Definition", 1, None),
    ("cl_6", "Clause 6 - Life Cycle Inventory Analysis (LCI)", 1, None),
    ("cl_7", "Clause 7 - Life Cycle Impact Assessment (LCIA)", 1, None),
    ("cl_8", "Clause 8 - Life Cycle Interpretation", 1, None),
    ("4_1", "4.1 General principles of LCA", 2, "cl_4"),
    ("4_2", "4.2 Phases of an LCA", 2, "cl_4"),
    ("4_3", "4.3 Key features", 2, "cl_4"),
    ("5_1", "5.1 General", 2, "cl_5"),
    ("5_2", "5.2 Goal of the study", 2, "cl_5"),
    ("5_3", "5.3 Scope of the study", 2, "cl_5"),
    ("5_4", "5.4 Function, functional unit and reference flow", 2, "cl_5"),
    ("5_5", "5.5 System boundary", 2, "cl_5"),
    ("6_1", "6.1 General", 2, "cl_6"),
    ("6_2", "6.2 Data collection", 2, "cl_6"),
    ("6_3", "6.3 Calculation procedures", 2, "cl_6"),
    ("6_4", "6.4 Allocation", 2, "cl_6"),
    ("7_1", "7.1 General", 2, "cl_7"),
    ("7_2", "7.2 Selection of impact categories and classification", 2, "cl_7"),
    ("7_3", "7.3 Characterization", 2, "cl_7"),
    ("7_4", "7.4 Optional elements", 2, "cl_7"),
    ("8_1", "8.1 General", 2, "cl_8"),
    ("8_2", "8.2 Identification of significant issues", 2, "cl_8"),
    ("8_3", "8.3 Evaluation (completeness, sensitivity, consistency checks)", 2, "cl_8"),
    ("8_4", "8.4 Conclusions, limitations and recommendations", 2, "cl_8"),
]


async def ingest_reg_iso_14040(conn) -> int:
    """Insert or update ISO 14040 system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_14040"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_14040", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_14040",
    )
    return count
