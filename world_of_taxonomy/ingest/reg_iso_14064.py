"""Ingest ISO 14064 Greenhouse Gases - Quantification and Reporting."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_14064",
    "ISO 14064",
    "ISO 14064 Greenhouse Gases - Quantification and Reporting",
    "2018",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/66453.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pt_1", "Part 1 - Organization Level GHG Emissions", 1, None),
    ("pt_2", "Part 2 - Project Level GHG Emissions", 1, None),
    ("pt_3", "Part 3 - Validation and Verification", 1, None),
    ("1_4", "1-4 GHG inventory boundaries", 2, "pt_1"),
    ("1_5", "1-5 Quantification of GHG emissions and removals", 2, "pt_1"),
    ("1_6", "1-6 GHG inventory quality management", 2, "pt_1"),
    ("1_7", "1-7 GHG reporting", 2, "pt_1"),
    ("1_8", "1-8 Organization role in verification activities", 2, "pt_1"),
    ("2_4", "2-4 GHG project requirements", 2, "pt_2"),
    ("2_5", "2-5 Identifying GHG sources, sinks and reservoirs", 2, "pt_2"),
    ("2_6", "2-6 Determining baseline scenario", 2, "pt_2"),
    ("2_7", "2-7 Selecting quantification methodology", 2, "pt_2"),
    ("2_8", "2-8 Monitoring of GHG project", 2, "pt_2"),
    ("2_9", "2-9 Quantifying GHG emission reductions", 2, "pt_2"),
    ("2_10", "2-10 Reporting on the GHG project", 2, "pt_2"),
    ("3_4", "3-4 Validation and verification principles", 2, "pt_3"),
    ("3_5", "3-5 Validation and verification process requirements", 2, "pt_3"),
    ("3_6", "3-6 Assessment of GHG information and systems", 2, "pt_3"),
    ("3_7", "3-7 Evaluating GHG evidence", 2, "pt_3"),
    ("3_8", "3-8 Validation and verification statements", 2, "pt_3"),
]


async def ingest_reg_iso_14064(conn) -> int:
    """Insert or update ISO 14064 system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_14064"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_14064", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_14064",
    )
    return count
