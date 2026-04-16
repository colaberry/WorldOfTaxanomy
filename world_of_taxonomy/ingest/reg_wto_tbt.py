"""Ingest WTO Agreement on Technical Barriers to Trade."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_wto_tbt",
    "WTO TBT Agreement",
    "WTO Agreement on Technical Barriers to Trade",
    "1995",
    "Global",
    "World Trade Organization (WTO)",
)
_SOURCE_URL = "https://www.wto.org/english/tratop_e/tbt_e/tbt_e.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (WTO)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("art_2", "Article 2 - Technical Regulations by Central Government Bodies", 1, None),
    ("art_3", "Article 3 - Technical Regulations by Local Government Bodies", 1, None),
    ("art_4", "Article 4 - Conformity Assessment Procedures (Preparation, Adoption, Application)", 1, None),
    ("art_5", "Article 5 - Conformity Assessment Procedures by Central Government Bodies", 1, None),
    ("art_6", "Article 6 - Recognition of Conformity Assessment by Central Government Bodies", 1, None),
    ("art_7", "Article 7 - Procedures for Assessment of Conformity by Local Government Bodies", 1, None),
    ("art_8", "Article 8 - Procedures for Assessment by Non-Governmental Bodies", 1, None),
    ("art_9", "Article 9 - International and Regional Systems", 1, None),
    ("art_10", "Article 10 - Information about Technical Regulations", 1, None),
    ("art_11", "Article 11 - Technical Assistance to Other Members", 1, None),
    ("art_12", "Article 12 - Special and Differential Treatment", 1, None),
    ("art_13", "Article 13 - TBT Committee", 1, None),
    ("ann_1", "Annex 1 - Definitions", 1, None),
    ("ann_2", "Annex 2 - Technical Expert Groups", 1, None),
    ("ann_3", "Annex 3 - Code of Good Practice for Standards", 1, None),
    ("ann_3_1", "Acceptance of Code of Good Practice", 2, "ann_3"),
    ("ann_3_2", "Obligations under the Code", 2, "ann_3"),
]


async def ingest_reg_wto_tbt(conn) -> int:
    """Insert or update WTO TBT Agreement system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_wto_tbt"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_wto_tbt", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_wto_tbt",
    )
    return count
