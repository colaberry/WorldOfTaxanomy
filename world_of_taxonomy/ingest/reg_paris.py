"""Ingest Paris Agreement under the UNFCCC."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_paris",
    "Paris Agreement",
    "Paris Agreement under the UNFCCC",
    "2015",
    "Global",
    "United Nations Framework Convention on Climate Change (UNFCCC)",
)
_SOURCE_URL = "https://unfccc.int/process-and-meetings/the-paris-agreement"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (UN)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("art_2", "Article 2 - Long-Term Goals", 1, None),
    ("art_3", "Article 3 - Nationally Determined Contributions", 1, None),
    ("art_4", "Article 4 - Mitigation", 1, None),
    ("art_5", "Article 5 - REDD+ (Forests)", 1, None),
    ("art_6", "Article 6 - Cooperative Approaches (Carbon Markets)", 1, None),
    ("art_7", "Article 7 - Adaptation", 1, None),
    ("art_8", "Article 8 - Loss and Damage", 1, None),
    ("art_9", "Article 9 - Finance", 1, None),
    ("art_10", "Article 10 - Technology Development and Transfer", 1, None),
    ("art_13", "Article 13 - Enhanced Transparency Framework", 1, None),
    ("art_14", "Article 14 - Global Stocktake", 1, None),
    ("6_2", "Art 6.2 - Internationally Transferred Mitigation Outcomes (ITMOs)", 2, "art_6"),
    ("6_4", "Art 6.4 - Sustainable Development Mechanism", 2, "art_6"),
    ("13_1", "Biennial Transparency Reports", 2, "art_13"),
    ("13_2", "Technical Expert Review", 2, "art_13"),
    ("13_3", "National Inventory Reports", 2, "art_13"),
    ("ndc", "NDC Components", 1, None),
    ("ndc_1", "Mitigation targets and pathways", 2, "ndc"),
    ("ndc_2", "Adaptation communications", 2, "ndc"),
    ("ndc_3", "Finance mobilization", 2, "ndc"),
]


async def ingest_reg_paris(conn) -> int:
    """Insert or update Paris Agreement system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_paris"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_paris", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_paris",
    )
    return count
