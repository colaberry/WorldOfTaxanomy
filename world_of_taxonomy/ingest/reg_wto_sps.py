"""Ingest WTO Agreement on the Application of Sanitary and Phytosanitary Measures."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_wto_sps",
    "WTO SPS Agreement",
    "WTO Agreement on the Application of Sanitary and Phytosanitary Measures",
    "1995",
    "Global",
    "World Trade Organization (WTO)",
)
_SOURCE_URL = "https://www.wto.org/english/tratop_e/sps_e/spsagr_e.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (WTO)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("art_1", "Article 1 - General Provisions", 1, None),
    ("art_2", "Article 2 - Basic Rights and Obligations", 1, None),
    ("art_3", "Article 3 - Harmonization", 1, None),
    ("art_4", "Article 4 - Equivalence", 1, None),
    ("art_5", "Article 5 - Assessment of Risk and Determination of Level of Protection", 1, None),
    ("art_6", "Article 6 - Adaptation to Regional Conditions", 1, None),
    ("art_7", "Article 7 - Transparency", 1, None),
    ("art_8", "Article 8 - Control, Inspection and Approval Procedures", 1, None),
    ("art_9", "Article 9 - Technical Assistance", 1, None),
    ("art_10", "Article 10 - Special and Differential Treatment", 1, None),
    ("art_11", "Article 11 - Consultations and Dispute Settlement", 1, None),
    ("art_12", "Article 12 - Administration", 1, None),
    ("art_13", "Article 13 - Implementation", 1, None),
    ("ann_a", "Annex A - Definitions", 1, None),
    ("ann_b", "Annex B - Transparency of SPS Regulations", 1, None),
    ("ann_c", "Annex C - Control, Inspection and Approval Procedures", 1, None),
    ("ann_b_1", "Notification requirements", 2, "ann_b"),
    ("ann_b_2", "Publication of regulations", 2, "ann_b"),
    ("ann_b_3", "Enquiry points", 2, "ann_b"),
]


async def ingest_reg_wto_sps(conn) -> int:
    """Insert or update WTO SPS Agreement system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_wto_sps"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_wto_sps", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_wto_sps",
    )
    return count
