"""Ingest Montreal Protocol on Substances that Deplete the Ozone Layer."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_montreal",
    "Montreal Protocol",
    "Montreal Protocol on Substances that Deplete the Ozone Layer",
    "2016",
    "Global",
    "United Nations Environment Programme (UNEP)",
)
_SOURCE_URL = "https://ozone.unep.org/treaties/montreal-protocol"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (UN)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("art_1", "Article 1 - Definitions", 1, None),
    ("art_2", "Article 2 - Control Measures", 1, None),
    ("art_4", "Article 4 - Trade Control", 1, None),
    ("art_5", "Article 5 - Special Situation of Developing Countries", 1, None),
    ("art_7", "Article 7 - Reporting of Data", 1, None),
    ("art_10", "Article 10 - Financial Mechanism", 1, None),
    ("kigali", "Kigali Amendment (HFCs)", 1, None),
    ("2a", "2A - CFCs", 2, "art_2"),
    ("2b", "2B - Halons", 2, "art_2"),
    ("2c", "2C - Other fully halogenated CFCs", 2, "art_2"),
    ("2d", "2D - Carbon tetrachloride", 2, "art_2"),
    ("2e", "2E - 1,1,1-trichloroethane", 2, "art_2"),
    ("2f", "2F - HCFCs", 2, "art_2"),
    ("2g", "2G - Hydrobromofluorocarbons", 2, "art_2"),
    ("2h", "2H - Methyl bromide", 2, "art_2"),
    ("2j", "2J - HFCs (Kigali)", 2, "art_2"),
    ("k1", "Schedule of HFC phase-down (Article 5 countries)", 2, "kigali"),
    ("k2", "Schedule of HFC phase-down (non-Article 5 countries)", 2, "kigali"),
    ("k3", "HFC baseline calculations", 2, "kigali"),
]


async def ingest_reg_montreal(conn) -> int:
    """Insert or update Montreal Protocol system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_montreal"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_montreal", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_montreal",
    )
    return count
