"""Ingest International Convention for the Prevention of Pollution from Ships (MARPOL 73/78)."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_marpol",
    "IMO MARPOL",
    "International Convention for the Prevention of Pollution from Ships (MARPOL 73/78)",
    "2023",
    "Global",
    "International Maritime Organization (IMO)",
)
_SOURCE_URL = "https://www.imo.org/en/About/Conventions/Pages/International-Convention-for-the-Prevention-of-Pollution-from-Ships-(MARPOL).aspx"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (IMO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ann_i", "Annex I - Oil", 1, None),
    ("ann_ii", "Annex II - Noxious Liquid Substances in Bulk", 1, None),
    ("ann_iii", "Annex III - Harmful Substances Carried in Packaged Form", 1, None),
    ("ann_iv", "Annex IV - Sewage", 1, None),
    ("ann_v", "Annex V - Garbage", 1, None),
    ("ann_vi", "Annex VI - Air Pollution", 1, None),
    ("i_1", "Oil Record Book requirements", 2, "ann_i"),
    ("i_2", "Oil discharge monitoring and control", 2, "ann_i"),
    ("i_3", "Double hull requirements for oil tankers", 2, "ann_i"),
    ("i_4", "Shipboard oil pollution emergency plan", 2, "ann_i"),
    ("ii_1", "Categorization of noxious liquid substances", 2, "ann_ii"),
    ("ii_2", "Discharge criteria and standards", 2, "ann_ii"),
    ("ii_3", "Procedures and arrangements manual", 2, "ann_ii"),
    ("vi_1", "NOx emission limits (Tier I, II, III)", 2, "ann_vi"),
    ("vi_2", "SOx emission limits and fuel oil quality", 2, "ann_vi"),
    ("vi_3", "Emission Control Areas (ECAs)", 2, "ann_vi"),
    ("vi_4", "Energy Efficiency Design Index (EEDI)", 2, "ann_vi"),
    ("vi_5", "Ship Energy Efficiency Management Plan (SEEMP)", 2, "ann_vi"),
    ("vi_6", "Carbon Intensity Indicator (CII)", 2, "ann_vi"),
    ("vi_7", "Energy Efficiency Existing Ship Index (EEXI)", 2, "ann_vi"),
]


async def ingest_reg_marpol(conn) -> int:
    """Insert or update IMO MARPOL system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_marpol"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_marpol", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_marpol",
    )
    return count
