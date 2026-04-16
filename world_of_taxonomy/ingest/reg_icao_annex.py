"""Ingest ICAO Annexes to the Convention on International Civil Aviation."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_icao_annex",
    "ICAO Annexes",
    "ICAO Annexes to the Convention on International Civil Aviation",
    "2023",
    "Global",
    "International Civil Aviation Organization (ICAO)",
)
_SOURCE_URL = "https://www.icao.int/safety/SafetyManagement/Pages/SARPs.aspx"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ICAO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ann_1", "Annex 1 - Personnel Licensing", 1, None),
    ("ann_2", "Annex 2 - Rules of the Air", 1, None),
    ("ann_3", "Annex 3 - Meteorological Service", 1, None),
    ("ann_4", "Annex 4 - Aeronautical Charts", 1, None),
    ("ann_5", "Annex 5 - Units of Measurement", 1, None),
    ("ann_6", "Annex 6 - Operation of Aircraft", 1, None),
    ("ann_7", "Annex 7 - Aircraft Nationality and Registration Marks", 1, None),
    ("ann_8", "Annex 8 - Airworthiness of Aircraft", 1, None),
    ("ann_9", "Annex 9 - Facilitation", 1, None),
    ("ann_10", "Annex 10 - Aeronautical Telecommunications", 1, None),
    ("ann_11", "Annex 11 - Air Traffic Services", 1, None),
    ("ann_12", "Annex 12 - Search and Rescue", 1, None),
    ("ann_13", "Annex 13 - Aircraft Accident and Incident Investigation", 1, None),
    ("ann_14", "Annex 14 - Aerodromes", 1, None),
    ("ann_15", "Annex 15 - Aeronautical Information Services", 1, None),
    ("ann_16", "Annex 16 - Environmental Protection", 1, None),
    ("ann_17", "Annex 17 - Security", 1, None),
    ("ann_18", "Annex 18 - Safe Transport of Dangerous Goods by Air", 1, None),
    ("ann_19", "Annex 19 - Safety Management", 1, None),
    ("6_1", "Part I - International Commercial Air Transport", 2, "ann_6"),
    ("6_2", "Part II - International General Aviation", 2, "ann_6"),
    ("6_3", "Part III - International Operations - Helicopters", 2, "ann_6"),
    ("16_1", "Volume I - Aircraft Noise", 2, "ann_16"),
    ("16_2", "Volume II - Aircraft Engine Emissions", 2, "ann_16"),
    ("16_3", "Volume III - Aeroplane CO2 Emissions", 2, "ann_16"),
    ("16_4", "Volume IV - CORSIA", 2, "ann_16"),
]


async def ingest_reg_icao_annex(conn) -> int:
    """Insert or update ICAO Annexes system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_icao_annex"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_icao_annex", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_icao_annex",
    )
    return count
