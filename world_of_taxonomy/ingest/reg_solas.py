"""Ingest International Convention for the Safety of Life at Sea (SOLAS 1974)."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_solas",
    "IMO SOLAS",
    "International Convention for the Safety of Life at Sea (SOLAS 1974)",
    "2024",
    "Global",
    "International Maritime Organization (IMO)",
)
_SOURCE_URL = "https://www.imo.org/en/About/Conventions/Pages/International-Convention-for-the-Safety-of-Life-at-Sea-(SOLAS),-1974.aspx"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (IMO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ch_i", "Chapter I - General Provisions", 1, None),
    ("ch_ii_1", "Chapter II-1 - Construction (Subdivision and Stability)", 1, None),
    ("ch_ii_2", "Chapter II-2 - Fire Protection, Detection and Extinction", 1, None),
    ("ch_iii", "Chapter III - Life-Saving Appliances", 1, None),
    ("ch_iv", "Chapter IV - Radiocommunications", 1, None),
    ("ch_v", "Chapter V - Safety of Navigation", 1, None),
    ("ch_vi", "Chapter VI - Carriage of Cargoes", 1, None),
    ("ch_vii", "Chapter VII - Carriage of Dangerous Goods", 1, None),
    ("ch_ix", "Chapter IX - Management for Safe Operation (ISM Code)", 1, None),
    ("ch_xi_1", "Chapter XI-1 - Special Measures to Enhance Maritime Safety", 1, None),
    ("ch_xi_2", "Chapter XI-2 - Special Measures to Enhance Maritime Security (ISPS Code)", 1, None),
    ("ch_xii", "Chapter XII - Additional Safety Measures for Bulk Carriers", 1, None),
    ("ch_xiv", "Chapter XIV - Safety Measures for Polar Code Ships", 1, None),
    ("ism_1", "ISM Code - Safety management objectives", 2, "ch_ix"),
    ("ism_2", "ISM Code - Safety and environmental protection policy", 2, "ch_ix"),
    ("ism_3", "ISM Code - Company responsibilities", 2, "ch_ix"),
    ("ism_4", "ISM Code - Designated Person Ashore", 2, "ch_ix"),
    ("isps_1", "ISPS Code - Ship security assessment", 2, "ch_xi_2"),
    ("isps_2", "ISPS Code - Ship security plan", 2, "ch_xi_2"),
    ("isps_3", "ISPS Code - Port facility security plan", 2, "ch_xi_2"),
    ("isps_4", "ISPS Code - Security levels", 2, "ch_xi_2"),
]


async def ingest_reg_solas(conn) -> int:
    """Insert or update IMO SOLAS system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_solas"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_solas", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_solas",
    )
    return count
