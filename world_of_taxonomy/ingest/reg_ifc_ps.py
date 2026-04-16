"""Ingest IFC Performance Standards on Environmental and Social Sustainability."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_ifc_ps",
    "IFC Performance Standards",
    "IFC Performance Standards on Environmental and Social Sustainability",
    "2012",
    "Global",
    "International Finance Corporation (IFC / World Bank Group)",
)
_SOURCE_URL = "https://www.ifc.org/en/insights-reports/2012/ifc-performance-standards"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (World Bank)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ps1", "PS1 - Assessment and Management of E&S Risks and Impacts", 1, None),
    ("ps2", "PS2 - Labor and Working Conditions", 1, None),
    ("ps3", "PS3 - Resource Efficiency and Pollution Prevention", 1, None),
    ("ps4", "PS4 - Community Health, Safety and Security", 1, None),
    ("ps5", "PS5 - Land Acquisition and Involuntary Resettlement", 1, None),
    ("ps6", "PS6 - Biodiversity Conservation and Sustainable Management of Living Natural Resources", 1, None),
    ("ps7", "PS7 - Indigenous Peoples", 1, None),
    ("ps8", "PS8 - Cultural Heritage", 1, None),
    ("1_1", "Social and environmental assessment and management system", 2, "ps1"),
    ("1_2", "Identification of risks and impacts", 2, "ps1"),
    ("1_3", "Management programmes", 2, "ps1"),
    ("1_4", "Stakeholder engagement", 2, "ps1"),
    ("2_1", "Working conditions and management of worker relationships", 2, "ps2"),
    ("2_2", "Occupational health and safety", 2, "ps2"),
    ("2_3", "Supply chain labor", 2, "ps2"),
    ("3_1", "Resource efficiency (greenhouse gases, water, waste)", 2, "ps3"),
    ("3_2", "Pollution prevention", 2, "ps3"),
    ("5_1", "Displacement and resettlement", 2, "ps5"),
    ("5_2", "Economic displacement", 2, "ps5"),
    ("6_1", "Protection and conservation of biodiversity", 2, "ps6"),
    ("6_2", "Management of living natural resources", 2, "ps6"),
]


async def ingest_reg_ifc_ps(conn) -> int:
    """Insert or update IFC Performance Standards system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_ifc_ps"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_ifc_ps", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_ifc_ps",
    )
    return count
