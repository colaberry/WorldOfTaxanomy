"""Ingest Heating, Ventilation, and Air Conditioning System Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_hvac_system",
    "HVAC System Types",
    "Heating, Ventilation, and Air Conditioning System Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("hv_heat", "Heating Systems", 1, None),
    ("hv_cool", "Cooling Systems", 1, None),
    ("hv_vent", "Ventilation Systems", 1, None),
    ("hv_heat_furnace", "Forced-air furnace (gas, electric, oil)", 2, "hv_heat"),
    ("hv_heat_boiler", "Boiler systems (hot water, steam)", 2, "hv_heat"),
    ("hv_heat_pump", "Heat pump (air-source, ground-source)", 2, "hv_heat"),
    ("hv_heat_radiant", "Radiant heating (floor, panel)", 2, "hv_heat"),
    ("hv_cool_split", "Split system (central AC)", 2, "hv_cool"),
    ("hv_cool_pkg", "Packaged rooftop unit (RTU)", 2, "hv_cool"),
    ("hv_cool_chiller", "Chiller (air-cooled, water-cooled)", 2, "hv_cool"),
    ("hv_cool_vrf", "Variable refrigerant flow (VRF/VRV)", 2, "hv_cool"),
    ("hv_cool_evap", "Evaporative cooling", 2, "hv_cool"),
    ("hv_vent_mech", "Mechanical ventilation (ERV, HRV)", 2, "hv_vent"),
    ("hv_vent_doas", "Dedicated outdoor air system (DOAS)", 2, "hv_vent"),
    ("hv_vent_nat", "Natural ventilation", 2, "hv_vent"),
    ("hv_vent_filt", "Air filtration (MERV, HEPA)", 2, "hv_vent"),
    ("hv_vent_uvgi", "UV germicidal irradiation (UVGI)", 2, "hv_vent"),
]


async def ingest_domain_hvac_system(conn) -> int:
    """Insert or update HVAC System Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_hvac_system"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_hvac_system", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_hvac_system",
    )
    return count
