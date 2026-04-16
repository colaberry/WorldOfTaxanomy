"""Ingest Electric Vehicle Charging Infrastructure Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_ev_charging",
    "EV Charging Infrastructure Types",
    "Electric Vehicle Charging Infrastructure Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("evc_level", "Charging Levels", 1, None),
    ("evc_net", "Network and Business Models", 1, None),
    ("evc_tech", "Technology Types", 1, None),
    ("evc_level_1", "Level 1 AC (120V, home outlet)", 2, "evc_level"),
    ("evc_level_2", "Level 2 AC (240V, dedicated EVSE)", 2, "evc_level"),
    ("evc_level_3", "DC Fast Charging (50-350kW)", 2, "evc_level"),
    ("evc_level_uf", "Ultra-fast charging (350kW+)", 2, "evc_level"),
    ("evc_net_pub", "Public charging network operator", 2, "evc_net"),
    ("evc_net_dest", "Destination charging (hotels, retail)", 2, "evc_net"),
    ("evc_net_work", "Workplace charging", 2, "evc_net"),
    ("evc_net_home", "Home charging (L1/L2 EVSE)", 2, "evc_net"),
    ("evc_net_fleet", "Fleet and depot charging", 2, "evc_net"),
    ("evc_tech_ccs", "CCS (Combined Charging System)", 2, "evc_tech"),
    ("evc_tech_chad", "CHAdeMO", 2, "evc_tech"),
    ("evc_tech_nacs", "NACS (Tesla/SAE J3400)", 2, "evc_tech"),
    ("evc_tech_wire", "Wireless (inductive) charging", 2, "evc_tech"),
    ("evc_tech_swap", "Battery swapping", 2, "evc_tech"),
    ("evc_tech_v2g", "Vehicle-to-grid (V2G)", 2, "evc_tech"),
]


async def ingest_domain_ev_charging(conn) -> int:
    """Insert or update EV Charging Infrastructure Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_ev_charging"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_ev_charging", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_ev_charging",
    )
    return count
