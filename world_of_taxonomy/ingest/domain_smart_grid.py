"""Ingest Smart Grid Technology and Component Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_smart_grid",
    "Smart Grid Types",
    "Smart Grid Technology and Component Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("sg_comp", "Grid Components", 1, None),
    ("sg_tech", "Smart Technologies", 1, None),
    ("sg_svc", "Grid Services", 1, None),
    ("sg_comp_ami", "Advanced metering infrastructure (AMI)", 2, "sg_comp"),
    ("sg_comp_da", "Distribution automation", 2, "sg_comp"),
    ("sg_comp_scada", "SCADA and grid management", 2, "sg_comp"),
    ("sg_comp_sub", "Smart substation", 2, "sg_comp"),
    ("sg_tech_der", "Distributed energy resources (DER)", 2, "sg_tech"),
    ("sg_tech_dr", "Demand response", 2, "sg_tech"),
    ("sg_tech_vpp", "Virtual power plant (VPP)", 2, "sg_tech"),
    ("sg_tech_micro", "Microgrid controller", 2, "sg_tech"),
    ("sg_tech_ev", "EV grid integration (V2G, smart charging)", 2, "sg_tech"),
    ("sg_svc_freq", "Frequency regulation", 2, "sg_svc"),
    ("sg_svc_volt", "Voltage support", 2, "sg_svc"),
    ("sg_svc_spin", "Spinning reserves", 2, "sg_svc"),
    ("sg_svc_peak", "Peak shaving", 2, "sg_svc"),
    ("sg_svc_black", "Black start capability", 2, "sg_svc"),
    ("sg_svc_cap", "Capacity market participation", 2, "sg_svc"),
]


async def ingest_domain_smart_grid(conn) -> int:
    """Insert or update Smart Grid Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_smart_grid"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_smart_grid", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_smart_grid",
    )
    return count
