"""Energy Storage Performance and Specification Types domain taxonomy ingester.

Classifies energy storage systems by their key performance and specification dimensions.
Orthogonal to application use case and storage technology type.
Used by procurement engineers, project developers, and asset managers when
specifying and comparing storage systems across vendors and technologies.

Code prefix: desperf_
System ID: domain_energy_storage_perf
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
ENERGY_STORAGE_PERF_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("desperf_energydens", "Energy Density and Capacity", 1, None),
    ("desperf_energydens_grav", "Gravimetric energy density (Wh/kg) - weight-sensitive applications", 2, "desperf_energydens"),
    ("desperf_energydens_vol", "Volumetric energy density (Wh/L) - space-constrained applications", 2, "desperf_energydens"),
    ("desperf_energydens_sys", "System-level energy capacity (kWh to GWh total storage)", 2, "desperf_energydens"),
    ("desperf_power", "Power Density and Response Speed", 1, None),
    ("desperf_power_density", "Power density (W/kg or W/L) - high-rate applications", 2, "desperf_power"),
    ("desperf_power_crate", "C-rate capability (1C, 2C, 4C charge/discharge rates)", 2, "desperf_power"),
    ("desperf_power_response", "Response time (milliseconds for frequency regulation)", 2, "desperf_power"),
    ("desperf_lifetime", "Cycle Life and Calendar Life", 1, None),
    ("desperf_lifetime_cycles", "Cycle life (number of charge-discharge cycles at EOL capacity)", 2, "desperf_lifetime"),
    ("desperf_lifetime_calendar", "Calendar life (years of useful service, degradation rate)", 2, "desperf_lifetime"),
    ("desperf_lifetime_warranted", "Warranted throughput (MWh guaranteed, annual degradation guarantee)", 2, "desperf_lifetime"),
    ("desperf_safety", "Safety, Thermal and Environmental", 1, None),
    ("desperf_safety_thermal", "Thermal management requirements (liquid cooling, phase change)", 2, "desperf_safety"),
    ("desperf_safety_fire", "Fire safety and thermal runaway propagation containment", 2, "desperf_safety"),
    ("desperf_safety_recycling", "End-of-life recycling and second-life repurposing options", 2, "desperf_safety"),
]

_DOMAIN_ROW = (
    "domain_energy_storage_perf",
    "Energy Storage Performance and Specification Types",
    "Energy storage performance, specification and lifecycle classification for technology comparison",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3359', '5417', '2211']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_energy_storage_perf(conn) -> int:
    """Ingest Energy Storage Performance and Specification Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_energy_storage_perf'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_energy_storage_perf",
        "Energy Storage Performance and Specification Types",
        "Energy storage performance, specification and lifecycle classification for technology comparison",
        "1.0",
        "Global",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in ENERGY_STORAGE_PERF_NODES if parent is not None}

    rows = [
        (
            "domain_energy_storage_perf",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in ENERGY_STORAGE_PERF_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(ENERGY_STORAGE_PERF_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_energy_storage_perf'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_energy_storage_perf'",
        count,
    )

    naics_codes = [
        row["code"]
        for prefix in _NAICS_PREFIXES
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE $1",
            prefix + "%",
        )
    ]

    if naics_codes:
        await conn.executemany(
            """INSERT INTO node_taxonomy_link
                   (system_id, node_code, taxonomy_id, relevance)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
            [("naics_2022", code, "domain_energy_storage_perf", "primary") for code in naics_codes],
        )

    return count
