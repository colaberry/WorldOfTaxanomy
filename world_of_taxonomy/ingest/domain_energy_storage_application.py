"""Energy Storage Application Use Case Types domain taxonomy ingester.

Classifies energy storage systems by their primary use case and deployment context.
Orthogonal to storage technology type (domain_energy_storage) and performance specs.
Based on NREL/DOE energy storage technology primer, BNEF ERCOT storage taxonomy,
and IEA Energy Storage Tracking report.
Used by utilities, developers, OEMs, policy analysts and project finance teams.

Code prefix: desapp_
System ID: domain_energy_storage_application
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
ENERGY_STORAGE_APP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("desapp_grid", "Grid-Scale (Front-of-Meter) Storage Applications", 1, None),
    ("desapp_grid_freq", "Frequency regulation and spinning reserve (fast-response BESS)", 2, "desapp_grid"),
    ("desapp_grid_peak", "Peak shaving and capacity firming (daily charge/discharge)", 2, "desapp_grid"),
    ("desapp_grid_arb", "Energy arbitrage (buy low off-peak, sell high on-peak)", 2, "desapp_grid"),
    ("desapp_grid_black", "Black start and grid restoration (post-outage repower)", 2, "desapp_grid"),
    ("desapp_grid_txdefer", "Transmission and distribution deferral (congestion relief)", 2, "desapp_grid"),
    ("desapp_btm", "Behind-the-Meter Commercial and Industrial (C&I)", 1, None),
    ("desapp_btm_demcharge", "Demand charge reduction (C&I utility bill management)", 2, "desapp_btm"),
    ("desapp_btm_solar", "Solar self-consumption optimization (solar + storage pairing)", 2, "desapp_btm"),
    ("desapp_btm_microgrid", "Microgrid and islanding (campus, industrial park, data center)", 2, "desapp_btm"),
    ("desapp_ev", "Electric Vehicle and Mobility Storage", 1, None),
    ("desapp_ev_bev", "Battery EV traction packs (BEV passenger cars, trucks)", 2, "desapp_ev"),
    ("desapp_ev_phev", "Plug-in hybrid EV (PHEV) traction and range extender packs", 2, "desapp_ev"),
    ("desapp_ev_v2g", "Vehicle-to-grid (V2G) bidirectional charging", 2, "desapp_ev"),
    ("desapp_consumer", "Consumer and Portable Electronics Storage", 1, None),
    ("desapp_consumer_mobile", "Smartphone, laptop and wearable batteries", 2, "desapp_consumer"),
    ("desapp_consumer_homres", "Residential home energy storage (Tesla Powerwall, Enphase)", 2, "desapp_consumer"),
    ("desapp_backup", "Backup Power, UPS and Off-Grid Storage", 1, None),
    ("desapp_backup_ups", "UPS (uninterruptible power supply) for data centers and hospitals", 2, "desapp_backup"),
    ("desapp_backup_offgrid", "Off-grid and rural electrification storage systems", 2, "desapp_backup"),
]

_DOMAIN_ROW = (
    "domain_energy_storage_application",
    "Energy Storage Application Use Case Types",
    "Energy storage system application and use case classification: grid, BTM, EV, consumer and backup",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['2211', '3359', '3362']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_energy_storage_application(conn) -> int:
    """Ingest Energy Storage Application Use Case Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_energy_storage_application'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_energy_storage_application",
        "Energy Storage Application Use Case Types",
        "Energy storage system application and use case classification: grid, BTM, EV, consumer and backup",
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

    parent_codes = {parent for _, _, _, parent in ENERGY_STORAGE_APP_NODES if parent is not None}

    rows = [
        (
            "domain_energy_storage_application",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in ENERGY_STORAGE_APP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(ENERGY_STORAGE_APP_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_energy_storage_application'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_energy_storage_application'",
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
            [("naics_2022", code, "domain_energy_storage_application", "primary") for code in naics_codes],
        )

    return count
