"""Utility Energy Source domain taxonomy ingester.

Energy source taxonomy organizes electricity generation methods:
  Fossil Fuels (due_fossil*) - coal, natural gas, oil/petroleum
  Nuclear      (due_nuclear*) - fission reactors, small modular reactors
  Renewable    (due_renew*)  - solar, wind, hydropower, geothermal, biomass
  Storage      (due_storage*) - battery, pumped hydro, hydrogen storage

Source: IEA (International Energy Agency) energy source classifications. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
ENERGY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Fossil Fuels category --
    ("due_fossil",      "Fossil Fuel Generation",                             1, None),
    ("due_fossil_coal", "Coal Power (thermal coal, lignite)",                2, "due_fossil"),
    ("due_fossil_gas",  "Natural Gas (CCGT, peaker, CHP)",                  2, "due_fossil"),
    ("due_fossil_oil",  "Oil and Petroleum Power",                           2, "due_fossil"),

    # -- Nuclear category --
    ("due_nuclear",         "Nuclear Energy",                                  1, None),
    ("due_nuclear_fission", "Nuclear Fission (LWR, PWR, BWR)",               2, "due_nuclear"),
    ("due_nuclear_smr",     "Small Modular Reactors (SMR, advanced reactor)", 2, "due_nuclear"),

    # -- Renewable Energy category --
    ("due_renew",       "Renewable Energy",                                    1, None),
    ("due_renew_solar", "Solar Power (PV, concentrated solar, thermal)",     2, "due_renew"),
    ("due_renew_wind",  "Wind Power (onshore, offshore)",                    2, "due_renew"),
    ("due_renew_hydro", "Hydropower (run-of-river, reservoir, tidal)",       2, "due_renew"),
    ("due_renew_geo",   "Geothermal Power",                                   2, "due_renew"),
    ("due_renew_bio",   "Biomass and Bioenergy (wood, waste-to-energy)",     2, "due_renew"),

    # -- Energy Storage category --
    ("due_storage",          "Energy Storage",                                 1, None),
    ("due_storage_battery",  "Battery Storage (lithium-ion, flow battery)",  2, "due_storage"),
    ("due_storage_pumped",   "Pumped Hydro Storage",                          2, "due_storage"),
    ("due_storage_hydrogen", "Hydrogen Energy Storage (electrolysis, FCEV)", 2, "due_storage"),
]

_DOMAIN_ROW = (
    "domain_util_energy",
    "Utility Energy Sources",
    "Fossil fuel, nuclear, renewable energy generation and storage taxonomy for electric utilities",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["221"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific energy types."""
    parts = code.split("_")
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_util_energy(conn) -> int:
    """Ingest Utility Energy Source domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_util_energy'), and links NAICS 221xxx nodes
    via node_taxonomy_link.

    Returns total energy source node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_util_energy",
        "Utility Energy Sources",
        "Fossil fuel, nuclear, renewable energy generation and storage taxonomy for electric utilities",
        "1.0",
        "United States",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in ENERGY_NODES if parent is not None}

    rows = [
        (
            "domain_util_energy",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in ENERGY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(ENERGY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_util_energy'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_util_energy'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '221%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_util_energy", "primary") for code in naics_codes],
    )

    return count
