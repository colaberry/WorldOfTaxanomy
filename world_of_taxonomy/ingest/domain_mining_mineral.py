"""Mining Mineral Types domain taxonomy ingester.

Mineral taxonomy organizes extractable resources into categories:
  Metal Minerals        (dmm_metal*)   - iron, copper, gold, silver, aluminum
  Energy Minerals       (dmm_energy*)  - coal, crude oil, natural gas, uranium
  Industrial Minerals   (dmm_indmin*)  - potash, limestone, silica, salt
  Construction Minerals (dmm_constr*)  - aggregates, clay, gypsum
  Gemstones             (dmm_gem*)     - diamonds, colored gemstones

Source: USGS Mineral Resources Program + SPE classifications. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
MINERAL_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Metal Minerals category --
    ("dmm_metal",       "Metal Minerals",                                      1, None),
    ("dmm_metal_fe",    "Iron Ore and Steel Minerals",                        2, "dmm_metal"),
    ("dmm_metal_cu",    "Copper",                                              2, "dmm_metal"),
    ("dmm_metal_au",    "Gold",                                                2, "dmm_metal"),
    ("dmm_metal_ag",    "Silver",                                              2, "dmm_metal"),
    ("dmm_metal_al",    "Aluminum (Bauxite)",                                 2, "dmm_metal"),
    ("dmm_metal_other", "Other Metals (zinc, lead, nickel, tin, manganese)",  2, "dmm_metal"),

    # -- Energy Minerals category --
    ("dmm_energy",         "Energy Minerals",                                  1, None),
    ("dmm_energy_coal",    "Coal (thermal, metallurgical, lignite)",          2, "dmm_energy"),
    ("dmm_energy_oil",     "Crude Oil and Condensate",                        2, "dmm_energy"),
    ("dmm_energy_gas",     "Natural Gas (conventional and unconventional)",   2, "dmm_energy"),
    ("dmm_energy_uranium", "Uranium and Nuclear Fuels",                       2, "dmm_energy"),

    # -- Industrial Minerals category --
    ("dmm_indmin",          "Industrial Minerals",                             1, None),
    ("dmm_indmin_potash",   "Potash and Fertilizer Minerals",                 2, "dmm_indmin"),
    ("dmm_indmin_lime",     "Limestone and Industrial Carbonates",            2, "dmm_indmin"),
    ("dmm_indmin_silica",   "Silica and Industrial Sands",                    2, "dmm_indmin"),
    ("dmm_indmin_salt",     "Salt and Evaporites",                            2, "dmm_indmin"),
    ("dmm_indmin_talc",     "Talc, Asbestos Alternatives, and Specialty Clay", 2, "dmm_indmin"),

    # -- Construction Minerals category --
    ("dmm_constr",        "Construction Minerals",                             1, None),
    ("dmm_constr_agg",    "Aggregates (crushed stone, sand, gravel)",         2, "dmm_constr"),
    ("dmm_constr_clay",   "Clay and Shale",                                   2, "dmm_constr"),
    ("dmm_constr_gypsum", "Gypsum and Plaster Minerals",                      2, "dmm_constr"),

    # -- Gemstones category --
    ("dmm_gem",         "Gemstones",                                           1, None),
    ("dmm_gem_diamond", "Diamonds",                                            2, "dmm_gem"),
    ("dmm_gem_color",   "Colored Gemstones (ruby, emerald, sapphire, opal)",  2, "dmm_gem"),
]

_DOMAIN_ROW = (
    "domain_mining_mineral",
    "Mining Mineral Types",
    "Metal, energy, industrial, construction mineral and gemstone taxonomy for mining sectors",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["21"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific mineral types."""
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


async def ingest_domain_mining_mineral(conn) -> int:
    """Ingest Mining Mineral Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_mining_mineral'), and links NAICS 21xxx nodes
    via node_taxonomy_link.

    Returns total mineral node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_mining_mineral",
        "Mining Mineral Types",
        "Metal, energy, industrial, construction mineral and gemstone taxonomy for mining sectors",
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

    parent_codes = {parent for _, _, _, parent in MINERAL_NODES if parent is not None}

    rows = [
        (
            "domain_mining_mineral",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in MINERAL_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(MINERAL_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_mining_mineral'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_mining_mineral'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '21%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_mining_mineral", "primary") for code in naics_codes],
    )

    return count
