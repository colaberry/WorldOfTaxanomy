"""Mining Extraction Method domain taxonomy ingester.

Extraction method taxonomy organizes mining techniques into categories:
  Surface Mining   (dme_surface*)     - open-pit, strip, dredge, quarry
  Underground      (dme_underground*) - room-and-pillar, longwall, shaft, block cave
  Fluid Extraction (dme_fluid*)       - drilling, hydraulic fracturing, solution mining
  Processing       (dme_process*)     - crushing, flotation, smelting, heap leaching

Source: SME (Society for Mining, Metallurgy and Exploration). Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
MINING_METHOD_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Surface Mining category --
    ("dme_surface",        "Surface Mining Methods",                          1, None),
    ("dme_surface_open",   "Open-Pit Mining",                                2, "dme_surface"),
    ("dme_surface_strip",  "Strip Mining",                                   2, "dme_surface"),
    ("dme_surface_dredge", "Dredging",                                       2, "dme_surface"),
    ("dme_surface_quarry", "Quarrying",                                      2, "dme_surface"),

    # -- Underground Mining category --
    ("dme_underground",        "Underground Mining Methods",                  1, None),
    ("dme_underground_room",   "Room and Pillar Mining",                     2, "dme_underground"),
    ("dme_underground_long",   "Longwall Mining",                            2, "dme_underground"),
    ("dme_underground_shaft",  "Shaft Mining and Drift Mining",              2, "dme_underground"),
    ("dme_underground_block",  "Block Caving",                               2, "dme_underground"),
    ("dme_underground_stope",  "Stoping (cut-and-fill, shrinkage)",          2, "dme_underground"),

    # -- Fluid and Solution Mining category --
    ("dme_fluid",          "Fluid and Solution Mining",                       1, None),
    ("dme_fluid_drill",    "Drilling and Well Completion",                   2, "dme_fluid"),
    ("dme_fluid_frack",    "Hydraulic Fracturing (fracking)",                2, "dme_fluid"),
    ("dme_fluid_solution", "Solution Mining (in-situ leaching)",             2, "dme_fluid"),
    ("dme_fluid_dewater",  "Dewatering and Brine Extraction",                2, "dme_fluid"),

    # -- Processing Methods category --
    ("dme_process",        "Mineral Processing Methods",                      1, None),
    ("dme_process_crush",  "Crushing and Grinding (comminution)",            2, "dme_process"),
    ("dme_process_float",  "Flotation (froth flotation)",                    2, "dme_process"),
    ("dme_process_smelt",  "Smelting and Refining (pyrometallurgy)",         2, "dme_process"),
    ("dme_process_heap",   "Heap Leaching (hydrometallurgy)",                2, "dme_process"),
]

_DOMAIN_ROW = (
    "domain_mining_method",
    "Mining Extraction Methods",
    "Surface, underground, fluid extraction and mineral processing method taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["21"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific method types."""
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


async def ingest_domain_mining_method(conn) -> int:
    """Ingest Mining Extraction Method domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_mining_method'), and links NAICS 21xxx nodes
    via node_taxonomy_link.

    Returns total method node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_mining_method",
        "Mining Extraction Methods",
        "Surface, underground, fluid extraction and mineral processing method taxonomy",
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

    parent_codes = {parent for _, _, _, parent in MINING_METHOD_NODES if parent is not None}

    rows = [
        (
            "domain_mining_method",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in MINING_METHOD_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(MINING_METHOD_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_mining_method'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_mining_method'",
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
        [("naics_2022", code, "domain_mining_method", "primary") for code in naics_codes],
    )

    return count
