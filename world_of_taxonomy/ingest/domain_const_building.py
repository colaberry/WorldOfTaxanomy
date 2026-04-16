"""Construction Building Type domain taxonomy ingester.

Building type taxonomy uses IBC (International Building Code) occupancy groups:
  Residential   (dcb_resid*)  - single-family, multi-family, mixed-use
  Commercial    (dcb_comm*)   - office, retail, hotel, restaurant
  Industrial    (dcb_indust*) - manufacturing, warehouse, utility infrastructure
  Institutional (dcb_inst*)   - healthcare, education, government, recreation/assembly

Source: International Building Code (IBC) occupancy classifications. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
BUILDING_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Residential category --
    ("dcb_resid",        "Residential Buildings",                             1, None),
    ("dcb_resid_single", "Single-Family Residential (detached, townhouse)",  2, "dcb_resid"),
    ("dcb_resid_multi",  "Multi-Family Residential (apartment, condo)",      2, "dcb_resid"),
    ("dcb_resid_mixed",  "Mixed-Use Residential (live/work, ground-floor retail)", 2, "dcb_resid"),

    # -- Commercial category --
    ("dcb_comm",        "Commercial Buildings",                               1, None),
    ("dcb_comm_office", "Office and Business (Class A/B/C office)",          2, "dcb_comm"),
    ("dcb_comm_retail", "Retail and Mercantile (mall, strip center, box)",   2, "dcb_comm"),
    ("dcb_comm_hotel",  "Hotel and Lodging (full-service, select-service)",  2, "dcb_comm"),
    ("dcb_comm_rest",   "Restaurant and Food Service",                        2, "dcb_comm"),

    # -- Industrial category --
    ("dcb_indust",           "Industrial Buildings",                          1, None),
    ("dcb_indust_mfg",       "Manufacturing and Factory (light, heavy)",     2, "dcb_indust"),
    ("dcb_indust_warehouse", "Warehouse and Distribution",                   2, "dcb_indust"),
    ("dcb_indust_utility",   "Utility and Infrastructure (plant, substation)", 2, "dcb_indust"),

    # -- Institutional category --
    ("dcb_inst",        "Institutional Buildings",                            1, None),
    ("dcb_inst_health", "Healthcare (hospital, clinic, medical office)",     2, "dcb_inst"),
    ("dcb_inst_edu",    "Education (K-12 school, university, daycare)",      2, "dcb_inst"),
    ("dcb_inst_gov",    "Government and Civic (courthouse, library, DMV)",   2, "dcb_inst"),
    ("dcb_inst_rec",    "Recreation and Assembly (arena, theater, gym)",     2, "dcb_inst"),
]

_DOMAIN_ROW = (
    "domain_const_building",
    "Construction Building Types",
    "Residential, commercial, industrial and institutional building type taxonomy based on IBC occupancy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["23"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific building types."""
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


async def ingest_domain_const_building(conn) -> int:
    """Ingest Construction Building Type domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_const_building'), and links NAICS 23xxx nodes
    via node_taxonomy_link.

    Returns total building type node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_const_building",
        "Construction Building Types",
        "Residential, commercial, industrial and institutional building type taxonomy based on IBC occupancy",
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

    parent_codes = {parent for _, _, _, parent in BUILDING_NODES if parent is not None}

    rows = [
        (
            "domain_const_building",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in BUILDING_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(BUILDING_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_const_building'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_const_building'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '23%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_const_building", "primary") for code in naics_codes],
    )

    return count
