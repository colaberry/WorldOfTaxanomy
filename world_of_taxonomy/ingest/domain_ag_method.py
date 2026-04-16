"""Agriculture Farming Method domain taxonomy ingester.

Farming method taxonomy organizes production practices into categories:
  Production System (dam_sys*)   - conventional, organic, biodynamic, hydroponic
  Scale             (dam_scale*) - smallholder, family farm, commercial, corporate
  Irrigation        (dam_irr*)   - dryland, irrigated, drip, flood, sprinkler
  Tillage           (dam_till*)  - conventional till, no-till, strip-till, reduced
  Certification     (dam_cert*)  - USDA organic, GAP, fair trade, non-GMO

Source: USDA NASS + NOP (National Organic Program). Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
METHOD_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Production System category --
    ("dam_sys",                "Production System",                           1, None),
    ("dam_sys_conventional",   "Conventional (synthetic inputs, standard)",   2, "dam_sys"),
    ("dam_sys_organic",        "Organic (USDA NOP certified)",                2, "dam_sys"),
    ("dam_sys_biodynamic",     "Biodynamic (Demeter certified)",              2, "dam_sys"),
    ("dam_sys_hydroponic",     "Hydroponic and Aquaponic",                    2, "dam_sys"),
    ("dam_sys_regenerative",   "Regenerative Agriculture",                    2, "dam_sys"),
    ("dam_sys_integrated",     "Integrated Pest Management (IPM)",            2, "dam_sys"),

    # -- Scale category --
    ("dam_scale",              "Farm Scale",                                   1, None),
    ("dam_scale_small",        "Smallholder (under 50 acres)",                2, "dam_scale"),
    ("dam_scale_family",       "Family Farm (50-499 acres)",                  2, "dam_scale"),
    ("dam_scale_commercial",   "Commercial (500-4,999 acres)",                2, "dam_scale"),
    ("dam_scale_corporate",    "Corporate and Industrial (5,000+ acres)",     2, "dam_scale"),

    # -- Irrigation category --
    ("dam_irr",                "Irrigation Method",                            1, None),
    ("dam_irr_dryland",        "Dryland (rain-fed, no irrigation)",           2, "dam_irr"),
    ("dam_irr_flood",          "Flood Irrigation (surface, furrow)",          2, "dam_irr"),
    ("dam_irr_sprinkler",      "Sprinkler Irrigation (center pivot, lateral)", 2, "dam_irr"),
    ("dam_irr_drip",           "Drip Irrigation (micro-irrigation, trickle)", 2, "dam_irr"),
    ("dam_irr_subsurface",     "Subsurface Drip Irrigation (SDI)",            2, "dam_irr"),

    # -- Tillage category --
    ("dam_till",               "Tillage Practice",                             1, None),
    ("dam_till_conventional",  "Conventional Tillage (full inversion)",       2, "dam_till"),
    ("dam_till_reduced",       "Reduced Tillage (minimum till)",              2, "dam_till"),
    ("dam_till_strip",         "Strip-Till",                                   2, "dam_till"),
    ("dam_till_notill",        "No-Till (direct seeding)",                    2, "dam_till"),

    # -- Certification category --
    ("dam_cert",               "Certification and Standards",                  1, None),
    ("dam_cert_usda_organic",  "USDA Organic Certified",                      2, "dam_cert"),
    ("dam_cert_gap",           "Good Agricultural Practices (GAP/GHP)",       2, "dam_cert"),
    ("dam_cert_fairtrade",     "Fair Trade Certified",                         2, "dam_cert"),
    ("dam_cert_nongmo",        "Non-GMO Project Verified",                    2, "dam_cert"),
    ("dam_cert_rainforest",    "Rainforest Alliance Certified",               2, "dam_cert"),
]

_DOMAIN_ROW = (
    "domain_ag_method",
    "Agricultural Farming Methods",
    "Production system, scale, irrigation, tillage and certification taxonomy for farming methods",
    "WorldOfTaxonomy",
    None,  # url
)

# NAICS prefixes to link (11xxx = Agriculture broadly)
_NAICS_PREFIXES = ["11"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific method types."""
    parts = code.split("_")
    # 'dam_sys'          -> ['dam', 'sys']               -> level 1
    # 'dam_sys_organic'  -> ['dam', 'sys', 'organic']    -> level 2
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_ag_method(conn) -> int:
    """Ingest Agricultural Farming Method domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_ag_method'), and links NAICS 11xxx nodes
    via node_taxonomy_link.

    Returns total method node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_ag_method",
        "Agricultural Farming Methods",
        "Production system, scale, irrigation, tillage and certification taxonomy for farming methods",
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

    parent_codes = {parent for _, _, _, parent in METHOD_NODES if parent is not None}

    rows = [
        (
            "domain_ag_method",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in METHOD_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(METHOD_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_ag_method'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_ag_method'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '11%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_ag_method", "primary") for code in naics_codes],
    )

    return count
