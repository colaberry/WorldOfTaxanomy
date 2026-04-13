"""Construction Trade Type domain taxonomy ingester.

Trade taxonomy organizes construction work by specialty trade (CSI MasterFormat):
  Site Work     (dct_site*)   - excavation, concrete/foundation, paving
  Structural    (dct_struct*) - steel/ironwork, carpentry/framing, masonry
  MEP Trades    (dct_mep*)    - electrical, plumbing, HVAC, fire protection
  Finish Trades (dct_finish*) - drywall, flooring, painting, glazing

Source: CSI MasterFormat (Construction Specifications Institute). Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
TRADE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Site Work Trades category --
    ("dct_site",           "Site Work Trades",                                1, None),
    ("dct_site_excav",     "Excavation, Grading, and Earthwork",             2, "dct_site"),
    ("dct_site_concrete",  "Concrete and Foundation Work",                   2, "dct_site"),
    ("dct_site_paving",    "Paving and Asphalt Work",                        2, "dct_site"),
    ("dct_site_utility",   "Site Utilities (underground piping, conduit)",   2, "dct_site"),

    # -- Structural Trades category --
    ("dct_struct",         "Structural Trades",                               1, None),
    ("dct_struct_steel",   "Structural Steel and Ironwork",                  2, "dct_struct"),
    ("dct_struct_wood",    "Carpentry and Wood Framing",                     2, "dct_struct"),
    ("dct_struct_mason",   "Masonry and Stonework (brick, block, tile)",     2, "dct_struct"),
    ("dct_struct_precast", "Precast Concrete and Tilt-Up",                   2, "dct_struct"),

    # -- MEP Trades (Mechanical-Electrical-Plumbing) category --
    ("dct_mep",          "MEP Trades (Mechanical-Electrical-Plumbing)",      1, None),
    ("dct_mep_elec",     "Electrical (power, lighting, low-voltage)",        2, "dct_mep"),
    ("dct_mep_plumb",    "Plumbing (domestic water, sanitary, storm)",       2, "dct_mep"),
    ("dct_mep_hvac",     "HVAC and Mechanical (heating, cooling, ventilation)", 2, "dct_mep"),
    ("dct_mep_fire",     "Fire Protection (sprinklers, alarms, suppression)", 2, "dct_mep"),

    # -- Finish Trades category --
    ("dct_finish",         "Finish Trades",                                   1, None),
    ("dct_finish_drywall", "Drywall and Plaster (interior partitions)",      2, "dct_finish"),
    ("dct_finish_floor",   "Flooring (hardwood, tile, carpet, LVP)",        2, "dct_finish"),
    ("dct_finish_paint",   "Painting and Coatings",                          2, "dct_finish"),
    ("dct_finish_glass",   "Glazing and Glass (windows, curtain wall)",      2, "dct_finish"),
]

_DOMAIN_ROW = (
    "domain_const_trade",
    "Construction Trade Types",
    "Site work, structural, MEP and finish trade taxonomy based on CSI MasterFormat",
    "WorldOfTaxanomy",
    None,
)

_NAICS_PREFIXES = ["23"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific trade types."""
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


async def ingest_domain_const_trade(conn) -> int:
    """Ingest Construction Trade Type domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_const_trade'), and links NAICS 23xxx nodes
    via node_taxonomy_link.

    Returns total trade node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_const_trade",
        "Construction Trade Types",
        "Site work, structural, MEP and finish trade taxonomy based on CSI MasterFormat",
        "1.0",
        "United States",
        "WorldOfTaxanomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in TRADE_NODES if parent is not None}

    rows = [
        (
            "domain_const_trade",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in TRADE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(TRADE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_const_trade'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_const_trade'",
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
        [("naics_2022", code, "domain_const_trade", "primary") for code in naics_codes],
    )

    return count
