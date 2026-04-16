"""Transportation Mode domain taxonomy ingester.

Transportation mode taxonomy organizes non-truck transport (NAICS 48-49):
  Air Transport    (dtm_air*)   - commercial airline, cargo, charter, general aviation
  Rail Transport   (dtm_rail*)  - freight rail, passenger rail, commuter rail
  Water Transport  (dtm_water*) - deep sea, coastal, inland waterway, ferry
  Pipeline         (dtm_pipe*)  - crude oil, natural gas, refined products, water/slurry
  Courier/Other    (dtm_other*) - express courier, postal, scenic, space

Source: DOT (US Department of Transportation) modal categories. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
TRANSPORT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Air Transport category --
    ("dtm_air",         "Air Transportation",                                  1, None),
    ("dtm_air_airline", "Scheduled Commercial Airline (passenger)",           2, "dtm_air"),
    ("dtm_air_cargo",   "Air Cargo and Freight (FedEx, UPS Air, freighter)", 2, "dtm_air"),
    ("dtm_air_charter", "Charter and Non-Scheduled Air Service",              2, "dtm_air"),
    ("dtm_air_ga",      "General Aviation (private, corporate aircraft)",     2, "dtm_air"),

    # -- Rail Transport category --
    ("dtm_rail",          "Rail Transportation",                               1, None),
    ("dtm_rail_freight",  "Freight Rail (Class I, regional, short-line)",    2, "dtm_rail"),
    ("dtm_rail_intercity","Intercity Passenger Rail (Amtrak)",                2, "dtm_rail"),
    ("dtm_rail_commuter", "Commuter Rail and Light Rail (urban transit)",     2, "dtm_rail"),

    # -- Water Transport category --
    ("dtm_water",         "Water Transportation",                              1, None),
    ("dtm_water_deep",    "Deep Sea and Ocean Shipping (container, bulk)",   2, "dtm_water"),
    ("dtm_water_coastal", "Coastal and Short-Sea Shipping",                  2, "dtm_water"),
    ("dtm_water_inland",  "Inland Waterway (barge, river, lake)",            2, "dtm_water"),
    ("dtm_water_ferry",   "Passenger Ferry and Water Taxi",                  2, "dtm_water"),

    # -- Pipeline Transport category --
    ("dtm_pipe",        "Pipeline Transportation",                             1, None),
    ("dtm_pipe_crude",  "Crude Oil Pipeline",                                2, "dtm_pipe"),
    ("dtm_pipe_gas",    "Natural Gas Pipeline (transmission, distribution)", 2, "dtm_pipe"),
    ("dtm_pipe_prod",   "Refined Products Pipeline (gasoline, jet fuel)",   2, "dtm_pipe"),

    # -- Courier and Other Transport category --
    ("dtm_other",        "Courier and Other Transportation",                   1, None),
    ("dtm_other_courier","Express Courier and Messenger Services",            2, "dtm_other"),
    ("dtm_other_postal", "Postal Service and Delivery",                       2, "dtm_other"),
    ("dtm_other_scenic", "Scenic and Sightseeing Transportation",             2, "dtm_other"),
]

_DOMAIN_ROW = (
    "domain_transport_mode",
    "Transportation Modes",
    "Air, rail, water, pipeline and courier transportation mode taxonomy for NAICS 48-49",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["48", "49"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific transport modes."""
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


async def ingest_domain_transport_mode(conn) -> int:
    """Ingest Transportation Mode domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_transport_mode'), and links NAICS 48-49xxx nodes
    via node_taxonomy_link.

    Returns total transport mode node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_transport_mode",
        "Transportation Modes",
        "Air, rail, water, pipeline and courier transportation mode taxonomy for NAICS 48-49",
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

    parent_codes = {parent for _, _, _, parent in TRANSPORT_NODES if parent is not None}

    rows = [
        (
            "domain_transport_mode",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in TRANSPORT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(TRANSPORT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_transport_mode'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_transport_mode'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' "
            "AND (code LIKE '48%' OR code LIKE '49%')"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_transport_mode", "primary") for code in naics_codes],
    )

    return count
