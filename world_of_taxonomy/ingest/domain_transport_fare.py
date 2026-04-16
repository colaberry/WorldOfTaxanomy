"""Transportation Fare and Pricing Model Types domain taxonomy ingester.

Classifies transportation services by their fare structure and pricing mechanism.
Orthogonal to mode type, service class, and infrastructure type.
Used by transit agencies, mobility platform operators, revenue management teams,
and transport economists evaluating pricing strategies.

Code prefix: dtrnfar_
System ID: domain_transport_fare
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
TRANSPORT_FARE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dtrnfar_flat", "Fixed and Flat-Rate Fare Structures", 1, None),
    ("dtrnfar_flat_single", "Single flat fare per trip (local bus, metro flat-fare zone)", 2, "dtrnfar_flat"),
    ("dtrnfar_flat_free", "Zero-fare / free transit (city-funded fare-free systems)", 2, "dtrnfar_flat"),
    ("dtrnfar_zonal", "Zone-Based and Distance-Based Fare Structures", 1, None),
    ("dtrnfar_zonal_zone", "Zone fare: price by number of zones crossed (London TfL)", 2, "dtrnfar_zonal"),
    ("dtrnfar_zonal_distance", "Distance-based fare: cents/mile or per-km tariff", 2, "dtrnfar_zonal"),
    ("dtrnfar_zonal_origin", "Origin-destination matrix pricing", 2, "dtrnfar_zonal"),
    ("dtrnfar_dynamic", "Dynamic and Surge Pricing", 1, None),
    ("dtrnfar_dynamic_surge", "Surge / dynamic pricing (Uber, Lyft multiplier pricing)", 2, "dtrnfar_dynamic"),
    ("dtrnfar_dynamic_congestion", "Cordon congestion pricing (London, Stockholm, NYC)", 2, "dtrnfar_dynamic"),
    ("dtrnfar_dynamic_yield", "Yield management (airline, intercity rail revenue mgmt)", 2, "dtrnfar_dynamic"),
    ("dtrnfar_subscription", "Subscription and Pass Models", 1, None),
    ("dtrnfar_subscription_monthly", "Monthly transit pass (unlimited or capped trips)", 2, "dtrnfar_subscription"),
    ("dtrnfar_subscription_mobility", "Mobility-as-a-Service (MaaS) bundle subscriptions", 2, "dtrnfar_subscription"),
    ("dtrnfar_regulated", "Government-Regulated and Subsidized Fare Models", 1, None),
    ("dtrnfar_regulated_pub", "Publicly-set fares (regulated transit authority pricing)", 2, "dtrnfar_regulated"),
    ("dtrnfar_regulated_discount", "Concessionary fares (seniors, students, disabled riders)", 2, "dtrnfar_regulated"),
    ("dtrnfar_regulated_freight", "Regulated freight tariffs (rail common carrier pricing)", 2, "dtrnfar_regulated"),
]

_DOMAIN_ROW = (
    "domain_transport_fare",
    "Transportation Fare and Pricing Model Types",
    "Transportation fare structure and pricing model classification for passenger and freight services",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['48', '49', '4851', '4859']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_transport_fare(conn) -> int:
    """Ingest Transportation Fare and Pricing Model Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_transport_fare'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_transport_fare",
        "Transportation Fare and Pricing Model Types",
        "Transportation fare structure and pricing model classification for passenger and freight services",
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

    parent_codes = {parent for _, _, _, parent in TRANSPORT_FARE_NODES if parent is not None}

    rows = [
        (
            "domain_transport_fare",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in TRANSPORT_FARE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(TRANSPORT_FARE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_transport_fare'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_transport_fare'",
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
            [("naics_2022", code, "domain_transport_fare", "primary") for code in naics_codes],
        )

    return count
