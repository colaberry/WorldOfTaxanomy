"""Retail Store Format and Size Types domain taxonomy ingester.

Classifies retail businesses by their store format and physical footprint.
Orthogonal to merchandise category, pricing strategy and fulfillment model.
Based on NRF, IGD and Kantar retail format taxonomy.
Used by retail real estate developers, brand strategy teams, and
location analytics providers.

Code prefix: drtlfmt_
System ID: domain_retail_format
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
RETAIL_FORMAT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("drtlfmt_mass", "Mass Market and Supercenter Formats", 1, None),
    ("drtlfmt_mass_hyper", "Hypermarket / supercenter (100k+ sq ft, full grocery + GM)", 2, "drtlfmt_mass"),
    ("drtlfmt_mass_superstore", "Superstore (50-100k sq ft, dominant category + grocery)", 2, "drtlfmt_mass"),
    ("drtlfmt_grocery", "Grocery and Supermarket Formats", 1, None),
    ("drtlfmt_grocery_traditional", "Traditional supermarket (25-50k sq ft, full-service)", 2, "drtlfmt_grocery"),
    ("drtlfmt_grocery_limited", "Limited assortment / hard discount (Aldi, Lidl, 2000 SKUs)", 2, "drtlfmt_grocery"),
    ("drtlfmt_grocery_natural", "Natural and organic grocer (Whole Foods, Sprouts)", 2, "drtlfmt_grocery"),
    ("drtlfmt_convenience", "Convenience and Small-Format Stores", 1, None),
    ("drtlfmt_convenience_c_store", "Traditional c-store (2-5k sq ft, fuel, beverages, snacks)", 2, "drtlfmt_convenience"),
    ("drtlfmt_convenience_express", "Grocer express format (urban micro-store, grab-and-go)", 2, "drtlfmt_convenience"),
    ("drtlfmt_specialty", "Specialty and Category-Killer Formats", 1, None),
    ("drtlfmt_specialty_cat", "Category killer (Home Depot, Best Buy, Petco - category dominance)", 2, "drtlfmt_specialty"),
    ("drtlfmt_specialty_boutique", "Specialty boutique (under 3000 sq ft, curated assortment)", 2, "drtlfmt_specialty"),
    ("drtlfmt_dept", "Department Store and Anchor Formats", 1, None),
    ("drtlfmt_dept_fullline", "Full-line department store (Nordstrom, Macy's - apparel, home, beauty)", 2, "drtlfmt_dept"),
    ("drtlfmt_dept_offprice", "Off-price department store (TJ Maxx, Marshalls, Burlington)", 2, "drtlfmt_dept"),
    ("drtlfmt_digital", "Digital-First and Hybrid Retail Formats", 1, None),
    ("drtlfmt_digital_pure", "Pure-play e-commerce (no physical stores)", 2, "drtlfmt_digital"),
    ("drtlfmt_digital_omni", "Omni-channel: integrated online + physical stores", 2, "drtlfmt_digital"),
    ("drtlfmt_digital_pop", "Pop-up shop and experiential retail format", 2, "drtlfmt_digital"),
]

_DOMAIN_ROW = (
    "domain_retail_format",
    "Retail Store Format and Size Types",
    "Retail store format, channel type and size tier classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['44', '45', '4541']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_retail_format(conn) -> int:
    """Ingest Retail Store Format and Size Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_retail_format'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_retail_format",
        "Retail Store Format and Size Types",
        "Retail store format, channel type and size tier classification",
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

    parent_codes = {parent for _, _, _, parent in RETAIL_FORMAT_NODES if parent is not None}

    rows = [
        (
            "domain_retail_format",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in RETAIL_FORMAT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(RETAIL_FORMAT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_retail_format'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_retail_format'",
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
            [("naics_2022", code, "domain_retail_format", "primary") for code in naics_codes],
        )

    return count
