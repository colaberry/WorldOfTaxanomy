"""Real Estate Property Type domain taxonomy ingester.

Real estate taxonomy organizes property types (NAICS 53):
  Residential    (drt_resid*)  - SFR, multifamily, condo, manufactured housing
  Commercial     (drt_comm*)   - office, retail, industrial, hospitality
  Special Purpose (drt_spec*)  - healthcare, senior housing, self-storage, data center
  Land           (drt_land*)   - farmland, timberland, development land, mineral rights

Source: CoStar/NCREIF property type classifications. Public domain concepts.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
REALESTATE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Residential category --
    ("drt_resid",        "Residential Real Estate",                           1, None),
    ("drt_resid_sfr",    "Single-Family Residential (SFR, detached)",        2, "drt_resid"),
    ("drt_resid_mf",     "Multifamily (apartment complex, garden-style)",    2, "drt_resid"),
    ("drt_resid_condo",  "Condominium and Cooperative",                      2, "drt_resid"),
    ("drt_resid_manuf",  "Manufactured Housing (mobile home park)",          2, "drt_resid"),

    # -- Commercial category --
    ("drt_comm",         "Commercial Real Estate",                            1, None),
    ("drt_comm_office",  "Office (Class A, B, C; suburban, CBD, flex)",     2, "drt_comm"),
    ("drt_comm_retail",  "Retail (mall, strip center, power center, NNN)",  2, "drt_comm"),
    ("drt_comm_indust",  "Industrial (warehouse, distribution, flex, R&D)", 2, "drt_comm"),
    ("drt_comm_hotel",   "Hospitality (hotel, motel, resort, extended-stay)", 2, "drt_comm"),

    # -- Special Purpose category --
    ("drt_spec",          "Special Purpose Real Estate",                      1, None),
    ("drt_spec_health",   "Healthcare Real Estate (MOB, hospital, clinic)",  2, "drt_spec"),
    ("drt_spec_senior",   "Senior Housing (independent living, AL, memory)", 2, "drt_spec"),
    ("drt_spec_storage",  "Self-Storage Facility",                            2, "drt_spec"),
    ("drt_spec_data",     "Data Center and Life Science Lab",                2, "drt_spec"),
    ("drt_spec_edu",      "Education and Student Housing",                   2, "drt_spec"),

    # -- Land category --
    ("drt_land",         "Land",                                              1, None),
    ("drt_land_farm",    "Farmland and Agricultural Land",                   2, "drt_land"),
    ("drt_land_timber",  "Timberland and Forestland",                        2, "drt_land"),
    ("drt_land_dev",     "Development Land (entitled, infill, greenfield)",  2, "drt_land"),
    ("drt_land_mineral", "Mineral Rights and Subsurface Interests",          2, "drt_land"),
]

_DOMAIN_ROW = (
    "domain_realestate_type",
    "Real Estate Property Types",
    "Residential, commercial, special purpose and land property type taxonomy for real estate",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["53"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific property types."""
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


async def ingest_domain_realestate_type(conn) -> int:
    """Ingest Real Estate Property Type domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_realestate_type'), and links NAICS 53xxx nodes
    via node_taxonomy_link.

    Returns total property type node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_realestate_type",
        "Real Estate Property Types",
        "Residential, commercial, special purpose and land property type taxonomy for real estate",
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

    parent_codes = {parent for _, _, _, parent in REALESTATE_NODES if parent is not None}

    rows = [
        (
            "domain_realestate_type",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in REALESTATE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REALESTATE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_realestate_type'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_realestate_type'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '53%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_realestate_type", "primary") for code in naics_codes],
    )

    return count
