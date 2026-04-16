"""Tourism and Travel domain taxonomy ingester.

Organizes tourism and travel sector types aligned with NAICS 5615
(Travel arrangement and reservation services) and NAICS 7211
(Traveler accommodation) covering leisure travel, business travel,
adventure tourism, cultural tourism, and medical tourism.

Code prefix: dtt_
Categories: Leisure Travel, Business Travel, Adventure Tourism,
Cultural Tourism, Medical Tourism.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Leisure Travel --
    ("dtt_leisure",            "Leisure Travel",                                     1, None),
    ("dtt_leisure_beach",      "Beach and Resort Vacations",                         2, "dtt_leisure"),
    ("dtt_leisure_cruise",     "Cruise Line and River Cruise Travel",                2, "dtt_leisure"),
    ("dtt_leisure_family",     "Family and Theme Park Travel",                       2, "dtt_leisure"),
    ("dtt_leisure_luxury",     "Luxury and Premium Leisure Travel",                  2, "dtt_leisure"),

    # -- Business Travel --
    ("dtt_business",           "Business Travel",                                    1, None),
    ("dtt_business_corp",      "Corporate Travel Management",                        2, "dtt_business"),
    ("dtt_business_conf",      "Conference and Convention Travel",                   2, "dtt_business"),
    ("dtt_business_incent",    "Incentive and Reward Travel Programs",               2, "dtt_business"),
    ("dtt_business_bleisure",  "Bleisure and Extended Business Stays",               2, "dtt_business"),

    # -- Adventure Tourism --
    ("dtt_adventure",          "Adventure Tourism",                                  1, None),
    ("dtt_adventure_eco",      "Ecotourism and Nature-Based Tourism",                2, "dtt_adventure"),
    ("dtt_adventure_extreme",  "Extreme Sports and Expedition Tourism",              2, "dtt_adventure"),
    ("dtt_adventure_trek",     "Trekking and Hiking Tourism",                        2, "dtt_adventure"),
    ("dtt_adventure_safari",   "Wildlife Safari and Observation Tourism",            2, "dtt_adventure"),

    # -- Cultural Tourism --
    ("dtt_culture",            "Cultural Tourism",                                   1, None),
    ("dtt_culture_heritage",   "Heritage and Historical Site Tourism",               2, "dtt_culture"),
    ("dtt_culture_food",       "Culinary and Food Tourism",                          2, "dtt_culture"),
    ("dtt_culture_festival",   "Festival and Event-Based Tourism",                   2, "dtt_culture"),
    ("dtt_culture_religious",  "Religious and Pilgrimage Tourism",                   2, "dtt_culture"),

    # -- Medical Tourism --
    ("dtt_medical",            "Medical Tourism",                                    1, None),
    ("dtt_medical_surgery",    "Surgical and Elective Procedure Tourism",            2, "dtt_medical"),
    ("dtt_medical_dental",     "Dental Tourism Services",                            2, "dtt_medical"),
    ("dtt_medical_wellness",   "Wellness and Spa Tourism",                           2, "dtt_medical"),
    ("dtt_medical_rehab",      "Rehabilitation and Recovery Tourism",                2, "dtt_medical"),
]

_DOMAIN_ROW = (
    "domain_tourism_travel",
    "Tourism and Travel Types",
    "Tourism and travel sector types covering leisure travel, business travel, "
    "adventure tourism, cultural tourism, and medical tourism",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 5615 (Travel arrangement), 7211 (Traveler accommodation)
_NAICS_PREFIXES = ["5615", "7211"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific tourism/travel types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_tourism_travel(conn) -> int:
    """Ingest Tourism and Travel domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_tourism_travel'), and links NAICS 5615/7211 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_tourism_travel",
        "Tourism and Travel Types",
        "Tourism and travel sector types covering leisure travel, business travel, "
        "adventure tourism, cultural tourism, and medical tourism",
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

    parent_codes = {parent for _, _, _, parent in NODES if parent is not None}

    rows = [
        (
            "domain_tourism_travel",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_tourism_travel'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_tourism_travel'",
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
            [("naics_2022", code, "domain_tourism_travel", "primary") for code in naics_codes],
        )

    return count
