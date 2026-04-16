"""Sports and Recreation domain taxonomy ingester.

Organizes sports and recreation industry types into categories aligned with
NAICS 7112 (Spectator Sports), NAICS 7131 (Amusement Parks and Arcades),
and NAICS 7139 (Other Amusement and Recreation Industries).

Code prefix: dsr_
Categories: professional sports, fitness and gym, outdoor recreation,
sports equipment, sports media.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SPORTS_RECREATION_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Professional Sports --
    ("dsr_pro",                "Professional Sports",                                1, None),
    ("dsr_pro_team",           "Professional Team Sports (leagues, franchises)",      2, "dsr_pro"),
    ("dsr_pro_individual",     "Individual Professional Sports (tennis, golf, MMA)",  2, "dsr_pro"),
    ("dsr_pro_esport",         "Esports and Competitive Gaming",                      2, "dsr_pro"),
    ("dsr_pro_racing",         "Motorsport and Racing Events",                        2, "dsr_pro"),
    ("dsr_pro_college",        "College Athletics and NCAA Programs",                  2, "dsr_pro"),

    # -- Fitness and Gym --
    ("dsr_fitness",            "Fitness and Gym",                                    1, None),
    ("dsr_fitness_club",       "Health Club and Gym Membership Facilities",            2, "dsr_fitness"),
    ("dsr_fitness_boutique",   "Boutique Fitness Studios (yoga, Pilates, cycling)",    2, "dsr_fitness"),
    ("dsr_fitness_personal",   "Personal Training and Coaching Services",              2, "dsr_fitness"),
    ("dsr_fitness_digital",    "Digital Fitness Platforms and Connected Equipment",     2, "dsr_fitness"),

    # -- Outdoor Recreation --
    ("dsr_outdoor",            "Outdoor Recreation",                                 1, None),
    ("dsr_outdoor_camp",       "Camping, Hiking and Trail Activities",                 2, "dsr_outdoor"),
    ("dsr_outdoor_water",      "Water Sports (kayaking, surfing, diving)",              2, "dsr_outdoor"),
    ("dsr_outdoor_snow",       "Snow Sports (skiing, snowboarding, ice skating)",       2, "dsr_outdoor"),
    ("dsr_outdoor_climb",      "Rock Climbing and Adventure Sports",                   2, "dsr_outdoor"),
    ("dsr_outdoor_fish",       "Recreational Fishing and Hunting Outfitters",           2, "dsr_outdoor"),

    # -- Sports Equipment --
    ("dsr_equip",              "Sports Equipment",                                   1, None),
    ("dsr_equip_apparel",      "Athletic Apparel and Footwear",                        2, "dsr_equip"),
    ("dsr_equip_gear",         "Sports Gear and Protective Equipment",                 2, "dsr_equip"),
    ("dsr_equip_tech",         "Sports Technology and Wearable Devices",               2, "dsr_equip"),
    ("dsr_equip_facility",     "Sports Facility and Turf Equipment",                   2, "dsr_equip"),

    # -- Sports Media --
    ("dsr_media",              "Sports Media",                                       1, None),
    ("dsr_media_broadcast",    "Sports Broadcasting and Streaming Rights",             2, "dsr_media"),
    ("dsr_media_betting",      "Sports Betting and Fantasy Sports Platforms",           2, "dsr_media"),
    ("dsr_media_data",         "Sports Data Analytics and Performance Tracking",        2, "dsr_media"),
    ("dsr_media_sponsor",      "Sports Sponsorship and Endorsement Management",         2, "dsr_media"),
]

_DOMAIN_ROW = (
    "domain_sports_recreation",
    "Sports and Recreation Types",
    "Professional sports, fitness and gym, outdoor recreation, "
    "sports equipment, and sports media domain taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link: 7112 (Spectator Sports), 7131 (Amusement Parks), 7139 (Other Rec)
_NAICS_PREFIXES = ["7112", "7131", "7139"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_sports_recreation(conn) -> int:
    """Ingest Sports and Recreation domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_sports_recreation'), and links NAICS 7112/7131/7139
    nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_sports_recreation",
        "Sports and Recreation Types",
        "Professional sports, fitness and gym, outdoor recreation, "
        "sports equipment, and sports media domain taxonomy",
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

    parent_codes = {parent for _, _, _, parent in SPORTS_RECREATION_NODES if parent is not None}

    rows = [
        (
            "domain_sports_recreation",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SPORTS_RECREATION_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SPORTS_RECREATION_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_sports_recreation'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_sports_recreation'",
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
            [("naics_2022", code, "domain_sports_recreation", "primary") for code in naics_codes],
        )

    return count
