"""Pet and Animal Services domain taxonomy ingester.

Organizes pet and animal industry types into categories aligned with
NAICS 5419 (Other Professional Services) and NAICS 4539 (Other
Miscellaneous Store Retailers).

Code prefix: dpa_
Categories: veterinary services, pet retail, pet grooming and boarding,
animal feed, pet tech.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
PET_ANIMAL_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Veterinary Services --
    ("dpa_vet",                "Veterinary Services",                                1, None),
    ("dpa_vet_companion",      "Companion Animal Veterinary Care (dogs, cats)",       2, "dpa_vet"),
    ("dpa_vet_equine",         "Equine Veterinary Services",                          2, "dpa_vet"),
    ("dpa_vet_exotic",         "Exotic and Avian Veterinary Care",                    2, "dpa_vet"),
    ("dpa_vet_emergency",      "Emergency and Specialty Veterinary Clinics",          2, "dpa_vet"),

    # -- Pet Retail --
    ("dpa_retail",             "Pet Retail",                                         1, None),
    ("dpa_retail_food",        "Pet Food and Treats Retail",                           2, "dpa_retail"),
    ("dpa_retail_supply",      "Pet Supplies and Accessories",                         2, "dpa_retail"),
    ("dpa_retail_pharmacy",    "Pet Pharmacy and Health Products",                     2, "dpa_retail"),
    ("dpa_retail_ecomm",       "Online Pet Retail and Subscription Boxes",             2, "dpa_retail"),

    # -- Pet Grooming and Boarding --
    ("dpa_care",               "Pet Grooming and Boarding",                           1, None),
    ("dpa_care_groom",         "Pet Grooming and Spa Services",                       2, "dpa_care"),
    ("dpa_care_board",         "Pet Boarding and Kennel Facilities",                   2, "dpa_care"),
    ("dpa_care_daycare",       "Pet Daycare and Socialization Centers",                2, "dpa_care"),
    ("dpa_care_walk",          "Dog Walking and Pet Sitting Services",                 2, "dpa_care"),

    # -- Animal Feed --
    ("dpa_feed",               "Animal Feed",                                        1, None),
    ("dpa_feed_premium",       "Premium and Specialty Pet Food Manufacturing",         2, "dpa_feed"),
    ("dpa_feed_raw",           "Raw and Fresh Pet Food Production",                    2, "dpa_feed"),
    ("dpa_feed_supplement",    "Pet Nutritional Supplements and Vitamins",             2, "dpa_feed"),

    # -- Pet Tech --
    ("dpa_tech",               "Pet Tech",                                           1, None),
    ("dpa_tech_wearable",      "Pet Wearables and GPS Trackers",                      2, "dpa_tech"),
    ("dpa_tech_televet",       "Telehealth and Virtual Veterinary Platforms",          2, "dpa_tech"),
    ("dpa_tech_smart",         "Smart Pet Feeders and Automated Care Devices",         2, "dpa_tech"),
    ("dpa_tech_insure",        "Pet Insurance Platforms and Services",                 2, "dpa_tech"),
]

_DOMAIN_ROW = (
    "domain_pet_animal",
    "Pet and Animal Services Types",
    "Veterinary services, pet retail, grooming and boarding, "
    "animal feed, and pet tech domain taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link: 5419 (Other Professional Services), 4539 (Other Misc Retail)
_NAICS_PREFIXES = ["5419", "4539"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_pet_animal(conn) -> int:
    """Ingest Pet and Animal Services domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_pet_animal'), and links NAICS 5419/4539 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_pet_animal",
        "Pet and Animal Services Types",
        "Veterinary services, pet retail, grooming and boarding, "
        "animal feed, and pet tech domain taxonomy",
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

    parent_codes = {parent for _, _, _, parent in PET_ANIMAL_NODES if parent is not None}

    rows = [
        (
            "domain_pet_animal",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in PET_ANIMAL_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(PET_ANIMAL_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_pet_animal'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_pet_animal'",
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
            [("naics_2022", code, "domain_pet_animal", "primary") for code in naics_codes],
        )

    return count
