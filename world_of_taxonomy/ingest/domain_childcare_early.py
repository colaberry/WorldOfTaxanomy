"""Childcare and Early Education domain taxonomy ingester.

Childcare and early education taxonomy organizes care and learning settings for children:
  Center-Based   (dce_center*)   - daycare center, Montessori, church-based, corporate
  Family Care    (dce_family*)   - family child care home, group home, nanny share
  Head Start     (dce_headstart*) - Head Start, Early Head Start, migrant/seasonal
  Preschool      (dce_presch*)   - public pre-K, private preschool, cooperative
  After-School   (dce_after*)    - before/after school, summer camp, enrichment

Source: Office of Child Care (HHS) and NAICS 6244 subsectors. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CHILDCARE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Center-Based Care category --
    ("dce_center",           "Center-Based Child Care",                              1, None),
    ("dce_center_daycare",   "Licensed Daycare Center (full-day, year-round)",      2, "dce_center"),
    ("dce_center_montessori","Montessori Program (mixed-age, child-directed)",      2, "dce_center"),
    ("dce_center_faith",     "Faith-Based Child Care Center (church, synagogue)",   2, "dce_center"),
    ("dce_center_corporate", "Employer-Sponsored Child Care (on-site, backup)",     2, "dce_center"),
    ("dce_center_waldorf",   "Waldorf and Reggio Emilia Program",                   2, "dce_center"),

    # -- Family Child Care category --
    ("dce_family",           "Family Child Care",                                    1, None),
    ("dce_family_home",      "Family Child Care Home (single provider, 6-8 kids)",  2, "dce_family"),
    ("dce_family_group",     "Group Family Child Care Home (assistant, 10-14 kids)",2, "dce_family"),
    ("dce_family_nanny",     "Nanny and Au Pair Care (in-home, one-on-one)",        2, "dce_family"),
    ("dce_family_share",     "Nanny Share and Cooperative Care Arrangement",        2, "dce_family"),

    # -- Head Start Programs category --
    ("dce_headstart",          "Head Start Programs",                                1, None),
    ("dce_headstart_classic",  "Head Start (3-5 year-olds, center or home-based)",  2, "dce_headstart"),
    ("dce_headstart_early",    "Early Head Start (birth to 3, pregnant women)",     2, "dce_headstart"),
    ("dce_headstart_migrant",  "Migrant and Seasonal Head Start (MSHS)",            2, "dce_headstart"),
    ("dce_headstart_tribal",   "American Indian/Alaska Native Head Start (AIAN)",   2, "dce_headstart"),

    # -- Preschool Education category --
    ("dce_presch",           "Preschool Education",                                  1, None),
    ("dce_presch_public",    "State-Funded Pre-Kindergarten (universal pre-K)",     2, "dce_presch"),
    ("dce_presch_private",   "Private Preschool (tuition-based, independent)",      2, "dce_presch"),
    ("dce_presch_coop",      "Cooperative Preschool (parent-participation model)",  2, "dce_presch"),
    ("dce_presch_special",   "Early Intervention and Special Needs Preschool",      2, "dce_presch"),

    # -- After-School Programs category --
    ("dce_after",            "After-School and Out-of-School Programs",              1, None),
    ("dce_after_school",     "Before and After-School Care (school-age, K-5)",      2, "dce_after"),
    ("dce_after_summer",     "Summer Camp and Seasonal Program",                    2, "dce_after"),
    ("dce_after_enrich",     "Enrichment Program (STEM, arts, sports, tutoring)",   2, "dce_after"),
    ("dce_after_youth",      "Youth Development Program (Boys and Girls Club, 4-H)",2, "dce_after"),
]

_DOMAIN_ROW = (
    "domain_childcare_early",
    "Childcare and Early Education Types",
    "Center-based, family, Head Start, preschool and after-school childcare taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["6244"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific childcare types."""
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


async def ingest_domain_childcare_early(conn) -> int:
    """Ingest Childcare and Early Education domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_childcare_early'), and links NAICS 6244xxx nodes
    via node_taxonomy_link.

    Returns total childcare node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_childcare_early",
        "Childcare and Early Education Types",
        "Center-based, family, Head Start, preschool and after-school childcare taxonomy",
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

    parent_codes = {parent for _, _, _, parent in CHILDCARE_NODES if parent is not None}

    rows = [
        (
            "domain_childcare_early",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in CHILDCARE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CHILDCARE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_childcare_early'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_childcare_early'",
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
            [("naics_2022", code, "domain_childcare_early", "primary") for code in naics_codes],
        )

    return count
