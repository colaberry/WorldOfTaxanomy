"""Nonprofit and Social Enterprise domain taxonomy ingester.

Organizes nonprofit and social enterprise types into categories aligned with
NAICS 8131 (Religious Organizations), NAICS 8132 (Grantmaking and Giving),
NAICS 8133 (Social Advocacy), and NAICS 8134 (Civic and Social Organizations).

Code prefix: dns_
Categories: charitable organizations, foundations, social enterprises,
advocacy groups, international development.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
NONPROFIT_SOCIAL_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Charitable Organizations --
    ("dns_charity",            "Charitable Organizations",                           1, None),
    ("dns_charity_human",      "Human Services Charities (food banks, shelters)",     2, "dns_charity"),
    ("dns_charity_health",     "Health and Medical Charities",                        2, "dns_charity"),
    ("dns_charity_education",  "Educational Charities and Scholarship Funds",         2, "dns_charity"),
    ("dns_charity_religious",  "Religious Charitable Organizations",                  2, "dns_charity"),
    ("dns_charity_disaster",   "Disaster Relief and Emergency Response",              2, "dns_charity"),

    # -- Foundations --
    ("dns_found",              "Foundations",                                        1, None),
    ("dns_found_private",      "Private Foundations (family, corporate)",              2, "dns_found"),
    ("dns_found_community",    "Community Foundations",                                2, "dns_found"),
    ("dns_found_operating",    "Operating Foundations (direct program delivery)",      2, "dns_found"),
    ("dns_found_donor",        "Donor-Advised Fund Sponsors",                         2, "dns_found"),

    # -- Social Enterprises --
    ("dns_social",             "Social Enterprises",                                 1, None),
    ("dns_social_bcorp",       "B Corporations and Benefit Corporations",              2, "dns_social"),
    ("dns_social_coop",        "Cooperative Enterprises and Mutual Organizations",     2, "dns_social"),
    ("dns_social_micro",       "Microfinance and Community Development Finance",       2, "dns_social"),
    ("dns_social_impact",      "Impact Investment Vehicles and Funds",                 2, "dns_social"),

    # -- Advocacy Groups --
    ("dns_advocacy",           "Advocacy Groups",                                    1, None),
    ("dns_advocacy_environ",   "Environmental Advocacy and Conservation",              2, "dns_advocacy"),
    ("dns_advocacy_rights",    "Civil Rights and Human Rights Organizations",          2, "dns_advocacy"),
    ("dns_advocacy_policy",    "Public Policy Think Tanks and Research Institutes",    2, "dns_advocacy"),
    ("dns_advocacy_labor",     "Labor Unions and Worker Advocacy",                     2, "dns_advocacy"),

    # -- International Development --
    ("dns_intl",               "International Development",                          1, None),
    ("dns_intl_ngo",           "International NGOs and Humanitarian Aid",              2, "dns_intl"),
    ("dns_intl_bilateral",     "Bilateral Development Agencies (USAID, DFID)",         2, "dns_intl"),
    ("dns_intl_multilateral",  "Multilateral Organizations (UN, World Bank)",          2, "dns_intl"),
    ("dns_intl_volunteer",     "International Volunteer and Service Programs",         2, "dns_intl"),
    ("dns_intl_capacity",      "Capacity Building and Technical Assistance",           2, "dns_intl"),
]

_DOMAIN_ROW = (
    "domain_nonprofit_social",
    "Nonprofit and Social Enterprise Types",
    "Charitable organizations, foundations, social enterprises, "
    "advocacy groups, and international development domain taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link: 8131 (Religious), 8132 (Grantmaking), 8133 (Social Advocacy),
# 8134 (Civic and Social Organizations)
_NAICS_PREFIXES = ["8131", "8132", "8133", "8134"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_nonprofit_social(conn) -> int:
    """Ingest Nonprofit and Social Enterprise domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_nonprofit_social'), and links NAICS 8131/8132/8133/8134
    nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_nonprofit_social",
        "Nonprofit and Social Enterprise Types",
        "Charitable organizations, foundations, social enterprises, "
        "advocacy groups, and international development domain taxonomy",
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

    parent_codes = {parent for _, _, _, parent in NONPROFIT_SOCIAL_NODES if parent is not None}

    rows = [
        (
            "domain_nonprofit_social",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in NONPROFIT_SOCIAL_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(NONPROFIT_SOCIAL_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_nonprofit_social'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_nonprofit_social'",
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
            [("naics_2022", code, "domain_nonprofit_social", "primary") for code in naics_codes],
        )

    return count
