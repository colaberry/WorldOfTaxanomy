"""Space Regulatory and Licensing Framework Types domain taxonomy ingester.

Classifies the regulatory and licensing regime applicable to space activities.
Orthogonal to mission type and orbital class. Used by satellite operators,
launch service providers, legal/regulatory teams, and government space agencies.
Key frameworks: Outer Space Treaty (OST), ITU Radio Regulations, FCC Part 25,
FAA AST launch licenses, UK Space Industry Act 2018, national space laws.

Code prefix: dspreg_
System ID: domain_space_regulatory
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SPACE_REGULATORY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dspreg_spectrum", "Radio Frequency / Spectrum Coordination", 1, None),
    ("dspreg_spectrum_itu", "ITU Radio Regulations coordination (filing, MIFR notification)", 2, "dspreg_spectrum"),
    ("dspreg_spectrum_fcc", "FCC Part 25 satellite communications licensing (US)", 2, "dspreg_spectrum"),
    ("dspreg_spectrum_ofcom", "UK Ofcom earth station and satellite authorizations", 2, "dspreg_spectrum"),
    ("dspreg_spectrum_rr", "ITU Radio Regulations Article 44 - equitable access", 2, "dspreg_spectrum"),
    ("dspreg_launch", "Launch and Re-entry Licensing", 1, None),
    ("dspreg_launch_faa", "FAA AST launch operator and vehicle licenses (14 CFR Part 450)", 2, "dspreg_launch"),
    ("dspreg_launch_uk", "UK Space Industry Act 2018 - launch from UK territory", 2, "dspreg_launch"),
    ("dspreg_launch_eu", "EU national launch authorizations (France, Germany, Netherlands)", 2, "dspreg_launch"),
    ("dspreg_debris", "Space Debris Mitigation and Orbital Sustainability", 1, None),
    ("dspreg_debris_iadc", "IADC 25-year rule and passivation requirements", 2, "dspreg_debris"),
    ("dspreg_debris_fcc", "FCC 5-year deorbit rule (2022 Order for LEO sats)", 2, "dspreg_debris"),
    ("dspreg_debris_esa", "ESA Zero Debris Charter and sustainable design criteria", 2, "dspreg_debris"),
    ("dspreg_national", "National Space Legislation and Authorization", 1, None),
    ("dspreg_national_ost", "Outer Space Treaty Art VI - national authorization and supervision", 2, "dspreg_national"),
    ("dspreg_national_liab", "Liability Convention - state liability for damage by space objects", 2, "dspreg_national"),
    ("dspreg_national_remote", "Remote sensing data laws (US NOAA, Landsat, commercial policy)", 2, "dspreg_national"),
]

_DOMAIN_ROW = (
    "domain_space_regulatory",
    "Space Regulatory and Licensing Framework Types",
    "Space activity regulatory framework: ITU frequency, FCC licensing, FAA AST launch permits, national frameworks",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3364', '4812', '5191']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_space_regulatory(conn) -> int:
    """Ingest Space Regulatory and Licensing Framework Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_space_regulatory'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_space_regulatory",
        "Space Regulatory and Licensing Framework Types",
        "Space activity regulatory framework: ITU frequency, FCC licensing, FAA AST launch permits, national frameworks",
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

    parent_codes = {parent for _, _, _, parent in SPACE_REGULATORY_NODES if parent is not None}

    rows = [
        (
            "domain_space_regulatory",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SPACE_REGULATORY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SPACE_REGULATORY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_space_regulatory'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_space_regulatory'",
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
            [("naics_2022", code, "domain_space_regulatory", "primary") for code in naics_codes],
        )

    return count
