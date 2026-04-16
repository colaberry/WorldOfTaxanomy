"""Defence Technology Readiness Levels domain taxonomy ingester.

Classifies defence technologies by readiness maturity per the NATO TRL framework
(aligned with DoD, ESA, and EU Horizon definitions). Orthogonal to capability type
and programme acquisition category. Used by R&D programme managers, SBIR/STTR
officers, venture investors, and innovation office staff to gauge investment risk
and transition readiness.

Code prefix: ddftrl_
System ID: domain_defence_trl
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
DEFENCE_TRL_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ddftrl_basic", "Basic Research (TRL 1-2)", 1, None),
    ("ddftrl_basic_trl1", "TRL 1 - Basic principles observed and reported", 2, "ddftrl_basic"),
    ("ddftrl_basic_trl2", "TRL 2 - Technology concept and/or application formulated", 2, "ddftrl_basic"),
    ("ddftrl_applied", "Applied Research and Proof of Concept (TRL 3-4)", 1, None),
    ("ddftrl_applied_trl3", "TRL 3 - Analytical and experimental critical function", 2, "ddftrl_applied"),
    ("ddftrl_applied_trl4", "TRL 4 - Component validation in laboratory environment", 2, "ddftrl_applied"),
    ("ddftrl_develop", "Technology Development and Validation (TRL 5-6)", 1, None),
    ("ddftrl_develop_trl5", "TRL 5 - Component validation in relevant environment", 2, "ddftrl_develop"),
    ("ddftrl_develop_trl6", "TRL 6 - System/subsystem prototype demo in relevant environment", 2, "ddftrl_develop"),
    ("ddftrl_demo", "System Demonstration and Test (TRL 7-8)", 1, None),
    ("ddftrl_demo_trl7", "TRL 7 - System prototype demo in operational environment", 2, "ddftrl_demo"),
    ("ddftrl_demo_trl8", "TRL 8 - System complete and qualified through test/demo", 2, "ddftrl_demo"),
    ("ddftrl_deploy", "Full Deployment and Operations (TRL 9)", 1, None),
    ("ddftrl_deploy_trl9", "TRL 9 - Actual system proven through successful mission operations", 2, "ddftrl_deploy"),
    ("ddftrl_deploy_sus", "Sustainment and upgrade of deployed operational system", 2, "ddftrl_deploy"),
]

_DOMAIN_ROW = (
    "domain_defence_trl",
    "Defence Technology Readiness Levels",
    "Technology readiness level (TRL) classification for defence and aerospace R&D programmes",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3364', '5413', '5417']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_defence_trl(conn) -> int:
    """Ingest Defence Technology Readiness Levels domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_defence_trl'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_defence_trl",
        "Defence Technology Readiness Levels",
        "Technology readiness level (TRL) classification for defence and aerospace R&D programmes",
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

    parent_codes = {parent for _, _, _, parent in DEFENCE_TRL_NODES if parent is not None}

    rows = [
        (
            "domain_defence_trl",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in DEFENCE_TRL_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(DEFENCE_TRL_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_defence_trl'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_defence_trl'",
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
            [("naics_2022", code, "domain_defence_trl", "primary") for code in naics_codes],
        )

    return count
