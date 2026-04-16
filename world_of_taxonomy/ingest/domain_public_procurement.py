"""Public Administration Procurement Method Types domain taxonomy ingester.

Classifies government procurement by contracting approach and competitive method.
Orthogonal to funding source type and administrative type.
Based on FAR (Federal Acquisition Regulation), OECD government procurement
principles, and World Bank procurement framework.
Used by government contracting officers, contractors, legal teams, and
public procurement reform advisors.

Code prefix: dpubprc_
System ID: domain_public_procurement
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
PUBLIC_PROCUREMENT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dpubprc_competitive", "Competitive Procurement Methods", 1, None),
    ("dpubprc_competitive_open", "Open competitive tendering (IFB, RFP, RFQ - full and open competition)", 2, "dpubprc_competitive"),
    ("dpubprc_competitive_sealed", "Sealed bidding (Invitation for Bids, lowest price technically acceptable)", 2, "dpubprc_competitive"),
    ("dpubprc_competitive_bestvalue", "Best-value competition (tradeoff between technical and price)", 2, "dpubprc_competitive"),
    ("dpubprc_restricted", "Restricted and Pre-Qualified Procurement", 1, None),
    ("dpubprc_restricted_prequalify", "Pre-qualification shortlisting before RFP stage", 2, "dpubprc_restricted"),
    ("dpubprc_restricted_twostage", "Two-stage / two-envelope process (technical then commercial)", 2, "dpubprc_restricted"),
    ("dpubprc_noncompetitive", "Non-Competitive and Sole-Source Methods", 1, None),
    ("dpubprc_noncompetitive_sole", "Sole-source (single award without competition, justified exception)", 2, "dpubprc_noncompetitive"),
    ("dpubprc_noncompetitive_emergency", "Emergency procurement (disaster, urgent operational need)", 2, "dpubprc_noncompetitive"),
    ("dpubprc_framework", "Framework and Indefinite Delivery Vehicle (IDV) Methods", 1, None),
    ("dpubprc_framework_gwac", "GWAC / IDIQ multi-award contract (GSA Schedules, SEWP, OASIS)", 2, "dpubprc_framework"),
    ("dpubprc_framework_blanket", "Blanket Purchase Agreement (BPA) for recurring needs", 2, "dpubprc_framework"),
    ("dpubprc_setaside", "Set-Aside and Preferential Procurement Programs", 1, None),
    ("dpubprc_setaside_small", "Small business set-aside (SBA 8(a), SDVOSB, HUBZone, WOSB)", 2, "dpubprc_setaside"),
    ("dpubprc_setaside_sbir", "SBIR/STTR innovation programs", 2, "dpubprc_setaside"),
    ("dpubprc_ppp", "Public-Private Partnership (PPP) Procurement", 1, None),
    ("dpubprc_ppp_concession", "Concession-based PPP (operator collects user fees)", 2, "dpubprc_ppp"),
    ("dpubprc_ppp_availability", "Availability payment PPP (government pays based on service availability)", 2, "dpubprc_ppp"),
]

_DOMAIN_ROW = (
    "domain_public_procurement",
    "Public Administration Procurement Method Types",
    "Government and public sector procurement method and contracting approach classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['92', '9211', '9221', '9231']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_public_procurement(conn) -> int:
    """Ingest Public Administration Procurement Method Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_public_procurement'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_public_procurement",
        "Public Administration Procurement Method Types",
        "Government and public sector procurement method and contracting approach classification",
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

    parent_codes = {parent for _, _, _, parent in PUBLIC_PROCUREMENT_NODES if parent is not None}

    rows = [
        (
            "domain_public_procurement",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in PUBLIC_PROCUREMENT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(PUBLIC_PROCUREMENT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_public_procurement'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_public_procurement'",
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
            [("naics_2022", code, "domain_public_procurement", "primary") for code in naics_codes],
        )

    return count
