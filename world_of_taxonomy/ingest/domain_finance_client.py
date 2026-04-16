"""Finance Client and Investor Segment Types domain taxonomy ingester.

Classifies financial services clients and investors by segment.
Orthogonal to product/instrument type, market structure, and regulatory framework.
Aligned with SEC Reg BI suitability standards, MiFID II client categorization
(retail, professional, eligible counterparty), and PwC wealth segment taxonomy.
Used by product managers, compliance officers, CRM systems and sales teams.

Code prefix: dfnclt_
System ID: domain_finance_client
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
FINANCE_CLIENT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dfnclt_retail", "Retail and Mass-Market Clients", 1, None),
    ("dfnclt_retail_mass", "Mass market: <$100k investable assets, basic products", 2, "dfnclt_retail"),
    ("dfnclt_retail_emerging", "Emerging affluent / mass-affluent: $100k-$250k assets", 2, "dfnclt_retail"),
    ("dfnclt_retail_digital", "Digital-first retail investors (fintech, robo-advisor, neobank)", 2, "dfnclt_retail"),
    ("dfnclt_hnw", "High-Net-Worth (HNW) Clients", 1, None),
    ("dfnclt_hnw_hnwi", "HNWI: $1M-$5M investable assets (private banking, WM)", 2, "dfnclt_hnw"),
    ("dfnclt_hnw_vhnwi", "Very HNWI: $5M-$30M (multi-family office, tailored products)", 2, "dfnclt_hnw"),
    ("dfnclt_hnw_uhnw", "Ultra HNWI: $30M+ (family office, private credit, alternatives)", 2, "dfnclt_hnw"),
    ("dfnclt_institutional", "Institutional Investors", 1, None),
    ("dfnclt_institutional_pension", "Pension funds (DB/DC, sovereign pension, public employee)", 2, "dfnclt_institutional"),
    ("dfnclt_institutional_endow", "Endowments and foundations (university endowments, community foundations)", 2, "dfnclt_institutional"),
    ("dfnclt_institutional_sovereign", "Sovereign wealth funds (GIC, ADIA, Norges Bank Investment Mgmt)", 2, "dfnclt_institutional"),
    ("dfnclt_institutional_ins", "Insurance company general accounts (P&C, life, re-insurance)", 2, "dfnclt_institutional"),
    ("dfnclt_corporate", "Corporate and Treasury Clients", 1, None),
    ("dfnclt_corporate_treasury", "Corporate treasury (cash mgmt, FX hedging, liquidity)", 2, "dfnclt_corporate"),
    ("dfnclt_corporate_sme", "SME and middle-market corporate banking clients", 2, "dfnclt_corporate"),
    ("dfnclt_public", "Government and Public Sector Clients", 1, None),
    ("dfnclt_public_muni", "Municipal issuers and public finance (tax-exempt bonds)", 2, "dfnclt_public"),
    ("dfnclt_public_dfi", "Development finance institutions (DFI, MDB, EXIM banks)", 2, "dfnclt_public"),
]

_DOMAIN_ROW = (
    "domain_finance_client",
    "Finance Client and Investor Segment Types",
    "Financial services client and investor segment classification: retail, HNW, institutional, corporate",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['52', '5221', '5222', '5231']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_finance_client(conn) -> int:
    """Ingest Finance Client and Investor Segment Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_finance_client'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_finance_client",
        "Finance Client and Investor Segment Types",
        "Financial services client and investor segment classification: retail, HNW, institutional, corporate",
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

    parent_codes = {parent for _, _, _, parent in FINANCE_CLIENT_NODES if parent is not None}

    rows = [
        (
            "domain_finance_client",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in FINANCE_CLIENT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(FINANCE_CLIENT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_finance_client'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_finance_client'",
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
            [("naics_2022", code, "domain_finance_client", "primary") for code in naics_codes],
        )

    return count
