"""Legal Practice domain taxonomy ingester.

Organizes legal practice area types aligned with NAICS 5411 (Legal services).

Code prefix: dlp_
Categories: Corporate Law, Litigation, Intellectual Property, Regulatory, Family Law.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
LEGAL_PRACTICE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Corporate Law --
    ("dlp_corp",             "Corporate Law",                                          1, None),
    ("dlp_corp_manda",       "Mergers and Acquisitions (buy-side, sell-side, SPAC)",   2, "dlp_corp"),
    ("dlp_corp_govern",      "Corporate Governance (board advisory, proxy fights)",    2, "dlp_corp"),
    ("dlp_corp_securities",  "Securities Law (IPO, SEC compliance, private placement)", 2, "dlp_corp"),
    ("dlp_corp_venture",     "Venture Capital and Startup Law (term sheets, SAFE)",    2, "dlp_corp"),
    ("dlp_corp_banking",     "Banking and Finance Law (loan agreements, syndication)", 2, "dlp_corp"),

    # -- Litigation --
    ("dlp_lit",              "Litigation",                                              1, None),
    ("dlp_lit_civil",        "Civil Litigation (breach of contract, tort claims)",      2, "dlp_lit"),
    ("dlp_lit_class",        "Class Action and Mass Tort Litigation",                   2, "dlp_lit"),
    ("dlp_lit_arbitration",  "Arbitration and Alternative Dispute Resolution",          2, "dlp_lit"),
    ("dlp_lit_appellate",    "Appellate Litigation (federal, state appeals courts)",    2, "dlp_lit"),

    # -- Intellectual Property --
    ("dlp_ip",               "Intellectual Property Law",                               1, None),
    ("dlp_ip_patent",        "Patent Law (prosecution, litigation, licensing)",         2, "dlp_ip"),
    ("dlp_ip_trademark",     "Trademark Law (registration, enforcement, dilution)",    2, "dlp_ip"),
    ("dlp_ip_copyright",     "Copyright Law (registration, fair use, DMCA)",           2, "dlp_ip"),
    ("dlp_ip_trade",         "Trade Secret Law (NDA, misappropriation, DTSA)",         2, "dlp_ip"),

    # -- Regulatory and Compliance --
    ("dlp_reg",              "Regulatory and Compliance Law",                           1, None),
    ("dlp_reg_antitrust",    "Antitrust and Competition Law (FTC, DOJ, merger review)", 2, "dlp_reg"),
    ("dlp_reg_environ",      "Environmental Law (EPA compliance, CERCLA, permitting)", 2, "dlp_reg"),
    ("dlp_reg_health",       "Healthcare Regulatory Law (HIPAA, Stark, Anti-Kickback)", 2, "dlp_reg"),
    ("dlp_reg_privacy",      "Privacy and Data Protection Law (CCPA, GDPR, COPPA)",    2, "dlp_reg"),
    ("dlp_reg_tax",          "Tax Law (federal, state, international tax planning)",    2, "dlp_reg"),

    # -- Family Law --
    ("dlp_family",           "Family Law",                                              1, None),
    ("dlp_family_divorce",   "Divorce and Separation (contested, collaborative)",       2, "dlp_family"),
    ("dlp_family_custody",   "Child Custody and Support (modification, enforcement)",   2, "dlp_family"),
    ("dlp_family_estate",    "Estate Planning and Probate (trusts, wills, guardianship)", 2, "dlp_family"),
    ("dlp_family_adoption",  "Adoption Law (domestic, international, stepparent)",      2, "dlp_family"),
]

_DOMAIN_ROW = (
    "domain_legal_practice",
    "Legal Practice Types",
    "Corporate law, litigation, intellectual property, regulatory, "
    "and family law practice area taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefix: 5411 (Legal services)
_NAICS_PREFIXES = ["5411"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific legal practice types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_legal_practice(conn) -> int:
    """Ingest Legal Practice domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_legal_practice'), and links NAICS 5411 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_legal_practice",
        "Legal Practice Types",
        "Corporate law, litigation, intellectual property, regulatory, "
        "and family law practice area taxonomy",
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

    parent_codes = {parent for _, _, _, parent in LEGAL_PRACTICE_NODES if parent is not None}

    rows = [
        (
            "domain_legal_practice",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in LEGAL_PRACTICE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(LEGAL_PRACTICE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_legal_practice'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_legal_practice'",
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
            [("naics_2022", code, "domain_legal_practice", "primary") for code in naics_codes],
        )

    return count
