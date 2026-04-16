"""Professional Services Billing and Fee Arrangement Types domain taxonomy ingester.

Classifies professional services firms by their primary billing and fee model.
Orthogonal to firm type, delivery model, and client segment.
Used by legal ops, procurement, management consulting buyers, and
professional services firm CFOs when designing pricing strategies.

Code prefix: dprofbil_
System ID: domain_prof_billing
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
PROF_BILLING_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dprofbil_hourly", "Hourly and Time-and-Materials Billing", 1, None),
    ("dprofbil_hourly_standard", "Standard hourly billing (law firms, accounting, consulting)", 2, "dprofbil_hourly"),
    ("dprofbil_hourly_blended", "Blended hourly rate (team rate, not individual partner/associate)", 2, "dprofbil_hourly"),
    ("dprofbil_hourly_tam", "Time-and-materials: hours + direct costs passed through", 2, "dprofbil_hourly"),
    ("dprofbil_fixed", "Fixed Fee and Project-Based Billing", 1, None),
    ("dprofbil_fixed_project", "Fixed project fee (defined scope, deliverable-based)", 2, "dprofbil_fixed"),
    ("dprofbil_fixed_capped", "Capped fee with downside protection for client", 2, "dprofbil_fixed"),
    ("dprofbil_fixed_phased", "Phased fixed fees (milestone payments tied to deliverables)", 2, "dprofbil_fixed"),
    ("dprofbil_retainer", "Retainer and Subscription Models", 1, None),
    ("dprofbil_retainer_monthly", "Monthly retainer: ongoing availability/counsel at fixed fee", 2, "dprofbil_retainer"),
    ("dprofbil_retainer_banked", "Banked hours retainer: pre-purchase hours used as needed", 2, "dprofbil_retainer"),
    ("dprofbil_retainer_saas", "SaaS-style annual subscription (LegalTech, consulting platforms)", 2, "dprofbil_retainer"),
    ("dprofbil_contingency", "Contingency and Success-Fee Models", 1, None),
    ("dprofbil_contingency_legal", "Legal contingency (% of recovery in litigation, no win no fee)", 2, "dprofbil_contingency"),
    ("dprofbil_contingency_ma", "M&A success fee (% of deal value, Lehman formula variants)", 2, "dprofbil_contingency"),
    ("dprofbil_contingency_exec", "Executive search contingency (% of first-year comp on placement)", 2, "dprofbil_contingency"),
    ("dprofbil_valuebased", "Value-Based and Outcome-Linked Billing", 1, None),
    ("dprofbil_valuebased_roi", "ROI-sharing: fee linked to measurable client outcome improvement", 2, "dprofbil_valuebased"),
    ("dprofbil_valuebased_hybrid", "Hybrid: base fixed fee + success uplift based on outcomes", 2, "dprofbil_valuebased"),
]

_DOMAIN_ROW = (
    "domain_prof_billing",
    "Professional Services Billing and Fee Arrangement Types",
    "Professional services firm billing model and fee arrangement classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['54', '5411', '5412', '5413', '5416']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_prof_billing(conn) -> int:
    """Ingest Professional Services Billing and Fee Arrangement Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_prof_billing'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_prof_billing",
        "Professional Services Billing and Fee Arrangement Types",
        "Professional services firm billing model and fee arrangement classification",
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

    parent_codes = {parent for _, _, _, parent in PROF_BILLING_NODES if parent is not None}

    rows = [
        (
            "domain_prof_billing",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in PROF_BILLING_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(PROF_BILLING_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_prof_billing'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_prof_billing'",
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
            [("naics_2022", code, "domain_prof_billing", "primary") for code in naics_codes],
        )

    return count
