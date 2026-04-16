"""Health Care Delivery and Payment Model Types domain taxonomy ingester.

Classifies health care delivery and payment models.
Orthogonal to care setting, payer type and specialty service line.
Based on CMS Innovation Center model taxonomy, NCQA accreditation categories,
and HFMA payment model classification.
Used by health system strategists, payers, value-based care consultants,
and population health managers.

Code prefix: dhlcdlv_
System ID: domain_health_delivery
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
HEALTH_DELIVERY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dhlcdlv_ffs", "Fee-for-Service (FFS) Delivery Models", 1, None),
    ("dhlcdlv_ffs_traditional", "Traditional FFS: unbundled service-by-service reimbursement", 2, "dhlcdlv_ffs"),
    ("dhlcdlv_ffs_perf", "Pay-for-performance (P4P): FFS with quality bonuses/penalties", 2, "dhlcdlv_ffs"),
    ("dhlcdlv_capitation", "Capitation and Per-Member-Per-Month (PMPM) Models", 1, None),
    ("dhlcdlv_capitation_full", "Full capitation: fixed payment covers all services", 2, "dhlcdlv_capitation"),
    ("dhlcdlv_capitation_partial", "Partial / specialty capitation for defined scope of care", 2, "dhlcdlv_capitation"),
    ("dhlcdlv_vbc", "Value-Based Care and Alternative Payment Models", 1, None),
    ("dhlcdlv_vbc_bundled", "Bundled payments (episode-of-care payment, CMS BPCI)", 2, "dhlcdlv_vbc"),
    ("dhlcdlv_vbc_sharedsvg", "Shared savings (ACO MSSP, REACH ACO, upside/downside risk)", 2, "dhlcdlv_vbc"),
    ("dhlcdlv_vbc_globalbudget", "Global budget / population-based payment (Maryland model)", 2, "dhlcdlv_vbc"),
    ("dhlcdlv_managed", "Managed Care and HMO Models", 1, None),
    ("dhlcdlv_managed_hmo", "Health Maintenance Organization (HMO): gated primary care model", 2, "dhlcdlv_managed"),
    ("dhlcdlv_managed_ppo", "Preferred Provider Organization (PPO): network with out-of-network option", 2, "dhlcdlv_managed"),
    ("dhlcdlv_managed_epo", "Exclusive Provider Organization (EPO): in-network only but no gatekeeper", 2, "dhlcdlv_managed"),
    ("dhlcdlv_direct", "Direct and Concierge Care Models", 1, None),
    ("dhlcdlv_direct_dpc", "Direct Primary Care (DPC): flat monthly fee, no insurance billing", 2, "dhlcdlv_direct"),
    ("dhlcdlv_direct_concierge", "Concierge medicine: retainer fee + insurance billing hybrid", 2, "dhlcdlv_direct"),
    ("dhlcdlv_direct_clinic", "Employer-sponsored or retail clinic (CVS MinuteClinic, on-site clinic)", 2, "dhlcdlv_direct"),
]

_DOMAIN_ROW = (
    "domain_health_delivery",
    "Health Care Delivery and Payment Model Types",
    "Health care delivery and reimbursement model classification: FFS, capitation, VBC, ACO, direct care",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['62', '6211', '6221', '6231']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_health_delivery(conn) -> int:
    """Ingest Health Care Delivery and Payment Model Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_health_delivery'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_health_delivery",
        "Health Care Delivery and Payment Model Types",
        "Health care delivery and reimbursement model classification: FFS, capitation, VBC, ACO, direct care",
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

    parent_codes = {parent for _, _, _, parent in HEALTH_DELIVERY_NODES if parent is not None}

    rows = [
        (
            "domain_health_delivery",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in HEALTH_DELIVERY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(HEALTH_DELIVERY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_health_delivery'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_health_delivery'",
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
            [("naics_2022", code, "domain_health_delivery", "primary") for code in naics_codes],
        )

    return count
