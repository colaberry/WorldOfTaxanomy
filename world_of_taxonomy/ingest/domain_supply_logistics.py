"""Supply Chain Logistics Service Model Types domain taxonomy ingester.

Classifies supply chain operations by the logistics service model used.
Orthogonal to technology platform type and risk category type.
Based on Council of Supply Chain Management Professionals (CSCMP) definitions,
and Armstrong & Associates logistics outsourcing taxonomy.
Used by supply chain executives, logistics procurement teams, and
3PL/4PL providers positioning their service offerings.

Code prefix: dsclog_
System ID: domain_supply_logistics
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SUPPLY_LOGISTICS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dsclog_firstparty", "1PL: First-Party Logistics (Self-Managed)", 1, None),
    ("dsclog_firstparty_shipper", "Shipper self-manages own fleet and warehousing (private fleet)", 2, "dsclog_firstparty"),
    ("dsclog_firstparty_captive", "Captive logistics subsidiary (in-house logistics arm)", 2, "dsclog_firstparty"),
    ("dsclog_secondparty", "2PL: Second-Party Logistics (Asset-Based Carriers)", 1, None),
    ("dsclog_secondparty_carrier", "Asset-based carrier (trucking, rail, ocean, air carriers)", 2, "dsclog_secondparty"),
    ("dsclog_secondparty_warehouse", "Public warehouse and distribution center operator", 2, "dsclog_secondparty"),
    ("dsclog_thirdparty", "3PL: Third-Party Logistics (Outsourced Logistics)", 1, None),
    ("dsclog_thirdparty_transport", "Transportation-based 3PL (freight brokerage + managed transport)", 2, "dsclog_thirdparty"),
    ("dsclog_thirdparty_warehouse", "Warehouse-based 3PL (fulfillment center, value-added services)", 2, "dsclog_thirdparty"),
    ("dsclog_thirdparty_integrated", "Integrated 3PL (full-service supply chain outsourcing, XPO, DSV)", 2, "dsclog_thirdparty"),
    ("dsclog_fourthparty", "4PL: Fourth-Party Logistics (Lead Logistics Provider)", 1, None),
    ("dsclog_fourthparty_llp", "LLP / 4PL: manages network of 3PLs on shipper's behalf", 2, "dsclog_fourthparty"),
    ("dsclog_fourthparty_controlower", "Control tower: 4PL providing end-to-end visibility and orchestration", 2, "dsclog_fourthparty"),
    ("dsclog_fifthparty", "5PL: Digital and Platform-Based Logistics", 1, None),
    ("dsclog_fifthparty_platform", "Digital freight platform aggregating carriers (Flexport, project44)", 2, "dsclog_fifthparty"),
    ("dsclog_fifthparty_marketplace", "Open logistics marketplace (Amazon Shipping, Uber Freight, Convoy)", 2, "dsclog_fifthparty"),
    ("dsclog_contract", "Contract Logistics and Dedicated Services", 1, None),
    ("dsclog_contract_dedicated", "Dedicated contract carriage (DCC): fleet dedicated to one shipper", 2, "dsclog_contract"),
    ("dsclog_contract_managed", "Managed logistics services (outsourced logistics department)", 2, "dsclog_contract"),
]

_DOMAIN_ROW = (
    "domain_supply_logistics",
    "Supply Chain Logistics Service Model Types",
    "Logistics service provider model classification: 1PL through 5PL and contract logistics",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['48', '49', '4885', '5413']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_supply_logistics(conn) -> int:
    """Ingest Supply Chain Logistics Service Model Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_supply_logistics'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_supply_logistics",
        "Supply Chain Logistics Service Model Types",
        "Logistics service provider model classification: 1PL through 5PL and contract logistics",
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

    parent_codes = {parent for _, _, _, parent in SUPPLY_LOGISTICS_NODES if parent is not None}

    rows = [
        (
            "domain_supply_logistics",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SUPPLY_LOGISTICS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SUPPLY_LOGISTICS_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_supply_logistics'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_supply_logistics'",
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
            [("naics_2022", code, "domain_supply_logistics", "primary") for code in naics_codes],
        )

    return count
