"""Manufacturing Supply Chain Integration Model Types domain taxonomy ingester.

Classifies manufacturing companies by their supply chain integration strategy.
Orthogonal to process type, industry vertical, quality and ops model.
Used by supply chain strategists, M&A teams, and operations consultants
when evaluating make-vs-buy decisions and vertical integration tradeoffs.

Code prefix: dmfgsc_
System ID: domain_mfg_supply_chain
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
MFG_SUPPLY_CHAIN_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dmfgsc_vertical", "Vertical Integration Models", 1, None),
    ("dmfgsc_vertical_full", "Fully integrated: raw material through finished goods", 2, "dmfgsc_vertical"),
    ("dmfgsc_vertical_back", "Backward integration: owns upstream raw/intermediate supply", 2, "dmfgsc_vertical"),
    ("dmfgsc_vertical_fwd", "Forward integration: owns distribution and retail", 2, "dmfgsc_vertical"),
    ("dmfgsc_outsource", "Outsourcing and Contract Manufacturing Models", 1, None),
    ("dmfgsc_outsource_cm", "Contract manufacturing: full-build outsourced (EMS)", 2, "dmfgsc_outsource"),
    ("dmfgsc_outsource_jdm", "JDM (joint design manufacturing): co-designed product", 2, "dmfgsc_outsource"),
    ("dmfgsc_outsource_oem", "OEM: brand owns IP, contract manufacturer builds", 2, "dmfgsc_outsource"),
    ("dmfgsc_lean", "Lean and Just-in-Time Supply Models", 1, None),
    ("dmfgsc_lean_jit", "Just-in-Time (JIT): minimal inventory, pull-based replenishment", 2, "dmfgsc_lean"),
    ("dmfgsc_lean_kanban", "Kanban-driven replenishment with visual signals", 2, "dmfgsc_lean"),
    ("dmfgsc_lean_vmi", "Vendor-managed inventory (VMI): supplier manages stock levels", 2, "dmfgsc_lean"),
    ("dmfgsc_resilient", "Resilient and Diversified Supply Models", 1, None),
    ("dmfgsc_resilient_dual", "Dual sourcing: two qualified suppliers per critical component", 2, "dmfgsc_resilient"),
    ("dmfgsc_resilient_near", "Nearshoring and friend-shoring: geographic diversification", 2, "dmfgsc_resilient"),
    ("dmfgsc_resilient_buffer", "Strategic buffer stock for risk components", 2, "dmfgsc_resilient"),
    ("dmfgsc_digital", "Digital and Platform-Enabled Supply Models", 1, None),
    ("dmfgsc_digital_marketplace", "Digital manufacturing marketplace (Xometry, Fictiv, Hubs)", 2, "dmfgsc_digital"),
    ("dmfgsc_digital_twin", "Digital twin-enabled supply chain coordination", 2, "dmfgsc_digital"),
]

_DOMAIN_ROW = (
    "domain_mfg_supply_chain",
    "Manufacturing Supply Chain Integration Model Types",
    "Manufacturing supply chain integration and outsourcing model classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['31', '32', '33']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_mfg_supply_chain(conn) -> int:
    """Ingest Manufacturing Supply Chain Integration Model Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_mfg_supply_chain'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_mfg_supply_chain",
        "Manufacturing Supply Chain Integration Model Types",
        "Manufacturing supply chain integration and outsourcing model classification",
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

    parent_codes = {parent for _, _, _, parent in MFG_SUPPLY_CHAIN_NODES if parent is not None}

    rows = [
        (
            "domain_mfg_supply_chain",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in MFG_SUPPLY_CHAIN_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(MFG_SUPPLY_CHAIN_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_mfg_supply_chain'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_mfg_supply_chain'",
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
            [("naics_2022", code, "domain_mfg_supply_chain", "primary") for code in naics_codes],
        )

    return count
