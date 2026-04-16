"""Semiconductor Application End-Market Types domain taxonomy ingester.

Classifies semiconductor products and companies by their primary end-market.
Orthogonal to device type and business model (domain_semiconductor).
Aligned with SIA (Semiconductor Industry Association) market segments,
VLSI Research application taxonomy, and IC Insights' WSTS end-market classification.
Used by semiconductor equity analysts, product managers, and market researchers.

Code prefix: dscapp_
System ID: domain_semiconductor_application
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SEMICONDUCTOR_APP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dscapp_consumer", "Consumer Electronics End-Markets", 1, None),
    ("dscapp_consumer_mobile", "Mobile: smartphone, tablet, TWS earbuds SoCs and chips", 2, "dscapp_consumer"),
    ("dscapp_consumer_pc", "PC: laptop, desktop CPU, GPU and peripheral chips", 2, "dscapp_consumer"),
    ("dscapp_consumer_av", "Consumer A/V: smart TV, gaming console, set-top box", 2, "dscapp_consumer"),
    ("dscapp_auto", "Automotive End-Markets", 1, None),
    ("dscapp_auto_adas", "ADAS and autonomous driving chips (lidar, radar, vision SoCs)", 2, "dscapp_auto"),
    ("dscapp_auto_ev", "EV power electronics (SiC MOSFET, GaN, inverter ICs)", 2, "dscapp_auto"),
    ("dscapp_auto_infotain", "Automotive infotainment and cockpit SoCs", 2, "dscapp_auto"),
    ("dscapp_auto_body", "Body electronics and ADAS MCUs (CAN, LIN controllers)", 2, "dscapp_auto"),
    ("dscapp_industrial", "Industrial and IoT End-Markets", 1, None),
    ("dscapp_industrial_factory", "Factory automation (PLCs, servo drives, motor control ICs)", 2, "dscapp_industrial"),
    ("dscapp_industrial_iot", "Industrial IoT sensors, edge MCUs, LPWAN transceivers", 2, "dscapp_industrial"),
    ("dscapp_industrial_medical", "Medical electronics (imaging, diagnostics, implantable ICs)", 2, "dscapp_industrial"),
    ("dscapp_datacom", "Data Center and Communications End-Markets", 1, None),
    ("dscapp_datacom_ai", "AI accelerators (GPU, TPU, custom ASICs for training/inference)", 2, "dscapp_datacom"),
    ("dscapp_datacom_net", "Networking ASICs (switch chips, DSPs, optical transceiver ICs)", 2, "dscapp_datacom"),
    ("dscapp_datacom_memory", "Data center DRAM, HBM and NAND flash storage", 2, "dscapp_datacom"),
    ("dscapp_defense", "Defence and Aerospace End-Markets", 1, None),
    ("dscapp_defense_rad", "Radiation-hardened ICs for space and defence", 2, "dscapp_defense"),
    ("dscapp_defense_rf", "Defence RF/microwave chips (GaAs, GaN MMIC radar, EW)", 2, "dscapp_defense"),
]

_DOMAIN_ROW = (
    "domain_semiconductor_application",
    "Semiconductor Application End-Market Types",
    "Semiconductor application end-market and vertical classification: consumer, automotive, industrial, datacom, defence",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3344', '3341', '3342']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_semiconductor_application(conn) -> int:
    """Ingest Semiconductor Application End-Market Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_semiconductor_application'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_semiconductor_application",
        "Semiconductor Application End-Market Types",
        "Semiconductor application end-market and vertical classification: consumer, automotive, industrial, datacom, defence",
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

    parent_codes = {parent for _, _, _, parent in SEMICONDUCTOR_APP_NODES if parent is not None}

    rows = [
        (
            "domain_semiconductor_application",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SEMICONDUCTOR_APP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SEMICONDUCTOR_APP_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_semiconductor_application'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_semiconductor_application'",
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
            [("naics_2022", code, "domain_semiconductor_application", "primary") for code in naics_codes],
        )

    return count
