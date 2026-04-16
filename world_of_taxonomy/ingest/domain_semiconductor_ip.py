"""Semiconductor IP and Business Model Types domain taxonomy ingester.

Classifies semiconductor companies and products by their intellectual property
ownership model and supply chain position in the semiconductor value chain.
Orthogonal to end-market and device type.
Used by M&A advisors, IP attorneys, equity analysts, and technology strategists
when evaluating competitive moats and supply chain integration risks.

Code prefix: dscip_
System ID: domain_semiconductor_ip
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SEMICONDUCTOR_IP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dscip_iplicense", "IP Licensing Model (ARM Architecture Model)", 1, None),
    ("dscip_iplicense_arch", "Architecture licensors (ARM, RISC-V consortium, MIPS) - ISA IP", 2, "dscip_iplicense"),
    ("dscip_iplicense_phyip", "Physical IP licensors (Synopsys, Cadence, Rambus) - standard cells, PHYs", 2, "dscip_iplicense"),
    ("dscip_iplicense_designip", "Design IP licensors (Imagination GPU, Tensilica DSP, Ceva DSP)", 2, "dscip_iplicense"),
    ("dscip_fabless", "Fabless Design Companies", 1, None),
    ("dscip_fabless_consumer", "Consumer fabless (Qualcomm, MediaTek, Apple silicon design)", 2, "dscip_fabless"),
    ("dscip_fabless_hpc", "HPC/AI fabless (NVIDIA, AMD, Marvell, Broadcom ASIC)", 2, "dscip_fabless"),
    ("dscip_fabless_startup", "Fabless startups and chip design companies (seed to Series C)", 2, "dscip_fabless"),
    ("dscip_idm", "Integrated Device Manufacturers (IDM)", 1, None),
    ("dscip_idm_memory", "Memory IDM (Samsung, SK Hynix, Micron - DRAM and NAND)", 2, "dscip_idm"),
    ("dscip_idm_logic", "Logic IDM (Intel, TI, STMicro - own design and fab)", 2, "dscip_idm"),
    ("dscip_foundry", "Pure-Play Foundry Services", 1, None),
    ("dscip_foundry_leading", "Leading-edge foundry (TSMC, Samsung Foundry - 2nm-5nm)", 2, "dscip_foundry"),
    ("dscip_foundry_mature", "Mature-node foundry (GlobalFoundries, SMIC, UMC - 28nm+)", 2, "dscip_foundry"),
    ("dscip_osat", "OSAT (Outsourced Semiconductor Assembly and Test)", 1, None),
    ("dscip_osat_traditional", "Traditional OSAT (ASE, Amkor, JCET wire-bond packages)", 2, "dscip_osat"),
    ("dscip_osat_advanced", "Advanced packaging OSAT (CoWoS, 2.5D/3D integration, chiplets)", 2, "dscip_osat"),
]

_DOMAIN_ROW = (
    "domain_semiconductor_ip",
    "Semiconductor IP and Business Model Types",
    "Semiconductor IP licensing, business model and supply chain position classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3344', '5415', '5417']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_semiconductor_ip(conn) -> int:
    """Ingest Semiconductor IP and Business Model Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_semiconductor_ip'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_semiconductor_ip",
        "Semiconductor IP and Business Model Types",
        "Semiconductor IP licensing, business model and supply chain position classification",
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

    parent_codes = {parent for _, _, _, parent in SEMICONDUCTOR_IP_NODES if parent is not None}

    rows = [
        (
            "domain_semiconductor_ip",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SEMICONDUCTOR_IP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SEMICONDUCTOR_IP_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_semiconductor_ip'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_semiconductor_ip'",
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
            [("naics_2022", code, "domain_semiconductor_ip", "primary") for code in naics_codes],
        )

    return count
