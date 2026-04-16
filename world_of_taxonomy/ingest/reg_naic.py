"""NAIC Model Laws regulatory taxonomy ingester.

National Association of Insurance Commissioners Model Laws and Regulations.
Authority: National Association of Insurance Commissioners (NAIC).
Source: https://content.naic.org/model-laws

Data provenance: manual_transcription
License: Public Domain

Total: 21 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_naic"
_SYSTEM_NAME = "NAIC Model Laws"
_FULL_NAME = "National Association of Insurance Commissioners Model Laws and Regulations"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "National Association of Insurance Commissioners (NAIC)"
_SOURCE_URL = "https://content.naic.org/model-laws"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_NAIC_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("market_conduct", "Market Conduct and Consumer Protection", 1, None),
    ("solvency", "Solvency and Financial Regulation", 1, None),
    ("data_security", "Insurance Data Security", 1, None),
    ("product_reg", "Product Regulation", 1, None),
    ("licensing", "Licensing and Registration", 1, None),
    ("unfair_trade", "Unfair Trade Practices Act (#880)", 2, "market_conduct"),
    ("unfair_claims", "Unfair Claims Settlement Practices Act (#900)", 2, "market_conduct"),
    ("advertising", "Advertisements of Accident and Health Insurance (#570)", 2, "market_conduct"),
    ("disclosure", "Life Insurance Disclosure Model Regulation (#580)", 2, "market_conduct"),
    ("rbc", "Risk-Based Capital (RBC) for Insurers Model Act (#312)", 2, "solvency"),
    ("own_risk", "Own Risk and Solvency Assessment (ORSA) Model Act (#505)", 2, "solvency"),
    ("holding_co", "Insurance Holding Company System Model Act (#440)", 2, "solvency"),
    ("reinsurance", "Credit for Reinsurance Model Law (#785)", 2, "solvency"),
    ("guaranty", "Life and Health Insurance Guaranty Association Model Act (#520)", 2, "solvency"),
    ("ids_model", "Insurance Data Security Model Law (#668)", 2, "data_security"),
    ("privacy", "Privacy of Consumer Financial Information (#672)", 2, "data_security"),
    ("cyber_notification", "Cybersecurity Event Notification", 2, "data_security"),
    ("rate_review", "Rate and Form Filing Requirements", 2, "product_reg"),
    ("standard_val", "Standard Valuation Law (#820)", 2, "product_reg"),
    ("producer_lic", "Producer Licensing Model Act (#218)", 2, "licensing"),
    ("uniform_cert", "Uniform Certificate of Authority Application", 2, "licensing"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_naic(conn) -> int:
    """Ingest NAIC Model Laws regulatory taxonomy.

    Returns 21 nodes.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0,
                   source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance,
                   license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )

    leaf_codes = set()
    parent_codes = set()
    for code, title, level, parent in REG_NAIC_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_NAIC_NODES:
        if code not in parent_codes:
            leaf_codes.add(code)

    rows = [
        (
            _SYSTEM_ID,
            code,
            title,
            level,
            parent,
            code.split("_")[0],
            code in leaf_codes,
        )
        for code, title, level, parent in REG_NAIC_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_NAIC_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
