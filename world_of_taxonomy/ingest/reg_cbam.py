"""CBAM regulatory taxonomy ingester.

Carbon Border Adjustment Mechanism (EU) 2023/956.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2023/956

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 18 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_cbam"
_SYSTEM_NAME = "CBAM"
_FULL_NAME = "Carbon Border Adjustment Mechanism (EU) 2023/956"
_VERSION = "2023"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2023/956"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_CBAM_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Covered Goods", 1, None),
    ("reporting", "Reporting Obligations", 1, None),
    ("certificates", "CBAM Certificates", 1, None),
    ("administration", "Administration and Governance", 1, None),
    ("cement", "Cement (CN Chapter 25)", 2, "scope"),
    ("iron_steel", "Iron and Steel (CN Chapter 72-73)", 2, "scope"),
    ("aluminium", "Aluminium (CN Chapter 76)", 2, "scope"),
    ("fertilisers", "Fertilisers (CN Chapter 31)", 2, "scope"),
    ("electricity", "Electricity", 2, "scope"),
    ("hydrogen", "Hydrogen (CN 2804)", 2, "scope"),
    ("transitional", "Transitional Phase Reporting (Oct 2023 - Dec 2025)", 2, "reporting"),
    ("definitive", "Definitive System (from Jan 2026)", 2, "reporting"),
    ("embedded_emissions", "Calculation of Embedded Emissions", 2, "reporting"),
    ("purchase", "Purchase of CBAM Certificates", 2, "certificates"),
    ("surrender", "Surrender of Certificates", 2, "certificates"),
    ("price", "Price Based on EU ETS Allowance Price", 2, "certificates"),
    ("authorised_declarant", "Authorised CBAM Declarant", 2, "administration"),
    ("national_authority", "National Competent Authorities", 2, "administration"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_cbam(conn) -> int:
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0, source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance, license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    leaf_codes, parent_codes = set(), set()
    for c, t, l, p in REG_CBAM_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_CBAM_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_CBAM_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_CBAM_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
