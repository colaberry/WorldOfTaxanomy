"""EU Cyber Resilience Act regulatory taxonomy ingester.

Cyber Resilience Act (EU) 2024/2847.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2024/2847

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 20 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_eu_cra"
_SYSTEM_NAME = "EU Cyber Resilience Act"
_FULL_NAME = "Cyber Resilience Act (EU) 2024/2847"
_VERSION = "2024"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2024/2847"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_EU_CRA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Definitions", 1, None),
    ("requirements", "Essential Cybersecurity Requirements", 1, None),
    ("obligations", "Manufacturer Obligations", 1, None),
    ("conformity", "Conformity Assessment", 1, None),
    ("vulnerability", "Vulnerability Handling", 1, None),
    ("market_surv", "Market Surveillance", 1, None),
    ("product_scope", "Products with Digital Elements (Art 2)", 2, "scope"),
    ("exclusions", "Exclusions (medical devices, vehicles, aviation)", 2, "scope"),
    ("security_design", "Security by Design (Annex I Part I)", 2, "requirements"),
    ("vuln_handling", "Vulnerability Handling Requirements (Annex I Part II)", 2, "requirements"),
    ("documentation", "Documentation and Instructions (Art 13)", 2, "requirements"),
    ("risk_assess", "Cybersecurity Risk Assessment (Art 13)", 2, "obligations"),
    ("sbom", "Software Bill of Materials (SBOM) (Art 13(11))", 2, "obligations"),
    ("reporting_vuln", "Actively Exploited Vulnerability Reporting - 24h (Art 14)", 2, "obligations"),
    ("security_updates", "Security Updates for Product Lifetime (Art 13(8))", 2, "obligations"),
    ("self_assess", "Self-Assessment (Default)", 2, "conformity"),
    ("third_party", "Third-Party Assessment (Critical Products Class I/II)", 2, "conformity"),
    ("csirt_reporting", "Reporting to CSIRT/ENISA", 2, "vulnerability"),
    ("coordinated_disc", "Coordinated Vulnerability Disclosure", 2, "vulnerability"),
    ("penalties_cra", "Penalties (up to EUR 15 million or 2.5% turnover)", 2, "market_surv"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_eu_cra(conn) -> int:
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
    for c, t, l, p in REG_EU_CRA_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_EU_CRA_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_EU_CRA_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_EU_CRA_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
