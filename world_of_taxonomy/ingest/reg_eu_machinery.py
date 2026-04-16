"""EU Machinery Regulation regulatory taxonomy ingester.

Regulation (EU) 2023/1230 on machinery products.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2023/1230

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 20 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_eu_machinery"
_SYSTEM_NAME = "EU Machinery Regulation"
_FULL_NAME = "Regulation (EU) 2023/1230 on machinery products"
_VERSION = "2023"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2023/1230"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_EU_MACHINERY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Definitions", 1, None),
    ("ehsr", "Essential Health and Safety Requirements", 1, None),
    ("conformity", "Conformity Assessment", 1, None),
    ("market", "Market Surveillance", 1, None),
    ("machinery_def", "Machinery Product Definitions (Art 2-3)", 2, "scope"),
    ("exclusions", "Exclusions (weapons, vehicles, ships, etc.)", 2, "scope"),
    ("general_ehsr", "General EHSR Principles (Annex III Part 1)", 2, "ehsr"),
    ("controls", "Control Systems and Safety Functions", 2, "ehsr"),
    ("mechanical", "Mechanical Hazards Protection", 2, "ehsr"),
    ("electrical", "Electrical Hazards Protection", 2, "ehsr"),
    ("ergonomics", "Ergonomic Design Requirements", 2, "ehsr"),
    ("autonomous", "Autonomous and AI-Enabled Machinery (new in 2023)", 2, "ehsr"),
    ("cyber_safety", "Cybersecurity of Safety Functions (new in 2023)", 2, "ehsr"),
    ("self_cert", "Self-Certification (internal checks)", 2, "conformity"),
    ("eu_type", "EU-Type Examination (Annex IX)", 2, "conformity"),
    ("quality_system", "Full Quality Assurance (Annex X)", 2, "conformity"),
    ("high_risk_list", "High-Risk Machinery List (Annex I)", 2, "conformity"),
    ("digital_docs", "Digital Instructions and Declaration of Conformity", 2, "conformity"),
    ("ce_marking_m", "CE Marking Requirements", 2, "market"),
    ("surveillance", "Market Surveillance Coordination", 2, "market"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_eu_machinery(conn) -> int:
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
    for c, t, l, p in REG_EU_MACHINERY_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_EU_MACHINERY_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_EU_MACHINERY_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_EU_MACHINERY_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
