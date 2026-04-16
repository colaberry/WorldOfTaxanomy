"""Digital Markets Act regulatory taxonomy ingester.

Digital Markets Act (EU) 2022/1925.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2022/1925

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 19 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_dma"
_SYSTEM_NAME = "Digital Markets Act"
_FULL_NAME = "Digital Markets Act (EU) 2022/1925"
_VERSION = "2022"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2022/1925"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_DMA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Gatekeeper Designation", 1, None),
    ("obligations", "Gatekeeper Obligations", 1, None),
    ("enforcement", "Enforcement and Remedies", 1, None),
    ("cps", "Core Platform Services (Art 2)", 2, "scope"),
    ("designation", "Gatekeeper Designation Criteria (Art 3)", 2, "scope"),
    ("quantitative", "Quantitative Thresholds (Art 3(2))", 2, "scope"),
    ("art_5", "Art 5 - Obligations (self-executing)", 2, "obligations"),
    ("art_6", "Art 6 - Obligations susceptible of being further specified", 2, "obligations"),
    ("art_7", "Art 7 - Obligations for messaging interoperability", 2, "obligations"),
    ("no_combine", "Prohibition on combining personal data across services (Art 5(2))", 2, "obligations"),
    ("sideloading", "Allow sideloading and third-party app stores (Art 6(4))", 2, "obligations"),
    ("self_pref", "Prohibition on self-preferencing (Art 6(5))", 2, "obligations"),
    ("data_portability", "Data portability (Art 6(9))", 2, "obligations"),
    ("fair_access", "Fair access to app stores (Art 6(12))", 2, "obligations"),
    ("investigation", "Market Investigation (Art 16-17)", 2, "enforcement"),
    ("non_compliance", "Non-Compliance Proceedings (Art 29)", 2, "enforcement"),
    ("fines", "Fines up to 10% of global turnover (Art 30)", 2, "enforcement"),
    ("periodic_penalty", "Periodic penalty payments (Art 31)", 2, "enforcement"),
    ("compliance_func", "Compliance Function (Art 28)", 2, "enforcement"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_dma(conn) -> int:
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
    for c, t, l, p in REG_DMA_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_DMA_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_DMA_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_DMA_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
