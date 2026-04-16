"""Digital Services Act regulatory taxonomy ingester.

Digital Services Act (EU) 2022/2065.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2022/2065

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 21 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_dsa"
_SYSTEM_NAME = "Digital Services Act"
_FULL_NAME = "Digital Services Act (EU) 2022/2065"
_VERSION = "2022"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2022/2065"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_DSA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ch_1", "Chapter I - General Provisions", 1, None),
    ("ch_2", "Chapter II - Liability of Intermediary Service Providers", 1, None),
    ("ch_3", "Chapter III - Due Diligence Obligations", 1, None),
    ("ch_4", "Chapter IV - Implementation and Enforcement", 1, None),
    ("sec_1", "Section 1 - Obligations for All Intermediary Services", 2, "ch_3"),
    ("sec_2", "Section 2 - Additional Obligations for Hosting Services", 2, "ch_3"),
    ("sec_3", "Section 3 - Additional Obligations for Online Platforms", 2, "ch_3"),
    ("sec_4", "Section 4 - Additional Obligations for Online Marketplaces", 2, "ch_3"),
    ("sec_5", "Section 5 - Additional Obligations for VLOPs and VLOSEs", 2, "ch_3"),
    ("notice_action", "Notice and Action Mechanism (Art 16)", 2, "sec_2"),
    ("transparency", "Transparency Reporting (Art 15)", 2, "sec_1"),
    ("terms_service", "Terms of Service (Art 14)", 2, "sec_1"),
    ("recommender", "Recommender System Transparency (Art 27)", 2, "sec_3"),
    ("advertising", "Online Advertising Transparency (Art 26)", 2, "sec_3"),
    ("dark_patterns", "Prohibition of Dark Patterns (Art 25)", 2, "sec_3"),
    ("risk_assess", "Systemic Risk Assessment for VLOPs (Art 34)", 2, "sec_5"),
    ("risk_mitigate", "Risk Mitigation for VLOPs (Art 35)", 2, "sec_5"),
    ("audit", "Independent Audit for VLOPs (Art 37)", 2, "sec_5"),
    ("data_access", "Data Access for Research (Art 40)", 2, "sec_5"),
    ("dsc", "Digital Services Coordinator (Art 49)", 2, "ch_4"),
    ("board", "European Board for Digital Services (Art 61)", 2, "ch_4"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_dsa(conn) -> int:
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
    for c, t, l, p in REG_DSA_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_DSA_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_DSA_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_DSA_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
