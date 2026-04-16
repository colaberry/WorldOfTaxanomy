"""NIS2 Directive regulatory taxonomy ingester.

Directive (EU) 2022/2555 on Security of Network and Information Systems.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/dir/2022/2555

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 24 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_nis2"
_SYSTEM_NAME = "NIS2 Directive"
_FULL_NAME = "Directive (EU) 2022/2555 on Security of Network and Information Systems"
_VERSION = "2022"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/dir/2022/2555"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_NIS2_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ch_1", "Chapter I - General Provisions", 1, None),
    ("ch_2", "Chapter II - Coordinated Cybersecurity Frameworks", 1, None),
    ("ch_3", "Chapter III - Cooperation", 1, None),
    ("ch_4", "Chapter IV - Cybersecurity Risk-Management and Reporting Obligations", 1, None),
    ("ch_5", "Chapter V - Jurisdiction and Registration", 1, None),
    ("ch_6", "Chapter VI - Information Sharing", 1, None),
    ("ch_7", "Chapter VII - Supervision and Enforcement", 1, None),
    ("art_2", "Art 2 - Scope (essential and important entities)", 2, "ch_1"),
    ("art_3", "Art 3 - Essential and important entities", 2, "ch_1"),
    ("art_7", "Art 7 - National cybersecurity strategy", 2, "ch_2"),
    ("art_8", "Art 8 - Competent authorities and CSIRTs", 2, "ch_2"),
    ("art_10", "Art 10 - CSIRTs", 2, "ch_2"),
    ("art_14", "Art 14 - EU-CyCLONe", 2, "ch_3"),
    ("art_15", "Art 15 - Cooperation Group", 2, "ch_3"),
    ("art_20", "Art 20 - Governance", 2, "ch_4"),
    ("art_21", "Art 21 - Cybersecurity risk-management measures", 2, "ch_4"),
    ("art_23", "Art 23 - Reporting obligations", 2, "ch_4"),
    ("art_26", "Art 26 - Jurisdiction and territoriality", 2, "ch_5"),
    ("art_29", "Art 29 - Cybersecurity information-sharing arrangements", 2, "ch_6"),
    ("art_31", "Art 31 - General supervision for essential entities", 2, "ch_7"),
    ("art_32", "Art 32 - Enforcement measures for essential entities", 2, "ch_7"),
    ("art_34", "Art 34 - Penalties", 2, "ch_7"),
    ("annex_i", "Annex I - Essential Sectors (energy, transport, banking, health, etc.)", 2, "ch_1"),
    ("annex_ii", "Annex II - Important Sectors (postal, waste, food, digital providers, etc.)", 2, "ch_1"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_nis2(conn) -> int:
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
    for c, t, l, p in REG_NIS2_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_NIS2_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_NIS2_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_NIS2_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
