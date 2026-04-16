"""DORA regulatory taxonomy ingester.

Digital Operational Resilience Act (EU) 2022/2554.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2022/2554

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 27 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_dora"
_SYSTEM_NAME = "DORA"
_FULL_NAME = "Digital Operational Resilience Act (EU) 2022/2554"
_VERSION = "2022"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2022/2554"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_DORA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ch_1", "Chapter I - General Provisions", 1, None),
    ("ch_2", "Chapter II - ICT Risk Management", 1, None),
    ("ch_3", "Chapter III - ICT-Related Incident Management", 1, None),
    ("ch_4", "Chapter IV - Digital Operational Resilience Testing", 1, None),
    ("ch_5", "Chapter V - Managing ICT Third-Party Risk", 1, None),
    ("ch_6", "Chapter VI - Information-Sharing Arrangements", 1, None),
    ("ch_7", "Chapter VII - Oversight Framework for Critical ICT Third-Party Providers", 1, None),
    ("art_1", "Art 1 - Subject matter", 2, "ch_1"),
    ("art_2", "Art 2 - Scope", 2, "ch_1"),
    ("art_3", "Art 3 - Definitions", 2, "ch_1"),
    ("art_5", "Art 5 - Governance and organisation", 2, "ch_2"),
    ("art_6", "Art 6 - ICT risk management framework", 2, "ch_2"),
    ("art_7", "Art 7 - ICT systems, protocols and tools", 2, "ch_2"),
    ("art_8", "Art 8 - Identification", 2, "ch_2"),
    ("art_9", "Art 9 - Protection and prevention", 2, "ch_2"),
    ("art_10", "Art 10 - Detection", 2, "ch_2"),
    ("art_11", "Art 11 - Response and recovery", 2, "ch_2"),
    ("art_17", "Art 17 - ICT-related incident management process", 2, "ch_3"),
    ("art_18", "Art 18 - Classification of ICT-related incidents", 2, "ch_3"),
    ("art_19", "Art 19 - Reporting of major ICT-related incidents", 2, "ch_3"),
    ("art_24", "Art 24 - General requirements for digital operational resilience testing", 2, "ch_4"),
    ("art_26", "Art 26 - Threat-led penetration testing (TLPT)", 2, "ch_4"),
    ("art_28", "Art 28 - Key principles for managing ICT third-party risk", 2, "ch_5"),
    ("art_30", "Art 30 - Key contractual provisions", 2, "ch_5"),
    ("art_31", "Art 31 - Designation of critical ICT third-party service providers", 2, "ch_7"),
    ("art_33", "Art 33 - Tasks of the Lead Overseer", 2, "ch_7"),
    ("art_35", "Art 35 - Oversight powers", 2, "ch_7"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_dora(conn) -> int:
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
    for c, t, l, p in REG_DORA_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_DORA_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_DORA_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_DORA_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
