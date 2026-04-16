"""ePrivacy Directive regulatory taxonomy ingester.

Directive 2002/58/EC concerning the processing of personal data and privacy in electronic communications.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/dir/2002/58

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 15 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_eprivacy"
_SYSTEM_NAME = "ePrivacy Directive"
_FULL_NAME = "Directive 2002/58/EC concerning the processing of personal data and privacy in electronic communications"
_VERSION = "2002"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/dir/2002/58"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_EPRIVACY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Definitions", 1, None),
    ("confidentiality", "Confidentiality of Communications", 1, None),
    ("traffic_data", "Traffic and Location Data", 1, None),
    ("marketing", "Direct Marketing", 1, None),
    ("cookies", "Cookies and Tracking", 1, None),
    ("enforcement", "Enforcement", 1, None),
    ("art_1", "Art 1 - Scope and aim", 2, "scope"),
    ("art_2", "Art 2 - Definitions", 2, "scope"),
    ("art_5", "Art 5 - Confidentiality of communications", 2, "confidentiality"),
    ("art_5_3", "Art 5(3) - Cookie consent requirement", 2, "cookies"),
    ("art_6", "Art 6 - Traffic data", 2, "traffic_data"),
    ("art_9", "Art 9 - Location data other than traffic data", 2, "traffic_data"),
    ("art_13", "Art 13 - Unsolicited communications (opt-in for marketing)", 2, "marketing"),
    ("art_15", "Art 15 - Application of certain provisions of GDPR", 2, "enforcement"),
    ("art_15a", "Art 15a - Implementation and enforcement", 2, "enforcement"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_eprivacy(conn) -> int:
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
    for c, t, l, p in REG_EPRIVACY_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_EPRIVACY_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_EPRIVACY_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_EPRIVACY_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
