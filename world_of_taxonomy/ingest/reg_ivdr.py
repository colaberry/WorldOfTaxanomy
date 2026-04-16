"""IVDR regulatory taxonomy ingester.

In Vitro Diagnostic Medical Devices Regulation (EU) 2017/746.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2017/746

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 17 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_ivdr"
_SYSTEM_NAME = "IVDR"
_FULL_NAME = "In Vitro Diagnostic Medical Devices Regulation (EU) 2017/746"
_VERSION = "2017"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2017/746"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_IVDR_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Definitions", 1, None),
    ("obligations", "Economic Operator Obligations", 1, None),
    ("classification", "Classification", 1, None),
    ("conformity", "Conformity Assessment", 1, None),
    ("clinical", "Clinical Evidence and Performance", 1, None),
    ("pms", "Post-Market Surveillance", 1, None),
    ("class_a", "Class A - Low Risk", 2, "classification"),
    ("class_b", "Class B - Moderate Risk", 2, "classification"),
    ("class_c", "Class C - High Risk", 2, "classification"),
    ("class_d", "Class D - Highest Risk", 2, "classification"),
    ("annex_viii", "Annex VIII - Classification Rules", 2, "classification"),
    ("perf_eval", "Performance Evaluation (Art 56)", 2, "clinical"),
    ("perf_study", "Performance Studies (Art 57-77)", 2, "clinical"),
    ("udi_ivd", "UDI for IVDs", 2, "obligations"),
    ("eu_common_specs", "EU Common Specifications", 2, "conformity"),
    ("vigilance_ivd", "Vigilance for IVDs", 2, "pms"),
    ("trend_report", "Trend Reporting", 2, "pms"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_ivdr(conn) -> int:
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
    for c, t, l, p in REG_IVDR_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_IVDR_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_IVDR_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_IVDR_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
