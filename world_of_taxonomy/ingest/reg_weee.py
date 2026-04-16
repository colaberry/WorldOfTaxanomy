"""WEEE Directive regulatory taxonomy ingester.

Waste Electrical and Electronic Equipment Directive 2012/19/EU.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/dir/2012/19

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 21 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_weee"
_SYSTEM_NAME = "WEEE Directive"
_FULL_NAME = "Waste Electrical and Electronic Equipment Directive 2012/19/EU"
_VERSION = "2012"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/dir/2012/19"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_WEEE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Categories", 1, None),
    ("collection", "Collection and Take-Back", 1, None),
    ("treatment", "Treatment and Recovery", 1, None),
    ("targets", "Recycling Targets", 1, None),
    ("producer", "Producer Responsibility", 1, None),
    ("cat_1", "Category 1 - Temperature Exchange Equipment", 2, "scope"),
    ("cat_2", "Category 2 - Screens and Monitors", 2, "scope"),
    ("cat_3", "Category 3 - Lamps", 2, "scope"),
    ("cat_4", "Category 4 - Large Equipment (>50cm)", 2, "scope"),
    ("cat_5", "Category 5 - Small Equipment (<=50cm)", 2, "scope"),
    ("cat_6", "Category 6 - Small IT and Telecom", 2, "scope"),
    ("separate_collection", "Separate Collection Requirements (Art 5)", 2, "collection"),
    ("collection_rate", "Collection Rate Targets (Art 7)", 2, "collection"),
    ("take_back", "Producer Take-Back Obligations", 2, "collection"),
    ("treatment_req", "Treatment Requirements (Art 8)", 2, "treatment"),
    ("best_available", "Best Available Treatment Techniques", 2, "treatment"),
    ("recovery_target", "Recovery Targets by Category (Annex V)", 2, "targets"),
    ("recycling_target", "Recycling Targets by Category (Annex V)", 2, "targets"),
    ("epr", "Extended Producer Responsibility", 2, "producer"),
    ("registration", "Producer Registration", 2, "producer"),
    ("financing", "Financing of Collection and Treatment", 2, "producer"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_weee(conn) -> int:
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
    for c, t, l, p in REG_WEEE_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_WEEE_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_WEEE_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_WEEE_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
