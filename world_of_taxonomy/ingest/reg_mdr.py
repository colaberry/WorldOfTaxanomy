"""MDR regulatory taxonomy ingester.

Medical Device Regulation (EU) 2017/745.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2017/745

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 22 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_mdr"
_SYSTEM_NAME = "MDR"
_FULL_NAME = "Medical Device Regulation (EU) 2017/745"
_VERSION = "2017"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2017/745"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_MDR_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ch_1", "Chapter I - Scope and Definitions", 1, None),
    ("ch_2", "Chapter II - Making Available and Putting into Service", 1, None),
    ("ch_3", "Chapter III - Identification and Traceability (UDI)", 1, None),
    ("ch_4", "Chapter IV - Notified Bodies", 1, None),
    ("ch_5", "Chapter V - Classification and Conformity Assessment", 1, None),
    ("ch_6", "Chapter VI - Clinical Evaluation and Investigation", 1, None),
    ("ch_7", "Chapter VII - Post-Market Surveillance and Vigilance", 1, None),
    ("ch_8", "Chapter VIII - Market Surveillance", 1, None),
    ("classification", "Device Classification (Annex VIII rules)", 2, "ch_5"),
    ("class_i", "Class I Devices", 2, "ch_5"),
    ("class_iia", "Class IIa Devices", 2, "ch_5"),
    ("class_iib", "Class IIb Devices", 2, "ch_5"),
    ("class_iii", "Class III Devices", 2, "ch_5"),
    ("udi", "Unique Device Identification (UDI) System", 2, "ch_3"),
    ("eudamed", "EUDAMED Database", 2, "ch_3"),
    ("clinical_eval", "Clinical Evaluation (Art 61)", 2, "ch_6"),
    ("clinical_inv", "Clinical Investigations (Art 62-82)", 2, "ch_6"),
    ("pms", "Post-Market Surveillance (Art 83-86)", 2, "ch_7"),
    ("vigilance", "Vigilance (Art 87-92)", 2, "ch_7"),
    ("nb_designation", "Notified Body Designation and Monitoring", 2, "ch_4"),
    ("nb_conformity", "Conformity Assessment Procedures (Annexes IX-XI)", 2, "ch_4"),
    ("tech_doc", "Technical Documentation (Annexes II-III)", 2, "ch_2"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_mdr(conn) -> int:
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
    for c, t, l, p in REG_MDR_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_MDR_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_MDR_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_MDR_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
