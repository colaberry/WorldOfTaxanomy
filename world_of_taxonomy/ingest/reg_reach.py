"""REACH regulatory taxonomy ingester.

Registration, Evaluation, Authorisation and Restriction of Chemicals (EC) No 1907/2006.
Authority: European Parliament and Council / ECHA.
Source: https://eur-lex.europa.eu/eli/reg/2006/1907

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 19 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_reach"
_SYSTEM_NAME = "REACH"
_FULL_NAME = "Registration, Evaluation, Authorisation and Restriction of Chemicals (EC) No 1907/2006"
_VERSION = "2006"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council / ECHA"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2006/1907"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_REACH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - General Issues", 1, None),
    ("title_2", "Title II - Registration of Substances", 1, None),
    ("title_3", "Title III - Data Sharing and Avoidance of Unnecessary Testing", 1, None),
    ("title_5", "Title V - Downstream User Obligations", 1, None),
    ("title_6", "Title VI - Evaluation", 1, None),
    ("title_7", "Title VII - Authorisation", 1, None),
    ("title_8", "Title VIII - Restrictions", 1, None),
    ("title_9", "Title IX - Fees and Charges", 1, None),
    ("title_10", "Title X - ECHA", 1, None),
    ("registration", "Registration Requirements (Art 5-24)", 2, "title_2"),
    ("sds", "Safety Data Sheets (Art 31-36)", 2, "title_2"),
    ("svhc", "Substances of Very High Concern (SVHC) (Art 57)", 2, "title_7"),
    ("candidate_list", "Candidate List (Art 59)", 2, "title_7"),
    ("annex_xiv", "Annex XIV - Authorisation List", 2, "title_7"),
    ("restrictions", "Annex XVII - Restrictions", 2, "title_8"),
    ("dossier_eval", "Dossier Evaluation (Art 40-43)", 2, "title_6"),
    ("substance_eval", "Substance Evaluation (Art 44-48)", 2, "title_6"),
    ("downstream", "Downstream User Obligations (Art 37-39)", 2, "title_5"),
    ("echa_role", "ECHA Role and Committees", 2, "title_10"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_reach(conn) -> int:
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
    for c, t, l, p in REG_REACH_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_REACH_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_REACH_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_REACH_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
