"""EU Data Act regulatory taxonomy ingester.

Data Act (EU) 2023/2854.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2023/2854

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 20 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_eu_data_act"
_SYSTEM_NAME = "EU Data Act"
_FULL_NAME = "Data Act (EU) 2023/2854"
_VERSION = "2023"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2023/2854"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_EU_DATA_ACT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("ch_2", "Chapter II - B2C and B2B Data Sharing from IoT", 1, None),
    ("ch_3", "Chapter III - Obligations for Data Holders", 1, None),
    ("ch_4", "Chapter IV - Unfair Contractual Terms", 1, None),
    ("ch_5", "Chapter V - Public Sector Access to Private Data", 1, None),
    ("ch_6", "Chapter VI - Switching Between Cloud Services", 1, None),
    ("ch_7", "Chapter VII - International Data Access Safeguards", 1, None),
    ("ch_8", "Chapter VIII - Interoperability", 1, None),
    ("user_access", "User Right to Access IoT Data (Art 4)", 2, "ch_2"),
    ("user_sharing", "User Right to Share Data with Third Parties (Art 5)", 2, "ch_2"),
    ("manufacturer_duty", "Manufacturer Design Obligations (Art 3)", 2, "ch_2"),
    ("fair_terms", "Fair, Reasonable and Non-Discriminatory Terms (Art 8-12)", 2, "ch_3"),
    ("compensation", "Reasonable Compensation for Data Sharing", 2, "ch_3"),
    ("b2b_unfair", "Assessment of Unfair Terms in B2B Data Contracts (Art 13)", 2, "ch_4"),
    ("exceptional_need", "Access for Exceptional Need (Art 14-18)", 2, "ch_5"),
    ("switching_rights", "Right to Switch Cloud Provider (Art 23-26)", 2, "ch_6"),
    ("portability", "Data Portability Between Cloud Services", 2, "ch_6"),
    ("switching_charges", "Gradual Elimination of Switching Charges", 2, "ch_6"),
    ("safeguards", "Safeguards for Non-EU Government Data Requests (Art 27-28)", 2, "ch_7"),
    ("interop_standards", "Interoperability Standards for Data Spaces (Art 33-36)", 2, "ch_8"),
    ("smart_contracts", "Smart Contract Requirements (Art 36)", 2, "ch_8"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_eu_data_act(conn) -> int:
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
    for c, t, l, p in REG_EU_DATA_ACT_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_EU_DATA_ACT_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_EU_DATA_ACT_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_EU_DATA_ACT_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
