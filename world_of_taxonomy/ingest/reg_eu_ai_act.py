"""EU AI Act regulatory taxonomy ingester.

Regulation (EU) 2024/1689 laying down harmonised rules on artificial intelligence.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2024/1689

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 27 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_eu_ai_act"
_SYSTEM_NAME = "EU AI Act"
_FULL_NAME = "Regulation (EU) 2024/1689 laying down harmonised rules on artificial intelligence"
_VERSION = "2024"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2024/1689"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_EU_AI_ACT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - General Provisions", 1, None),
    ("title_2", "Title II - Prohibited AI Practices", 1, None),
    ("title_3", "Title III - High-Risk AI Systems", 1, None),
    ("title_4", "Title IV - Transparency Obligations", 1, None),
    ("title_5", "Title V - General-Purpose AI Models", 1, None),
    ("title_6", "Title VI - Governance", 1, None),
    ("title_7", "Title VII - EU Database for High-Risk AI", 1, None),
    ("title_8", "Title VIII - Post-Market Monitoring and Enforcement", 1, None),
    ("title_9", "Title IX - Codes of Conduct", 1, None),
    ("title_10", "Title X - Penalties", 1, None),
    ("art_5", "Art 5 - Prohibited AI practices (social scoring, biometric categorisation, etc.)", 2, "title_2"),
    ("art_6", "Art 6 - Classification rules for high-risk AI systems", 2, "title_3"),
    ("art_9", "Art 9 - Risk management system", 2, "title_3"),
    ("art_10", "Art 10 - Data and data governance", 2, "title_3"),
    ("art_13", "Art 13 - Transparency and information to deployers", 2, "title_3"),
    ("art_14", "Art 14 - Human oversight", 2, "title_3"),
    ("art_15", "Art 15 - Accuracy, robustness and cybersecurity", 2, "title_3"),
    ("art_50", "Art 50 - Transparency obligations for certain AI systems", 2, "title_4"),
    ("art_51", "Art 51 - Classification of general-purpose AI models with systemic risk", 2, "title_5"),
    ("art_53", "Art 53 - Obligations for providers of GPAI models", 2, "title_5"),
    ("art_55", "Art 55 - Obligations for GPAI models with systemic risk", 2, "title_5"),
    ("art_64", "Art 64 - AI Office", 2, "title_6"),
    ("art_65", "Art 65 - European Artificial Intelligence Board", 2, "title_6"),
    ("art_72", "Art 72 - Market surveillance and control", 2, "title_8"),
    ("art_99", "Art 99 - Penalties", 2, "title_10"),
    ("annex_i", "Annex I - EU Harmonisation Legislation (list)", 2, "title_3"),
    ("annex_iii", "Annex III - High-Risk AI Systems (use cases by area)", 2, "title_3"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_eu_ai_act(conn) -> int:
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
    for c, t, l, p in REG_EU_AI_ACT_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_EU_AI_ACT_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_EU_AI_ACT_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_EU_AI_ACT_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
