"""SFDR (Detailed) regulatory taxonomy ingester.

Sustainable Finance Disclosure Regulation (EU) 2019/2088 - Detailed.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/reg/2019/2088

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 22 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_sfdr_detail"
_SYSTEM_NAME = "SFDR (Detailed)"
_FULL_NAME = "Sustainable Finance Disclosure Regulation (EU) 2019/2088 - Detailed"
_VERSION = "2019"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/reg/2019/2088"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_SFDR_DETAIL_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("entity_level", "Entity-Level Disclosures", 1, None),
    ("product_level", "Product-Level Disclosures", 1, None),
    ("pai", "Principal Adverse Impacts (PAI)", 1, None),
    ("taxonomy_alignment", "EU Taxonomy Alignment", 1, None),
    ("rts", "Regulatory Technical Standards", 1, None),
    ("art_3", "Art 3 - Sustainability Risk Policies (entity)", 2, "entity_level"),
    ("art_4", "Art 4 - PAI Consideration at Entity Level", 2, "entity_level"),
    ("art_5", "Art 5 - Remuneration Policies and Sustainability Risks", 2, "entity_level"),
    ("art_6", "Art 6 - Pre-Contractual Sustainability Risk Disclosures", 2, "product_level"),
    ("art_8", "Art 8 - Light Green Products (environmental/social characteristics)", 2, "product_level"),
    ("art_9", "Art 9 - Dark Green Products (sustainable investment objective)", 2, "product_level"),
    ("art_10", "Art 10 - Website Disclosures for Art 8/9 Products", 2, "product_level"),
    ("art_11", "Art 11 - Periodic Reporting for Art 8/9 Products", 2, "product_level"),
    ("pai_climate", "PAI - Climate and Environment Indicators", 2, "pai"),
    ("pai_social", "PAI - Social and Employee Indicators", 2, "pai"),
    ("pai_governance", "PAI - Governance Indicators", 2, "pai"),
    ("pai_opt_env", "PAI - Optional Environmental Indicators", 2, "pai"),
    ("pai_opt_social", "PAI - Optional Social Indicators", 2, "pai"),
    ("do_no_harm", "Do No Significant Harm (DNSH) Assessment", 2, "taxonomy_alignment"),
    ("minimum_safeguards", "Minimum Safeguards (OECD, UN Guiding Principles)", 2, "taxonomy_alignment"),
    ("rts_annex", "RTS Annex Templates (pre-contractual, periodic)", 2, "rts"),
    ("rts_pai_table", "RTS PAI Statement Template", 2, "rts"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_sfdr_detail(conn) -> int:
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
    for c, t, l, p in REG_SFDR_DETAIL_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_SFDR_DETAIL_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_SFDR_DETAIL_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_SFDR_DETAIL_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
