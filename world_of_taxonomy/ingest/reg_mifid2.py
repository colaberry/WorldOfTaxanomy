"""MiFID II regulatory taxonomy ingester.

Markets in Financial Instruments Directive 2014/65/EU and MiFIR.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/dir/2014/65

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 24 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_mifid2"
_SYSTEM_NAME = "MiFID II"
_FULL_NAME = "Markets in Financial Instruments Directive 2014/65/EU and MiFIR"
_VERSION = "2014"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/dir/2014/65"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_MIFID2_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Scope and Definitions", 1, None),
    ("title_2", "Title II - Authorisation and Operating Conditions", 1, None),
    ("title_3", "Title III - Regulated Markets", 1, None),
    ("title_4", "Title IV - Trading Venues", 1, None),
    ("title_5", "Title V - Market Data and Transparency", 1, None),
    ("title_6", "Title VI - Supervision", 1, None),
    ("auth", "Authorisation of Investment Firms", 2, "title_2"),
    ("conduct", "Conduct of Business Obligations", 2, "title_2"),
    ("best_exec", "Best Execution (Art 27)", 2, "title_2"),
    ("suitability", "Suitability and Appropriateness (Art 25)", 2, "title_2"),
    ("inducements", "Inducements and Costs Disclosure", 2, "title_2"),
    ("product_gov", "Product Governance Requirements", 2, "title_2"),
    ("recording", "Recording of Telephone Conversations and Electronic Communications", 2, "title_2"),
    ("rm_auth", "Authorisation of Regulated Markets", 2, "title_3"),
    ("rm_access", "Access to Regulated Markets", 2, "title_3"),
    ("otf", "Organised Trading Facilities (OTFs)", 2, "title_4"),
    ("mtf", "Multilateral Trading Facilities (MTFs)", 2, "title_4"),
    ("si", "Systematic Internalisers", 2, "title_4"),
    ("pre_trade", "Pre-Trade Transparency", 2, "title_5"),
    ("post_trade", "Post-Trade Transparency", 2, "title_5"),
    ("consolidated_tape", "Consolidated Tape Provider", 2, "title_5"),
    ("ncas", "National Competent Authorities", 2, "title_6"),
    ("esma", "ESMA Powers", 2, "title_6"),
    ("sanctions", "Administrative Sanctions", 2, "title_6"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_mifid2(conn) -> int:
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
    for c, t, l, p in REG_MIFID2_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_MIFID2_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_MIFID2_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_MIFID2_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
