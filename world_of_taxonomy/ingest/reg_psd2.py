"""PSD2 regulatory taxonomy ingester.

Payment Services Directive 2 (EU) 2015/2366.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/dir/2015/2366

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 19 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_psd2"
_SYSTEM_NAME = "PSD2"
_FULL_NAME = "Payment Services Directive 2 (EU) 2015/2366"
_VERSION = "2015"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/dir/2015/2366"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_PSD2_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("title_1", "Title I - Subject Matter, Scope, Definitions", 1, None),
    ("title_2", "Title II - Payment Service Providers", 1, None),
    ("title_3", "Title III - Transparency of Conditions and Information Requirements", 1, None),
    ("title_4", "Title IV - Rights and Obligations in Relation to Provision of Payment Services", 1, None),
    ("title_5", "Title V - Operational and Security Risks", 1, None),
    ("scope", "Scope and Definitions (Art 1-4)", 2, "title_1"),
    ("auth", "Authorisation of Payment Institutions (Art 5-11)", 2, "title_2"),
    ("passporting", "Passporting Rights (Art 28)", 2, "title_2"),
    ("agents", "Use of Agents (Art 19)", 2, "title_2"),
    ("info_req", "Information Requirements (Art 38-60)", 2, "title_3"),
    ("sca", "Strong Customer Authentication (SCA) (Art 97)", 2, "title_4"),
    ("open_banking", "Access to Payment Account (Open Banking) (Art 66-67)", 2, "title_4"),
    ("liability", "Liability for Unauthorised Transactions (Art 73-74)", 2, "title_4"),
    ("refund", "Refund Rights (Art 76-77)", 2, "title_4"),
    ("pisp", "Payment Initiation Service Provider (PISP)", 2, "title_4"),
    ("aisp", "Account Information Service Provider (AISP)", 2, "title_4"),
    ("security", "Operational and Security Risk Management (Art 95)", 2, "title_5"),
    ("incident_report", "Incident Reporting (Art 96)", 2, "title_5"),
    ("rts_sca", "RTS on SCA and Common and Secure Communication", 2, "title_4"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_psd2(conn) -> int:
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
    for c, t, l, p in REG_PSD2_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_PSD2_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_PSD2_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_PSD2_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
