"""EU Whistleblower Directive regulatory taxonomy ingester.

Directive (EU) 2019/1937 on the protection of persons who report breaches of Union law.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/dir/2019/1937

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 17 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_eu_whistleblower"
_SYSTEM_NAME = "EU Whistleblower Directive"
_FULL_NAME = "Directive (EU) 2019/1937 on the protection of persons who report breaches of Union law"
_VERSION = "2019"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/dir/2019/1937"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_EU_WHISTLEBLOWER_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Definitions", 1, None),
    ("internal", "Internal Reporting Channels", 1, None),
    ("external", "External Reporting Channels", 1, None),
    ("disclosure", "Public Disclosure", 1, None),
    ("protection", "Protection of Reporting Persons", 1, None),
    ("material_scope", "Material Scope (public procurement, financial services, environment, etc.)", 2, "scope"),
    ("personal_scope", "Personal Scope (employees, contractors, shareholders, etc.)", 2, "scope"),
    ("int_channel", "Establishment of Internal Reporting Channels (Art 8)", 2, "internal"),
    ("int_procedure", "Procedure for Internal Reporting (Art 9)", 2, "internal"),
    ("int_followup", "Obligation to Follow Up (Art 9(1)(f))", 2, "internal"),
    ("ext_channel", "Establishment of External Reporting Channels (Art 11)", 2, "external"),
    ("ext_authority", "Competent Authorities (Art 11-12)", 2, "external"),
    ("pub_conditions", "Conditions for Public Disclosure Protection (Art 15)", 2, "disclosure"),
    ("prohibition", "Prohibition of Retaliation (Art 19)", 2, "protection"),
    ("support", "Support Measures (Art 20)", 2, "protection"),
    ("remedies", "Remedies and Burden of Proof (Art 21-22)", 2, "protection"),
    ("penalties", "Penalties (Art 23-25)", 2, "protection"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_eu_whistleblower(conn) -> int:
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
    for c, t, l, p in REG_EU_WHISTLEBLOWER_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_EU_WHISTLEBLOWER_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_EU_WHISTLEBLOWER_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_EU_WHISTLEBLOWER_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
