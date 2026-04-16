"""Solvency II regulatory taxonomy ingester.

Directive 2009/138/EC on the taking-up and pursuit of Insurance and Reinsurance.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/dir/2009/138

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 22 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_solvency2"
_SYSTEM_NAME = "Solvency II"
_FULL_NAME = "Directive 2009/138/EC on the taking-up and pursuit of Insurance and Reinsurance"
_VERSION = "2009"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/dir/2009/138"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_SOLVENCY2_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("pillar_1", "Pillar 1 - Quantitative Requirements", 1, None),
    ("pillar_2", "Pillar 2 - Governance and Supervision", 1, None),
    ("pillar_3", "Pillar 3 - Reporting and Disclosure", 1, None),
    ("groups", "Group Supervision", 1, None),
    ("scr", "Solvency Capital Requirement (SCR)", 2, "pillar_1"),
    ("mcr", "Minimum Capital Requirement (MCR)", 2, "pillar_1"),
    ("technical_prov", "Technical Provisions", 2, "pillar_1"),
    ("own_funds", "Own Funds", 2, "pillar_1"),
    ("standard_formula", "Standard Formula", 2, "pillar_1"),
    ("internal_model", "Internal Model Approval", 2, "pillar_1"),
    ("governance", "System of Governance", 2, "pillar_2"),
    ("risk_mgmt", "Risk Management System", 2, "pillar_2"),
    ("orsa", "Own Risk and Solvency Assessment (ORSA)", 2, "pillar_2"),
    ("fit_proper", "Fit and Proper Requirements", 2, "pillar_2"),
    ("outsourcing", "Outsourcing Requirements", 2, "pillar_2"),
    ("supervisory_review", "Supervisory Review Process", 2, "pillar_2"),
    ("sfcr", "Solvency and Financial Condition Report (SFCR)", 2, "pillar_3"),
    ("rsr", "Regular Supervisory Reporting (RSR)", 2, "pillar_3"),
    ("qrt", "Quantitative Reporting Templates (QRTs)", 2, "pillar_3"),
    ("public_disc", "Public Disclosure Requirements", 2, "pillar_3"),
    ("group_scr", "Group SCR Calculation", 2, "groups"),
    ("group_supervision", "Group Supervisor Role", 2, "groups"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_solvency2(conn) -> int:
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
    for c, t, l, p in REG_SOLVENCY2_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_SOLVENCY2_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_SOLVENCY2_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_SOLVENCY2_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
