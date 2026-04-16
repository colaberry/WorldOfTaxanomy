"""CSRD regulatory taxonomy ingester.

Corporate Sustainability Reporting Directive (EU) 2022/2464.
Authority: European Parliament and Council.
Source: https://eur-lex.europa.eu/eli/dir/2022/2464

Data provenance: manual_transcription
License: Public Domain (EUR-Lex)

Total: 25 nodes.
"""
from __future__ import annotations
from typing import Optional

_SYSTEM_ID = "reg_csrd"
_SYSTEM_NAME = "CSRD"
_FULL_NAME = "Corporate Sustainability Reporting Directive (EU) 2022/2464"
_VERSION = "2022"
_REGION = "European Union"
_AUTHORITY = "European Parliament and Council"
_SOURCE_URL = "https://eur-lex.europa.eu/eli/dir/2022/2464"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (EUR-Lex)"

REG_CSRD_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("scope", "Scope and Phased Application", 1, None),
    ("reporting", "Reporting Requirements", 1, None),
    ("standards", "European Sustainability Reporting Standards (ESRS)", 1, None),
    ("assurance", "Assurance and Audit", 1, None),
    ("digital", "Digital Tagging and Access", 1, None),
    ("large_co", "Large Companies (from FY 2024)", 2, "scope"),
    ("listed_sme", "Listed SMEs (from FY 2026)", 2, "scope"),
    ("non_eu", "Non-EU Companies (from FY 2028)", 2, "scope"),
    ("double_mat", "Double Materiality Assessment", 2, "reporting"),
    ("value_chain", "Value Chain Reporting", 2, "reporting"),
    ("management_report", "Sustainability Statement in Management Report", 2, "reporting"),
    ("esrs_1", "ESRS 1 - General Requirements", 2, "standards"),
    ("esrs_2", "ESRS 2 - General Disclosures", 2, "standards"),
    ("esrs_e1", "ESRS E1 - Climate Change", 2, "standards"),
    ("esrs_e2", "ESRS E2 - Pollution", 2, "standards"),
    ("esrs_e3", "ESRS E3 - Water and Marine Resources", 2, "standards"),
    ("esrs_e4", "ESRS E4 - Biodiversity and Ecosystems", 2, "standards"),
    ("esrs_e5", "ESRS E5 - Resource Use and Circular Economy", 2, "standards"),
    ("esrs_s1", "ESRS S1 - Own Workforce", 2, "standards"),
    ("esrs_s2", "ESRS S2 - Workers in the Value Chain", 2, "standards"),
    ("esrs_s3", "ESRS S3 - Affected Communities", 2, "standards"),
    ("esrs_s4", "ESRS S4 - Consumers and End-Users", 2, "standards"),
    ("esrs_g1", "ESRS G1 - Business Conduct", 2, "standards"),
    ("limited_assurance", "Limited Assurance Requirement", 2, "assurance"),
    ("xbrl_tagging", "XBRL Digital Tagging (ESEF)", 2, "digital"),
]

_SYSTEM_ROW = (_SYSTEM_ID, _SYSTEM_NAME, _FULL_NAME, _VERSION, _REGION, _AUTHORITY)

async def ingest_reg_csrd(conn) -> int:
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
    for c, t, l, p in REG_CSRD_NODES:
        if p is not None:
            parent_codes.add(p)
    for c, t, l, p in REG_CSRD_NODES:
        if c not in parent_codes:
            leaf_codes.add(c)
    rows = [(_SYSTEM_ID, c, t, l, p, c.split("_")[0], c in leaf_codes)
            for c, t, l, p in REG_CSRD_NODES]
    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""", rows)
    count = len(REG_CSRD_NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, _SYSTEM_ID)
    return count
