"""FERPA regulatory taxonomy ingester.

Family Educational Rights and Privacy Act of 1974 (20 USC 1232g).
Authority: US Congress / US Department of Education.
Source: https://www.govinfo.gov/content/pkg/USCODE-2023-title20/html/USCODE-2023-title20-chap31-subchapIII-part4-sec1232g.htm

Data provenance: manual_transcription
License: Public Domain

Total: 30 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_ferpa"
_SYSTEM_NAME = "FERPA"
_FULL_NAME = "Family Educational Rights and Privacy Act of 1974 (20 USC 1232g)"
_VERSION = "1974"
_REGION = "United States"
_AUTHORITY = "US Congress / US Department of Education"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2023-title20/html/USCODE-2023-title20-chap31-subchapIII-part4-sec1232g.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_FERPA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("rights", "Student Rights and Protections", 1, None),
    ("records", "Education Records", 1, None),
    ("disclosure", "Disclosure and Consent", 1, None),
    ("enforcement", "Enforcement and Compliance", 1, None),
    ("definitions", "Definitions and Scope", 1, None),
    ("right_inspect", "Right to Inspect and Review Education Records (99.10)", 2, "rights"),
    ("right_amend", "Right to Request Amendment of Records (99.20)", 2, "rights"),
    ("right_consent", "Right to Consent to Disclosures (99.30)", 2, "rights"),
    ("right_complaint", "Right to File a Complaint (99.63-99.64)", 2, "rights"),
    ("right_annual", "Annual Notification of Rights (99.7)", 2, "rights"),
    ("rec_definition", "Definition of Education Records (99.3)", 2, "records"),
    ("rec_excluded", "Records Excluded from Definition (99.3(b))", 2, "records"),
    ("rec_directory", "Directory Information (99.3, 99.37)", 2, "records"),
    ("rec_maintenance", "Maintenance and Security of Records", 2, "records"),
    ("disc_general", "General Consent Requirement (99.30)", 2, "disclosure"),
    ("disc_exceptions", "Exceptions to Consent (99.31)", 2, "disclosure"),
    ("disc_school_official", "School Officials with Legitimate Educational Interest (99.31(a)(1))", 2, "disclosure"),
    ("disc_transfer", "Transfer to Other Schools (99.31(a)(2))", 2, "disclosure"),
    ("disc_health_safety", "Health or Safety Emergency (99.31(a)(10), 99.36)", 2, "disclosure"),
    ("disc_judicial", "Judicial Order or Subpoena (99.31(a)(9))", 2, "disclosure"),
    ("disc_financial_aid", "Financial Aid Purposes (99.31(a)(4))", 2, "disclosure"),
    ("disc_studies", "Studies for Educational Agencies (99.31(a)(6))", 2, "disclosure"),
    ("disc_recordkeeping", "Recordkeeping Requirements (99.32)", 2, "disclosure"),
    ("enf_office", "Family Policy Compliance Office (FPCO)", 2, "enforcement"),
    ("enf_procedures", "Investigation Procedures (99.63-99.67)", 2, "enforcement"),
    ("enf_penalties", "Funding Termination Remedies (99.67)", 2, "enforcement"),
    ("def_student", "Eligible Student (99.3)", 2, "definitions"),
    ("def_institution", "Educational Agency or Institution (99.1)", 2, "definitions"),
    ("def_parent", "Parent Definition (99.3)", 2, "definitions"),
    ("def_attendance", "Attendance at or Connected to an Institution (99.3)", 2, "definitions"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_ferpa(conn) -> int:
    """Ingest FERPA regulatory taxonomy.

    Returns 30 nodes.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0,
                   source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance,
                   license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )

    leaf_codes = set()
    parent_codes = set()
    for code, title, level, parent in REG_FERPA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FERPA_NODES:
        if code not in parent_codes:
            leaf_codes.add(code)

    rows = [
        (
            _SYSTEM_ID,
            code,
            title,
            level,
            parent,
            code.split("_")[0],
            code in leaf_codes,
        )
        for code, title, level, parent in REG_FERPA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FERPA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
