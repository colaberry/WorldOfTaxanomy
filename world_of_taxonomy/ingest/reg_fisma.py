"""FISMA regulatory taxonomy ingester.

Federal Information Security Modernization Act of 2014 (44 USC 3551 et seq.).
Authority: US Congress / OMB / NIST / DHS.
Source: https://www.govinfo.gov/content/pkg/PLAW-113publ283/html/PLAW-113publ283.htm

Data provenance: manual_transcription
License: Public Domain

Total: 27 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_fisma"
_SYSTEM_NAME = "FISMA"
_FULL_NAME = "Federal Information Security Modernization Act of 2014 (44 USC 3551 et seq.)"
_VERSION = "2014"
_REGION = "United States"
_AUTHORITY = "US Congress / OMB / NIST / DHS"
_SOURCE_URL = "https://www.govinfo.gov/content/pkg/PLAW-113publ283/html/PLAW-113publ283.htm"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_FISMA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("framework", "FISMA Framework and Requirements", 1, None),
    ("agency", "Agency Responsibilities", 1, None),
    ("oversight", "Oversight and Reporting", 1, None),
    ("nist_integration", "NIST Standards Integration", 1, None),
    ("info_sec_program", "Information Security Program Requirements (3544)", 2, "framework"),
    ("risk_assessment", "Risk Assessment Requirements", 2, "framework"),
    ("security_controls", "Security Controls Implementation", 2, "framework"),
    ("continuous_monitoring", "Continuous Monitoring", 2, "framework"),
    ("incident_response", "Incident Response Procedures", 2, "framework"),
    ("contingency_plan", "Contingency Planning", 2, "framework"),
    ("security_awareness", "Security Awareness Training", 2, "framework"),
    ("cio_role", "Chief Information Officer Responsibilities", 2, "agency"),
    ("agency_head", "Agency Head Accountability", 2, "agency"),
    ("saop_role", "Senior Agency Official for Privacy", 2, "agency"),
    ("auth_to_operate", "Authorization to Operate (ATO) Process", 2, "agency"),
    ("poa_m", "Plan of Action and Milestones (POA&M)", 2, "agency"),
    ("omb_reporting", "OMB Annual Reporting Requirements", 2, "oversight"),
    ("ig_audit", "Inspector General Annual Audits", 2, "oversight"),
    ("congress_report", "Congressional Reporting", 2, "oversight"),
    ("fisma_metrics", "FISMA Performance Metrics (CyberScope)", 2, "oversight"),
    ("gao_review", "GAO Review and Recommendations", 2, "oversight"),
    ("fips_199", "FIPS 199 - Security Categorization", 2, "nist_integration"),
    ("fips_200", "FIPS 200 - Minimum Security Requirements", 2, "nist_integration"),
    ("sp_800_37", "SP 800-37 - Risk Management Framework", 2, "nist_integration"),
    ("sp_800_53", "SP 800-53 - Security and Privacy Controls", 2, "nist_integration"),
    ("sp_800_60", "SP 800-60 - Information Types Mapping", 2, "nist_integration"),
    ("sp_800_137", "SP 800-137 - Continuous Monitoring", 2, "nist_integration"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_fisma(conn) -> int:
    """Ingest FISMA regulatory taxonomy.

    Returns 27 nodes.
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
    for code, title, level, parent in REG_FISMA_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FISMA_NODES:
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
        for code, title, level, parent in REG_FISMA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FISMA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
