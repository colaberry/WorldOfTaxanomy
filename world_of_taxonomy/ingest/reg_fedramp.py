"""FedRAMP regulatory taxonomy ingester.

Federal Risk and Authorization Management Program.
Authority: US General Services Administration (GSA) / OMB.
Source: https://www.fedramp.gov/program-basics/

Data provenance: manual_transcription
License: Public Domain

Total: 40 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_fedramp"
_SYSTEM_NAME = "FedRAMP"
_FULL_NAME = "Federal Risk and Authorization Management Program"
_VERSION = "2024"
_REGION = "United States"
_AUTHORITY = "US General Services Administration (GSA) / OMB"
_SOURCE_URL = "https://www.fedramp.gov/program-basics/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_FEDRAMP_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("baselines", "FedRAMP Security Baselines", 1, None),
    ("process", "Authorization Process", 1, None),
    ("continuous", "Continuous Monitoring", 1, None),
    ("governance", "Governance and Oversight", 1, None),
    ("low_baseline", "FedRAMP Low Baseline", 2, "baselines"),
    ("moderate_baseline", "FedRAMP Moderate Baseline", 2, "baselines"),
    ("high_baseline", "FedRAMP High Baseline", 2, "baselines"),
    ("tailored_li", "FedRAMP Tailored for Low-Impact SaaS (LI-SaaS)", 2, "baselines"),
    ("ac_family", "Access Control (AC)", 2, "baselines"),
    ("at_family", "Awareness and Training (AT)", 2, "baselines"),
    ("au_family", "Audit and Accountability (AU)", 2, "baselines"),
    ("ca_family", "Assessment, Authorization, and Monitoring (CA)", 2, "baselines"),
    ("cm_family", "Configuration Management (CM)", 2, "baselines"),
    ("cp_family", "Contingency Planning (CP)", 2, "baselines"),
    ("ia_family", "Identification and Authentication (IA)", 2, "baselines"),
    ("ir_family", "Incident Response (IR)", 2, "baselines"),
    ("ma_family", "Maintenance (MA)", 2, "baselines"),
    ("mp_family", "Media Protection (MP)", 2, "baselines"),
    ("pe_family", "Physical and Environmental Protection (PE)", 2, "baselines"),
    ("pl_family", "Planning (PL)", 2, "baselines"),
    ("ps_family", "Personnel Security (PS)", 2, "baselines"),
    ("ra_family", "Risk Assessment (RA)", 2, "baselines"),
    ("sa_family", "System and Services Acquisition (SA)", 2, "baselines"),
    ("sc_family", "System and Communications Protection (SC)", 2, "baselines"),
    ("si_family", "System and Information Integrity (SI)", 2, "baselines"),
    ("readiness", "FedRAMP Ready Assessment", 2, "process"),
    ("jab_auth", "JAB Provisional Authorization (P-ATO)", 2, "process"),
    ("agency_auth", "Agency Authorization (ATO)", 2, "process"),
    ("ssp", "System Security Plan (SSP)", 2, "process"),
    ("sap", "Security Assessment Plan (SAP)", 2, "process"),
    ("sar", "Security Assessment Report (SAR)", 2, "process"),
    ("poam", "Plan of Actions and Milestones (POA&M)", 2, "process"),
    ("conmon_monthly", "Monthly Vulnerability Scanning", 2, "continuous"),
    ("conmon_annual", "Annual Security Assessment", 2, "continuous"),
    ("conmon_sig_change", "Significant Change Process", 2, "continuous"),
    ("conmon_reporting", "Continuous Monitoring Reporting", 2, "continuous"),
    ("fedramp_pmo", "FedRAMP Program Management Office (PMO)", 2, "governance"),
    ("fedramp_board", "FedRAMP Board", 2, "governance"),
    ("3pao", "Third Party Assessment Organizations (3PAO)", 2, "governance"),
    ("marketplace", "FedRAMP Marketplace", 2, "governance"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_fedramp(conn) -> int:
    """Ingest FedRAMP regulatory taxonomy.

    Returns 40 nodes.
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
    for code, title, level, parent in REG_FEDRAMP_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_FEDRAMP_NODES:
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
        for code, title, level, parent in REG_FEDRAMP_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_FEDRAMP_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
