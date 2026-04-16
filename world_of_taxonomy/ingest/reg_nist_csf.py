"""NIST CSF 2.0 regulatory taxonomy ingester.

NIST Cybersecurity Framework 2.0.
Authority: National Institute of Standards and Technology (NIST).
Source: https://www.nist.gov/cyberframework

Data provenance: manual_transcription
License: Public Domain

Total: 28 nodes.
"""
from __future__ import annotations

from typing import Optional

_SYSTEM_ID = "reg_nist_csf"
_SYSTEM_NAME = "NIST CSF 2.0"
_FULL_NAME = "NIST Cybersecurity Framework 2.0"
_VERSION = "2.0"
_REGION = "United States"
_AUTHORITY = "National Institute of Standards and Technology (NIST)"
_SOURCE_URL = "https://www.nist.gov/cyberframework"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

# (code, title, level, parent_code)
REG_NIST_CSF_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("gv", "GV - Govern", 1, None),
    ("id", "ID - Identify", 1, None),
    ("pr", "PR - Protect", 1, None),
    ("de", "DE - Detect", 1, None),
    ("rs", "RS - Respond", 1, None),
    ("rc", "RC - Recover", 1, None),
    ("gv_oc", "GV.OC - Organizational Context", 2, "gv"),
    ("gv_rm", "GV.RM - Risk Management Strategy", 2, "gv"),
    ("gv_rr", "GV.RR - Roles, Responsibilities, and Authorities", 2, "gv"),
    ("gv_po", "GV.PO - Policy", 2, "gv"),
    ("gv_ov", "GV.OV - Oversight", 2, "gv"),
    ("gv_sc", "GV.SC - Cybersecurity Supply Chain Risk Management", 2, "gv"),
    ("id_am", "ID.AM - Asset Management", 2, "id"),
    ("id_ra", "ID.RA - Risk Assessment", 2, "id"),
    ("id_im", "ID.IM - Improvement", 2, "id"),
    ("pr_aa", "PR.AA - Identity Management, Authentication, and Access Control", 2, "pr"),
    ("pr_at", "PR.AT - Awareness and Training", 2, "pr"),
    ("pr_ds", "PR.DS - Data Security", 2, "pr"),
    ("pr_ps", "PR.PS - Platform Security", 2, "pr"),
    ("pr_ir", "PR.IR - Technology Infrastructure Resilience", 2, "pr"),
    ("de_cm", "DE.CM - Continuous Monitoring", 2, "de"),
    ("de_ae", "DE.AE - Adverse Event Analysis", 2, "de"),
    ("rs_ma", "RS.MA - Incident Management", 2, "rs"),
    ("rs_an", "RS.AN - Incident Analysis", 2, "rs"),
    ("rs_co", "RS.CO - Incident Response Reporting and Communication", 2, "rs"),
    ("rs_mi", "RS.MI - Incident Mitigation", 2, "rs"),
    ("rc_rp", "RC.RP - Incident Recovery Plan Execution", 2, "rc"),
    ("rc_co", "RC.CO - Incident Recovery Communication", 2, "rc"),
]

_SYSTEM_ROW = (
    _SYSTEM_ID,
    _SYSTEM_NAME,
    _FULL_NAME,
    _VERSION,
    _REGION,
    _AUTHORITY,
)


async def ingest_reg_nist_csf(conn) -> int:
    """Ingest NIST CSF 2.0 regulatory taxonomy.

    Returns 28 nodes.
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
    for code, title, level, parent in REG_NIST_CSF_NODES:
        if parent is not None:
            parent_codes.add(parent)
    for code, title, level, parent in REG_NIST_CSF_NODES:
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
        for code, title, level, parent in REG_NIST_CSF_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(REG_NIST_CSF_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, _SYSTEM_ID,
    )

    return count
