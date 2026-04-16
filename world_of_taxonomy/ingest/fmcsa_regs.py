"""FMCSA Regulatory Codes ingester.

Federal Motor Carrier Safety Administration (FMCSA) regulatory program areas.
Published by US DOT/FMCSA. Public domain.
Reference: https://www.fmcsa.dot.gov/regulations

Hand-coded hierarchy of FMCSA regulatory requirements, organized by program area.

Hierarchy (2 levels):
  Category    (level 1): major regulatory program area
  Requirement (level 2): specific regulatory requirement within the category

Codes: 'fmcsa_{category}' and 'fmcsa_{category}_{seq}'
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
FMCSA_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # ── Hours of Service (HOS) ──
    ("fmcsa_hos", "Hours of Service (HOS)", 1, None),
    ("fmcsa_hos_1", "11-Hour Driving Limit (property-carrying)", 2, "fmcsa_hos"),
    ("fmcsa_hos_2", "14-Hour On-Duty Window", 2, "fmcsa_hos"),
    ("fmcsa_hos_3", "60/70-Hour Weekly Limit", 2, "fmcsa_hos"),
    ("fmcsa_hos_4", "10-Hour Off-Duty Requirement", 2, "fmcsa_hos"),
    ("fmcsa_hos_5", "30-Minute Break Requirement", 2, "fmcsa_hos"),
    ("fmcsa_hos_6", "Sleeper Berth Provision", 2, "fmcsa_hos"),
    ("fmcsa_hos_7", "Short-Haul Exemption (150-air-mile radius)", 2, "fmcsa_hos"),
    ("fmcsa_hos_8", "Adverse Driving Conditions Exception", 2, "fmcsa_hos"),
    ("fmcsa_hos_9", "Emergency Conditions Exception", 2, "fmcsa_hos"),
    ("fmcsa_hos_10", "10-Hour Driving Limit (passenger-carrying)", 2, "fmcsa_hos"),
    ("fmcsa_hos_11", "15-Hour On-Duty Window (passenger-carrying)", 2, "fmcsa_hos"),

    # ── Electronic Logging Devices (ELD) ──
    ("fmcsa_eld", "Electronic Logging Devices (ELD)", 1, None),
    ("fmcsa_eld_1", "ELD Mandate - Required Use", 2, "fmcsa_eld"),
    ("fmcsa_eld_2", "ELD Technical Specifications", 2, "fmcsa_eld"),
    ("fmcsa_eld_3", "ELD Malfunction Reporting Procedures", 2, "fmcsa_eld"),
    ("fmcsa_eld_4", "ELD Data Transfer Methods", 2, "fmcsa_eld"),
    ("fmcsa_eld_5", "ELD Exemptions (pre-2000 engines, short-haul, driveaway)", 2, "fmcsa_eld"),
    ("fmcsa_eld_6", "Automatic On-Board Recording Devices (AOBRD) Transition", 2, "fmcsa_eld"),

    # ── Commercial Driver's License (CDL) ──
    ("fmcsa_cdl", "Commercial Driver's License (CDL)", 1, None),
    ("fmcsa_cdl_1", "CDL Classes (A, B, C) and Vehicle Requirements", 2, "fmcsa_cdl"),
    ("fmcsa_cdl_2", "CDL Endorsements (H, N, P, S, T, X)", 2, "fmcsa_cdl"),
    ("fmcsa_cdl_3", "CDL Knowledge and Skills Tests", 2, "fmcsa_cdl"),
    ("fmcsa_cdl_4", "CDL Disqualifying Offenses", 2, "fmcsa_cdl"),
    ("fmcsa_cdl_5", "CDL Medical Certificate Requirements", 2, "fmcsa_cdl"),
    ("fmcsa_cdl_6", "Non-CDL Commercial Motor Vehicle (CMV) Rules", 2, "fmcsa_cdl"),
    ("fmcsa_cdl_7", "Entry-Level Driver Training (ELDT) Requirements", 2, "fmcsa_cdl"),

    # ── Drug and Alcohol Testing (DAT) ──
    ("fmcsa_dat", "Drug and Alcohol Testing (DAT)", 1, None),
    ("fmcsa_dat_1", "Pre-Employment Drug Testing", 2, "fmcsa_dat"),
    ("fmcsa_dat_2", "Random Drug and Alcohol Testing (50%/10%)", 2, "fmcsa_dat"),
    ("fmcsa_dat_3", "Post-Accident Testing Requirements", 2, "fmcsa_dat"),
    ("fmcsa_dat_4", "Reasonable Suspicion Testing", 2, "fmcsa_dat"),
    ("fmcsa_dat_5", "Return-to-Duty Process (SAP Program)", 2, "fmcsa_dat"),
    ("fmcsa_dat_6", "Drug and Alcohol Clearinghouse", 2, "fmcsa_dat"),
    ("fmcsa_dat_7", "Prohibited Substances List", 2, "fmcsa_dat"),

    # ── Vehicle Inspection and Maintenance (VIM) ──
    ("fmcsa_vim", "Vehicle Inspection and Maintenance (VIM)", 1, None),
    ("fmcsa_vim_1", "Annual Periodic Inspection Requirement", 2, "fmcsa_vim"),
    ("fmcsa_vim_2", "Pre-Trip and Post-Trip Driver Inspection", 2, "fmcsa_vim"),
    ("fmcsa_vim_3", "Driver Vehicle Inspection Report (DVIR)", 2, "fmcsa_vim"),
    ("fmcsa_vim_4", "Roadside Inspection (Level I-VI)", 2, "fmcsa_vim"),
    ("fmcsa_vim_5", "Out-of-Service Criteria", 2, "fmcsa_vim"),
    ("fmcsa_vim_6", "Maintenance Records Retention Requirements", 2, "fmcsa_vim"),
    ("fmcsa_vim_7", "Brake System Requirements", 2, "fmcsa_vim"),
    ("fmcsa_vim_8", "Tire and Wheel Requirements", 2, "fmcsa_vim"),
    ("fmcsa_vim_9", "Cargo Securement Standards", 2, "fmcsa_vim"),

    # ── Hazardous Materials (HAZMAT) ──
    ("fmcsa_hazmat", "Hazardous Materials (HAZMAT)", 1, None),
    ("fmcsa_hazmat_1", "HAZMAT Registration Requirements", 2, "fmcsa_hazmat"),
    ("fmcsa_hazmat_2", "Hazardous Materials Endorsement (HME)", 2, "fmcsa_hazmat"),
    ("fmcsa_hazmat_3", "Placarding Requirements (DOT Classes 1-9)", 2, "fmcsa_hazmat"),
    ("fmcsa_hazmat_4", "Shipping Papers and Emergency Response Information", 2, "fmcsa_hazmat"),
    ("fmcsa_hazmat_5", "Security Plans for Hazardous Materials", 2, "fmcsa_hazmat"),
    ("fmcsa_hazmat_6", "Route Restrictions for HAZMAT Transport", 2, "fmcsa_hazmat"),
    ("fmcsa_hazmat_7", "HAZMAT Incident Reporting (OMC-3 Form)", 2, "fmcsa_hazmat"),

    # ── Financial Responsibility (FR) ──
    ("fmcsa_fr", "Financial Responsibility (FR)", 1, None),
    ("fmcsa_fr_1", "Minimum Insurance for Property Carriers ($750K-$5M)", 2, "fmcsa_fr"),
    ("fmcsa_fr_2", "Minimum Insurance for Passenger Carriers ($1.5M-$5M)", 2, "fmcsa_fr"),
    ("fmcsa_fr_3", "Surety Bond Requirements", 2, "fmcsa_fr"),
    ("fmcsa_fr_4", "BMC-91 Insurance Filing", 2, "fmcsa_fr"),
    ("fmcsa_fr_5", "Cargo Liability Insurance", 2, "fmcsa_fr"),

    # ── Operating Authority (OA) ──
    ("fmcsa_oa", "Operating Authority (OA)", 1, None),
    ("fmcsa_oa_1", "USDOT Number Registration", 2, "fmcsa_oa"),
    ("fmcsa_oa_2", "Motor Carrier (MC) Number - Interstate Authority", 2, "fmcsa_oa"),
    ("fmcsa_oa_3", "Unified Registration System (URS)", 2, "fmcsa_oa"),
    ("fmcsa_oa_4", "Broker Authority Registration", 2, "fmcsa_oa"),
    ("fmcsa_oa_5", "Freight Forwarder Authority", 2, "fmcsa_oa"),
    ("fmcsa_oa_6", "Process Agent Designation (BOC-3)", 2, "fmcsa_oa"),
    ("fmcsa_oa_7", "Annual Update Requirements (MCS-150)", 2, "fmcsa_oa"),

    # ── Carrier Safety Fitness (CSF) ──
    ("fmcsa_csf", "Carrier Safety Fitness (CSF)", 1, None),
    ("fmcsa_csf_1", "Safety Fitness Determination (Satisfactory/Conditional/Unsatisfactory)", 2, "fmcsa_csf"),
    ("fmcsa_csf_2", "Compliance, Safety, Accountability (CSA) BASIC Scores", 2, "fmcsa_csf"),
    ("fmcsa_csf_3", "Safety Measurement System (SMS) Percentiles", 2, "fmcsa_csf"),
    ("fmcsa_csf_4", "Interventions (Warning Letter, Offsite Investigation, Onsite)", 2, "fmcsa_csf"),
    ("fmcsa_csf_5", "Consent Orders and Settlement Agreements", 2, "fmcsa_csf"),
    ("fmcsa_csf_6", "Operations Out-of-Service Orders", 2, "fmcsa_csf"),
    ("fmcsa_csf_7", "New Entrant Safety Audit Program", 2, "fmcsa_csf"),

    # ── Accident Reporting (AR) ──
    ("fmcsa_ar", "Accident Reporting (AR)", 1, None),
    ("fmcsa_ar_1", "Recordable Accident Definition (MCS-150C thresholds)", 2, "fmcsa_ar"),
    ("fmcsa_ar_2", "Accident Register Maintenance Requirements", 2, "fmcsa_ar"),
    ("fmcsa_ar_3", "OMC Accident Report Filing Deadlines", 2, "fmcsa_ar"),
    ("fmcsa_ar_4", "Fatality and Serious Injury Notification", 2, "fmcsa_ar"),
]

_SYSTEM_ROW = (
    "fmcsa_regs",
    "FMCSA Regulations",
    "Federal Motor Carrier Safety Administration - Regulatory Requirements",
    "2024",
    "United States",
    "US Department of Transportation / FMCSA",
)


def _determine_level(code: str) -> int:
    """Return hierarchy level from FMCSA code structure.

    'fmcsa_hos'   -> 1 (Category - 2 parts separated by underscore)
    'fmcsa_hos_1' -> 2 (Requirement - 3 parts)
    """
    return len(code.split("_")) - 1


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_fmcsa_regs(conn) -> int:
    """Ingest FMCSA regulatory program areas and requirements.

    Hand-coded from fmcsa.dot.gov (public domain).
    Returns total node count inserted.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        *_SYSTEM_ROW,
    )

    parent_codes = {parent for _, _, _, parent in FMCSA_NODES if parent is not None}

    rows = [
        (
            "fmcsa_regs",
            code,
            title,
            level,
            parent,
            code.split("_")[1],        # sector_code = program area (e.g. 'hos')
            code not in parent_codes,   # is_leaf
        )
        for code, title, level, parent in FMCSA_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(FMCSA_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'fmcsa_regs'",
        count,
    )

    return count
