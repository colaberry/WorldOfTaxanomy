"""Cyber Framework domain taxonomy ingester.

Organizes cybersecurity framework and compliance standard types aligned
with NAICS 5415 (Computer systems design and related services).

Code prefix: dcf_
Categories: Risk Frameworks, Compliance Standards, Security Controls, Privacy Frameworks.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CYBER_FRAMEWORK_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Risk Frameworks --
    ("dcf_risk",             "Cybersecurity Risk Frameworks",                          1, None),
    ("dcf_risk_nist",        "NIST Cybersecurity Framework (CSF 2.0, identify-protect-detect)", 2, "dcf_risk"),
    ("dcf_risk_iso27001",    "ISO 27001/27002 (ISMS, risk treatment, Annex A controls)", 2, "dcf_risk"),
    ("dcf_risk_cobit",       "COBIT Framework (IT governance, management objectives)", 2, "dcf_risk"),
    ("dcf_risk_fair",        "FAIR Risk Quantification (factor analysis, loss magnitude)", 2, "dcf_risk"),
    ("dcf_risk_cis",         "CIS Controls (implementation groups, benchmarks, CAT)", 2, "dcf_risk"),

    # -- Compliance Standards --
    ("dcf_comply",           "Compliance Standards and Regulations",                   1, None),
    ("dcf_comply_pci",       "PCI DSS (payment card data, SAQ, QSA, network segmentation)", 2, "dcf_comply"),
    ("dcf_comply_sox",       "SOX IT Controls (Sarbanes-Oxley, ITGC, change management)", 2, "dcf_comply"),
    ("dcf_comply_hipaa",     "HIPAA Security Rule (PHI safeguards, risk analysis, BAA)", 2, "dcf_comply"),
    ("dcf_comply_fedramp",   "FedRAMP (cloud authorization, POAM, 3PAO assessment)",   2, "dcf_comply"),
    ("dcf_comply_cmmc",      "CMMC (Cybersecurity Maturity Model, DoD, level assessment)", 2, "dcf_comply"),

    # -- Security Controls --
    ("dcf_controls",         "Security Control Frameworks",                            1, None),
    ("dcf_controls_nist800", "NIST SP 800-53 (federal controls, baselines, overlays)", 2, "dcf_controls"),
    ("dcf_controls_mitre",   "MITRE ATT&CK (tactics, techniques, procedures, groups)", 2, "dcf_controls"),
    ("dcf_controls_owasp",   "OWASP Framework (Top 10, ASVS, SAMM, testing guide)",   2, "dcf_controls"),
    ("dcf_controls_zero",    "Zero Trust Architecture (NIST 800-207, microsegmentation)", 2, "dcf_controls"),
    ("dcf_controls_soc2",    "SOC 2 Trust Services Criteria (security, availability)", 2, "dcf_controls"),

    # -- Privacy Frameworks --
    ("dcf_privacy",          "Privacy and Data Protection Frameworks",                 1, None),
    ("dcf_privacy_gdpr",     "GDPR Framework (data subject rights, DPIA, DPO, SCCs)", 2, "dcf_privacy"),
    ("dcf_privacy_ccpa",     "CCPA and CPRA (consumer rights, opt-out, data broker)",  2, "dcf_privacy"),
    ("dcf_privacy_nistpf",   "NIST Privacy Framework (identify, govern, control, communicate)", 2, "dcf_privacy"),
    ("dcf_privacy_iso27701", "ISO 27701 (PIMS, privacy controls, processor obligations)", 2, "dcf_privacy"),
]

_DOMAIN_ROW = (
    "domain_cyber_framework",
    "Cyber Framework Types",
    "Risk frameworks, compliance standards, security controls, "
    "and privacy framework taxonomy for cybersecurity",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefix: 5415 (Computer systems design and related services)
_NAICS_PREFIXES = ["5415"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific cyber framework types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_cyber_framework(conn) -> int:
    """Ingest Cyber Framework domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_cyber_framework'), and links NAICS 5415 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_cyber_framework",
        "Cyber Framework Types",
        "Risk frameworks, compliance standards, security controls, "
        "and privacy framework taxonomy for cybersecurity",
        "1.0",
        "Global",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in CYBER_FRAMEWORK_NODES if parent is not None}

    rows = [
        (
            "domain_cyber_framework",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in CYBER_FRAMEWORK_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CYBER_FRAMEWORK_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_cyber_framework'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_cyber_framework'",
        count,
    )

    naics_codes = [
        row["code"]
        for prefix in _NAICS_PREFIXES
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE $1",
            prefix + "%",
        )
    ]

    if naics_codes:
        await conn.executemany(
            """INSERT INTO node_taxonomy_link
                   (system_id, node_code, taxonomy_id, relevance)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
            [("naics_2022", code, "domain_cyber_framework", "primary") for code in naics_codes],
        )

    return count
