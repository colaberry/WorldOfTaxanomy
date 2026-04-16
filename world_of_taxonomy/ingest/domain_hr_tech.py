"""Ingest Human Resources Technology Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_hr_tech",
    "HR Technology Types",
    "Human Resources Technology Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("hrt_core", "Core HR Systems", 1, None),
    ("hrt_talent", "Talent Management", 1, None),
    ("hrt_exp", "Employee Experience", 1, None),
    ("hrt_core_hris", "HRIS (Human Resource Information System)", 2, "hrt_core"),
    ("hrt_core_pay", "Payroll processing", 2, "hrt_core"),
    ("hrt_core_ben", "Benefits administration", 2, "hrt_core"),
    ("hrt_core_time", "Time and attendance tracking", 2, "hrt_core"),
    ("hrt_core_comp", "Compensation management", 2, "hrt_core"),
    ("hrt_talent_ats", "Applicant tracking system (ATS)", 2, "hrt_talent"),
    ("hrt_talent_onb", "Onboarding platforms", 2, "hrt_talent"),
    ("hrt_talent_perf", "Performance management", 2, "hrt_talent"),
    ("hrt_talent_lms", "Learning management system (LMS)", 2, "hrt_talent"),
    ("hrt_talent_succ", "Succession planning", 2, "hrt_talent"),
    ("hrt_exp_engage", "Employee engagement surveys", 2, "hrt_exp"),
    ("hrt_exp_wellb", "Wellbeing platforms", 2, "hrt_exp"),
    ("hrt_exp_recog", "Recognition and rewards", 2, "hrt_exp"),
    ("hrt_exp_pulse", "Pulse surveys and analytics", 2, "hrt_exp"),
    ("hrt_exp_deia", "DEI analytics", 2, "hrt_exp"),
]


async def ingest_domain_hr_tech(conn) -> int:
    """Insert or update HR Technology Types system and its nodes. Returns node count."""
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
    await conn.execute(
        "DELETE FROM classification_node WHERE system_id = $1", "domain_hr_tech"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_hr_tech", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_hr_tech",
    )
    return count
