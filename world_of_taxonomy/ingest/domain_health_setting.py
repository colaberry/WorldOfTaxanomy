"""Health Care Setting domain taxonomy ingester.

Healthcare setting taxonomy organizes care delivery environments:
  Inpatient     (dhs_inpt*)  - acute hospital, ICU, surgical, rehabilitation
  Outpatient    (dhs_outpt*) - clinic, physician office, urgent care, ASC
  Post-Acute    (dhs_post*)  - skilled nursing, home health, hospice, LTACH
  Behavioral    (dhs_beh*)   - inpatient psych, outpatient mental health, SUD
  Telehealth    (dhs_tele*)  - synchronous, asynchronous, remote monitoring

Source: CMS (Centers for Medicare and Medicaid Services) facility types. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
HEALTH_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Inpatient category --
    ("dhs_inpt",          "Inpatient Care Settings",                         1, None),
    ("dhs_inpt_acute",    "Acute Care Hospital (general medical/surgical)",  2, "dhs_inpt"),
    ("dhs_inpt_icu",      "Intensive Care Unit (ICU, CCU, NICU)",           2, "dhs_inpt"),
    ("dhs_inpt_surg",     "Inpatient Surgical (OR, PACU)",                  2, "dhs_inpt"),
    ("dhs_inpt_rehab",    "Inpatient Rehabilitation Hospital (IRF)",        2, "dhs_inpt"),

    # -- Outpatient category --
    ("dhs_outpt",         "Outpatient Care Settings",                        1, None),
    ("dhs_outpt_clinic",  "Outpatient Clinic and Health Center (FQHC)",     2, "dhs_outpt"),
    ("dhs_outpt_phys",    "Physician Office and Medical Practice",           2, "dhs_outpt"),
    ("dhs_outpt_urgent",  "Urgent Care Center (walk-in clinic)",            2, "dhs_outpt"),
    ("dhs_outpt_asc",     "Ambulatory Surgery Center (ASC)",                2, "dhs_outpt"),
    ("dhs_outpt_diag",    "Diagnostic Center (imaging, lab, radiology)",    2, "dhs_outpt"),

    # -- Post-Acute Care category --
    ("dhs_post",          "Post-Acute Care Settings",                        1, None),
    ("dhs_post_snf",      "Skilled Nursing Facility (SNF, nursing home)",   2, "dhs_post"),
    ("dhs_post_home",     "Home Health Agency (HHA, home visits)",          2, "dhs_post"),
    ("dhs_post_hospice",  "Hospice Care (palliative, end-of-life)",         2, "dhs_post"),
    ("dhs_post_ltach",    "Long-Term Acute Care Hospital (LTACH)",          2, "dhs_post"),

    # -- Behavioral Health category --
    ("dhs_beh",           "Behavioral Health Settings",                      1, None),
    ("dhs_beh_inpt",      "Inpatient Psychiatric Hospital (IPF)",           2, "dhs_beh"),
    ("dhs_beh_outpt",     "Outpatient Mental Health and SUD Clinic",        2, "dhs_beh"),
    ("dhs_beh_crisis",    "Crisis Stabilization Unit (CSU, 23-hour)",       2, "dhs_beh"),

    # -- Telehealth category --
    ("dhs_tele",          "Telehealth and Virtual Care",                     1, None),
    ("dhs_tele_sync",     "Synchronous Telehealth (live video visit)",      2, "dhs_tele"),
    ("dhs_tele_async",    "Asynchronous (store-and-forward, e-consult)",    2, "dhs_tele"),
    ("dhs_tele_rpm",      "Remote Patient Monitoring (RPM, wearables)",     2, "dhs_tele"),
]

_DOMAIN_ROW = (
    "domain_health_setting",
    "Health Care Settings",
    "Inpatient, outpatient, post-acute, behavioral health and telehealth care setting taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["62"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific care settings."""
    parts = code.split("_")
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_health_setting(conn) -> int:
    """Ingest Health Care Setting domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_health_setting'), and links NAICS 62xxx nodes
    via node_taxonomy_link.

    Returns total care setting node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_health_setting",
        "Health Care Settings",
        "Inpatient, outpatient, post-acute, behavioral health and telehealth care setting taxonomy",
        "1.0",
        "United States",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in HEALTH_NODES if parent is not None}

    rows = [
        (
            "domain_health_setting",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in HEALTH_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(HEALTH_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_health_setting'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_health_setting'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '62%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_health_setting", "primary") for code in naics_codes],
    )

    return count
