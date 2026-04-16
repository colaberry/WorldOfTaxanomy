"""Senior Care and Elder Services domain taxonomy ingester.

Senior care taxonomy organizes elder care delivery settings and services:
  Assisted Living  (dsc_assisted*) - independent living, memory care, CCRC
  Nursing Home     (dsc_nursing*)  - skilled nursing, subacute, ventilator
  Home Health      (dsc_home*)     - home health aide, visiting nurse, personal care
  Adult Day        (dsc_day*)      - social model, medical model, dementia-specific
  Hospice          (dsc_hospice*)  - home hospice, inpatient, respite, palliative

Source: CMS (Centers for Medicare and Medicaid Services) facility types and
Administration for Community Living (ACL). Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SENIOR_CARE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Assisted Living category --
    ("dsc_assisted",           "Assisted Living Facilities",                          1, None),
    ("dsc_assisted_standard",  "Standard Assisted Living (ADL support, meals)",       2, "dsc_assisted"),
    ("dsc_assisted_memory",    "Memory Care Unit (Alzheimer, dementia-specific)",     2, "dsc_assisted"),
    ("dsc_assisted_indep",     "Independent Living Community (55+, active senior)",   2, "dsc_assisted"),
    ("dsc_assisted_ccrc",      "Continuing Care Retirement Community (CCRC/LCRC)",    2, "dsc_assisted"),
    ("dsc_assisted_board",     "Board and Care Home (residential, small group)",      2, "dsc_assisted"),

    # -- Nursing Home category --
    ("dsc_nursing",            "Nursing Home and Skilled Nursing",                     1, None),
    ("dsc_nursing_snf",        "Skilled Nursing Facility (SNF, Medicare Part A)",      2, "dsc_nursing"),
    ("dsc_nursing_subacute",   "Subacute Care Unit (post-surgical, wound care)",      2, "dsc_nursing"),
    ("dsc_nursing_ltc",        "Long-Term Care Nursing (chronic, custodial)",         2, "dsc_nursing"),
    ("dsc_nursing_vent",       "Ventilator and Respiratory Care Unit",                2, "dsc_nursing"),

    # -- Home Health category --
    ("dsc_home",               "Home Health Services",                                 1, None),
    ("dsc_home_aide",          "Home Health Aide (bathing, dressing, mobility)",       2, "dsc_home"),
    ("dsc_home_nurse",         "Visiting Nurse Service (skilled nursing at home)",     2, "dsc_home"),
    ("dsc_home_personal",      "Personal Care Attendant (companion, light housework)",2, "dsc_home"),
    ("dsc_home_therapy",       "Home-Based Therapy (PT, OT, speech at home)",         2, "dsc_home"),
    ("dsc_home_telecare",      "Remote Monitoring and Telecare (PERS, sensors)",      2, "dsc_home"),

    # -- Adult Day Services category --
    ("dsc_day",                "Adult Day Services",                                   1, None),
    ("dsc_day_social",         "Social Model Adult Day (recreation, meals, socialization)", 2, "dsc_day"),
    ("dsc_day_medical",        "Medical Model Adult Day (nursing, therapy on-site)",   2, "dsc_day"),
    ("dsc_day_dementia",       "Dementia-Specific Day Program (structured activities)",2, "dsc_day"),

    # -- Hospice Care category --
    ("dsc_hospice",            "Hospice and Palliative Care",                           1, None),
    ("dsc_hospice_home",       "Home Hospice (end-of-life care at patient home)",      2, "dsc_hospice"),
    ("dsc_hospice_inpatient",  "Inpatient Hospice Unit (symptom management, GIP)",    2, "dsc_hospice"),
    ("dsc_hospice_respite",    "Respite Hospice Care (caregiver relief, short-term)",  2, "dsc_hospice"),
    ("dsc_hospice_palliative", "Palliative Care Consultation (non-hospice, concurrent)",2, "dsc_hospice"),
]

_DOMAIN_ROW = (
    "domain_senior_care",
    "Senior Care and Elder Services Types",
    "Assisted living, nursing home, home health, adult day and hospice care taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["6231", "6232", "6233"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific senior care types."""
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


async def ingest_domain_senior_care(conn) -> int:
    """Ingest Senior Care and Elder Services domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_senior_care'), and links NAICS 6231/6232/6233 nodes
    via node_taxonomy_link.

    Returns total senior care node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_senior_care",
        "Senior Care and Elder Services Types",
        "Assisted living, nursing home, home health, adult day and hospice care taxonomy",
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

    parent_codes = {parent for _, _, _, parent in SENIOR_CARE_NODES if parent is not None}

    rows = [
        (
            "domain_senior_care",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SENIOR_CARE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SENIOR_CARE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_senior_care'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_senior_care'",
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
            [("naics_2022", code, "domain_senior_care", "primary") for code in naics_codes],
        )

    return count
