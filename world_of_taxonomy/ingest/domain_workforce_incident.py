"""Workforce Safety Incident Classification Types domain taxonomy ingester.

Classifies workplace safety events by incident severity and type.
Orthogonal to safety management system and PPE type.
Aligned with OSHA 300 Log recordkeeping requirements (29 CFR 1904),
ANSI Z16.1/Z16.2 standards, and ICSI incident classification framework.
Used by EHS managers, insurance underwriters, risk consultants, and
OSHA compliance officers for incident tracking and benchmarking.

Code prefix: dwkfinc_
System ID: domain_workforce_incident
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
WORKFORCE_INCIDENT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dwkfinc_fatality", "Fatalities and Life-Altering Events", 1, None),
    ("dwkfinc_fatality_workfatal", "Work-related fatality (death on or from workplace cause)", 2, "dwkfinc_fatality"),
    ("dwkfinc_fatality_amputation", "Work-related amputation or loss of an eye (OSHA severe injury)", 2, "dwkfinc_fatality"),
    ("dwkfinc_recordable", "OSHA Recordable Injuries and Illnesses", 1, None),
    ("dwkfinc_recordable_losttime", "Lost-time incident (DART case, days away from work)", 2, "dwkfinc_recordable"),
    ("dwkfinc_recordable_restricted", "Restricted work or job transfer case (RWTD case)", 2, "dwkfinc_recordable"),
    ("dwkfinc_recordable_medical", "Medical treatment beyond first aid (other recordable)", 2, "dwkfinc_recordable"),
    ("dwkfinc_recordable_illness", "Occupational illness (OSHA 300 Column M: skin, respiratory, poisoning)", 2, "dwkfinc_recordable"),
    ("dwkfinc_firstaid", "First Aid Only Cases", 1, None),
    ("dwkfinc_firstaid_minor", "Minor injury treated on-site (cut, bruise, minor sprain)", 2, "dwkfinc_firstaid"),
    ("dwkfinc_firstaid_nonrecordable", "Non-recordable first aid (single-visit treatment only)", 2, "dwkfinc_firstaid"),
    ("dwkfinc_nearmiss", "Near Misses and Unsafe Conditions", 1, None),
    ("dwkfinc_nearmiss_close", "Close call (potential for injury but no contact/harm)", 2, "dwkfinc_nearmiss"),
    ("dwkfinc_nearmiss_unsafe", "Unsafe condition (hazard identified, no incident yet)", 2, "dwkfinc_nearmiss"),
    ("dwkfinc_nearmiss_obs", "Safety observation (positive or negative field observation)", 2, "dwkfinc_nearmiss"),
    ("dwkfinc_property", "Property Damage and Environmental Events", 1, None),
    ("dwkfinc_property_vehicle", "Vehicle or equipment damage incident", 2, "dwkfinc_property"),
    ("dwkfinc_property_spill", "Chemical spill or environmental release (reportable or not)", 2, "dwkfinc_property"),
]

_DOMAIN_ROW = (
    "domain_workforce_incident",
    "Workforce Safety Incident Classification Types",
    "Occupational safety incident and event classification: OSHA recordables, near misses, fatalities, property damage",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['23', '31', '32', '33', '48', '49', '62']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_workforce_incident(conn) -> int:
    """Ingest Workforce Safety Incident Classification Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_workforce_incident'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_workforce_incident",
        "Workforce Safety Incident Classification Types",
        "Occupational safety incident and event classification: OSHA recordables, near misses, fatalities, property damage",
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

    parent_codes = {parent for _, _, _, parent in WORKFORCE_INCIDENT_NODES if parent is not None}

    rows = [
        (
            "domain_workforce_incident",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in WORKFORCE_INCIDENT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(WORKFORCE_INCIDENT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_workforce_incident'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_workforce_incident'",
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
            [("naics_2022", code, "domain_workforce_incident", "primary") for code in naics_codes],
        )

    return count
