"""Workforce Safety cross-cutting domain taxonomy ingester.

Workforce safety taxonomy organizes occupational health and safety concepts (cross-cutting):
  OSHA Standard   (dws_osha*)    - General Industry (29 CFR 1910), Construction (1926), Maritime
  Hazard Category (dws_hazard*)  - chemical, physical, biological, ergonomic, psychosocial
  PPE Type        (dws_ppe*)     - head, eye/face, respiratory, hand, foot, fall protection
  Incident Class  (dws_incident*)- fatality, DART, first aid, near-miss, property damage

Source: OSHA (Occupational Safety and Health Administration) 29 CFR standards.
Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
WORKFORCE_SAFETY_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- OSHA Standard category --
    ("dws_osha",           "OSHA Regulatory Standard",                          1, None),
    ("dws_osha_general",   "General Industry (29 CFR Part 1910)",              2, "dws_osha"),
    ("dws_osha_construct", "Construction (29 CFR Part 1926)",                  2, "dws_osha"),
    ("dws_osha_maritime",  "Maritime (29 CFR Parts 1915-1919)",                2, "dws_osha"),
    ("dws_osha_agri",      "Agriculture (29 CFR Part 1928)",                   2, "dws_osha"),

    # -- Hazard Category --
    ("dws_hazard",          "Occupational Hazard Category",                     1, None),
    ("dws_hazard_chemical", "Chemical Hazard (toxic, corrosive, flammable)",   2, "dws_hazard"),
    ("dws_hazard_physical", "Physical Hazard (noise, radiation, temperature)", 2, "dws_hazard"),
    ("dws_hazard_bio",      "Biological Hazard (pathogen, bloodborne, mold)",  2, "dws_hazard"),
    ("dws_hazard_ergo",     "Ergonomic Hazard (repetitive motion, lifting)",   2, "dws_hazard"),
    ("dws_hazard_psych",    "Psychosocial Hazard (stress, violence, fatigue)", 2, "dws_hazard"),

    # -- PPE Type category --
    ("dws_ppe",            "Personal Protective Equipment (PPE) Type",          1, None),
    ("dws_ppe_head",       "Head Protection (hard hat, bump cap)",              2, "dws_ppe"),
    ("dws_ppe_eye",        "Eye and Face Protection (safety glasses, shield)",  2, "dws_ppe"),
    ("dws_ppe_resp",       "Respiratory Protection (N95, SCBA, supplied air)", 2, "dws_ppe"),
    ("dws_ppe_hand",       "Hand Protection (gloves: cut, chemical, thermal)", 2, "dws_ppe"),
    ("dws_ppe_foot",       "Foot Protection (safety boots, metatarsal guards)",2, "dws_ppe"),
    ("dws_ppe_fall",       "Fall Protection (harness, lanyard, guardrail)",    2, "dws_ppe"),

    # -- Incident Classification --
    ("dws_incident",         "Workplace Incident Classification",               1, None),
    ("dws_incident_fatal",   "Fatality (work-related death, OSHA 300 Log)",   2, "dws_incident"),
    ("dws_incident_dart",    "DART Case (Days Away, Restricted, Transferred)", 2, "dws_incident"),
    ("dws_incident_first",   "First Aid Case (recordable, no lost time)",      2, "dws_incident"),
    ("dws_incident_near",    "Near-Miss (close call, no injury)",              2, "dws_incident"),
    ("dws_incident_prop",    "Property Damage (equipment, facility damage)",   2, "dws_incident"),
]

_DOMAIN_ROW = (
    "domain_workforce_safety",
    "Workforce Safety and Health",
    "OSHA standard, hazard category, PPE type and incident classification taxonomy (cross-cutting)",
    "WorldOfTaxanomy",
    None,
)


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific safety types."""
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


async def ingest_domain_workforce_safety(conn) -> int:
    """Ingest Workforce Safety cross-cutting domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_workforce_safety'). Cross-cutting: links to NAICS
    high-hazard sectors (11, 21, 23, 31-33, 48-49) broadly.

    Returns total workforce safety node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_workforce_safety",
        "Workforce Safety and Health",
        "OSHA standard, hazard category, PPE type and incident classification taxonomy (cross-cutting)",
        "1.0",
        "United States",
        "WorldOfTaxanomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in WORKFORCE_SAFETY_NODES if parent is not None}

    rows = [
        (
            "domain_workforce_safety",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in WORKFORCE_SAFETY_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(WORKFORCE_SAFETY_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_workforce_safety'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_workforce_safety'",
        count,
    )

    # Cross-cutting: link to high-hazard industry NAICS sectors
    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' "
            "AND (code LIKE '11%' OR code LIKE '21%' OR code LIKE '23%' "
            "     OR code LIKE '31%' OR code LIKE '32%' OR code LIKE '33%' "
            "     OR code LIKE '48%' OR code LIKE '49%')"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_workforce_safety", "secondary") for code in naics_codes],
    )

    return count
