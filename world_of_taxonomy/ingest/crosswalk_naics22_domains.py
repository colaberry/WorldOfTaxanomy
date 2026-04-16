"""Crosswalk: NAICS 22 (Utilities) -> Utility Domain Taxonomies.

Links NAICS 22xxx sector nodes to the two Utility domain taxonomies:
  - domain_util_energy (due_* nodes)
  - domain_util_grid   (dug_* nodes)

Source: Derived. Open.
"""
from __future__ import annotations

from typing import List, Tuple

# (naics_code, naics_system, domain_code, domain_system)
NAICS_DOMAIN_LINKS: List[Tuple[str, str, str, str]] = [
    # Sector 22 broad links to all energy/grid root categories
    ("22",  "naics_2022", "due_fossil",  "domain_util_energy"),
    ("22",  "naics_2022", "due_nuclear", "domain_util_energy"),
    ("22",  "naics_2022", "due_renew",   "domain_util_energy"),
    ("22",  "naics_2022", "due_storage", "domain_util_energy"),
    ("22",  "naics_2022", "dug_voltage", "domain_util_grid"),
    ("22",  "naics_2022", "dug_region",  "domain_util_grid"),

    # 2211 Electric Power Generation, Transmission, Distribution
    ("221",  "naics_2022", "due_fossil",  "domain_util_energy"),
    ("221",  "naics_2022", "due_nuclear", "domain_util_energy"),
    ("221",  "naics_2022", "due_renew",   "domain_util_energy"),
    ("221",  "naics_2022", "dug_voltage", "domain_util_grid"),
    ("221",  "naics_2022", "dug_region",  "domain_util_grid"),

    # 22111 Electric Power Generation
    ("22111", "naics_2022", "due_fossil",  "domain_util_energy"),
    ("22111", "naics_2022", "due_nuclear", "domain_util_energy"),
    ("22111", "naics_2022", "due_renew",   "domain_util_energy"),
    ("22111", "naics_2022", "due_storage", "domain_util_energy"),

    # 22112 Electric Power Transmission and Distribution
    ("22112", "naics_2022", "dug_voltage", "domain_util_grid"),
    ("22112", "naics_2022", "dug_region",  "domain_util_grid"),

    # 2212 Natural Gas Distribution -> fossil fuel energy
    ("2212", "naics_2022", "due_fossil", "domain_util_energy"),

    # 2213 Water, Sewage, Other Systems
    ("2213", "naics_2022", "dug_voltage", "domain_util_grid"),
]


async def ingest_crosswalk_naics22_domains(conn) -> int:
    """Ingest NAICS 22 -> Utility Domain Taxonomy crosswalk.

    Creates equivalence edges linking NAICS Utilities sector codes
    to the two utility domain taxonomies.
    All edges use match_type='broad'.

    Returns count of edges inserted.
    """
    rows = [
        (naics_code, naics_sys, domain_code, domain_sys, "broad")
        for naics_code, naics_sys, domain_code, domain_sys in NAICS_DOMAIN_LINKS
    ]

    await conn.executemany(
        """INSERT INTO equivalence
               (source_code, source_system, target_code, target_system, match_type)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (source_code, source_system, target_code, target_system) DO NOTHING""",
        rows,
    )

    return len(rows)
