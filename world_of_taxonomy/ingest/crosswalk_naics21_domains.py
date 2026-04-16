"""Crosswalk: NAICS 21 (Mining/Oil/Gas) -> Mining Domain Taxonomies.

Links NAICS 21xxx sector nodes to the three Mining domain taxonomies:
  - domain_mining_mineral (dmm_* nodes)
  - domain_mining_method  (dme_* nodes)
  - domain_mining_reserve (dmr_* nodes)

Source: Derived. Open.
"""
from __future__ import annotations

from typing import List, Tuple

# (naics_code, naics_system, domain_code, domain_system)
NAICS_DOMAIN_LINKS: List[Tuple[str, str, str, str]] = [
    # Sector 21 broad links to all mineral/method/reserve root categories
    ("21", "naics_2022", "dmm_metal",   "domain_mining_mineral"),
    ("21", "naics_2022", "dmm_energy",  "domain_mining_mineral"),
    ("21", "naics_2022", "dmm_indmin",  "domain_mining_mineral"),
    ("21", "naics_2022", "dmm_constr",  "domain_mining_mineral"),
    ("21", "naics_2022", "dme_surface", "domain_mining_method"),
    ("21", "naics_2022", "dme_underground", "domain_mining_method"),
    ("21", "naics_2022", "dme_fluid",   "domain_mining_method"),
    ("21", "naics_2022", "dme_process", "domain_mining_method"),
    ("21", "naics_2022", "dmr_res",     "domain_mining_reserve"),
    ("21", "naics_2022", "dmr_cont",    "domain_mining_reserve"),
    ("21", "naics_2022", "dmr_prosp",   "domain_mining_reserve"),

    # 211 Oil and Gas Extraction -> energy minerals + fluid methods + reserves
    ("211",  "naics_2022", "dmm_energy",   "domain_mining_mineral"),
    ("211",  "naics_2022", "dme_fluid",    "domain_mining_method"),
    ("211",  "naics_2022", "dmr_res",      "domain_mining_reserve"),
    ("2111", "naics_2022", "dmm_energy",   "domain_mining_mineral"),
    ("2111", "naics_2022", "dme_fluid",    "domain_mining_method"),
    ("2111", "naics_2022", "dmr_res",      "domain_mining_reserve"),

    # 212 Mining (except Oil and Gas) -> metal/industrial minerals + surface/underground
    ("212",  "naics_2022", "dmm_metal",       "domain_mining_mineral"),
    ("212",  "naics_2022", "dmm_indmin",      "domain_mining_mineral"),
    ("212",  "naics_2022", "dmm_constr",      "domain_mining_mineral"),
    ("212",  "naics_2022", "dme_surface",     "domain_mining_method"),
    ("212",  "naics_2022", "dme_underground", "domain_mining_method"),

    # 2121 Coal Mining
    ("2121", "naics_2022", "dmm_energy",      "domain_mining_mineral"),
    ("2121", "naics_2022", "dme_surface",     "domain_mining_method"),
    ("2121", "naics_2022", "dme_underground", "domain_mining_method"),

    # 2122 Metal Ore Mining
    ("2122", "naics_2022", "dmm_metal",       "domain_mining_mineral"),
    ("2122", "naics_2022", "dme_underground", "domain_mining_method"),

    # 2123 Nonmetallic Mineral Mining
    ("2123", "naics_2022", "dmm_indmin",  "domain_mining_mineral"),
    ("2123", "naics_2022", "dmm_constr",  "domain_mining_mineral"),
    ("2123", "naics_2022", "dme_surface", "domain_mining_method"),

    # 213 Support Activities for Mining -> processing methods
    ("213",  "naics_2022", "dme_process",  "domain_mining_method"),
]


async def ingest_crosswalk_naics21_domains(conn) -> int:
    """Ingest NAICS 21 -> Mining Domain Taxonomy crosswalk.

    Creates equivalence edges linking NAICS Mining sector codes
    to the three mining domain taxonomies.
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
