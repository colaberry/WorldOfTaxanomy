"""Crosswalk: NAICS 23 (Construction) -> Construction Domain Taxonomies.

Links NAICS 23xxx sector nodes to the two Construction domain taxonomies:
  - domain_const_trade    (dct_* nodes)
  - domain_const_building (dcb_* nodes)

Source: Derived. Open.
"""
from __future__ import annotations

from typing import List, Tuple

# (naics_code, naics_system, domain_code, domain_system)
NAICS_DOMAIN_LINKS: List[Tuple[str, str, str, str]] = [
    # Sector 23 broad links to all trade/building root categories
    ("23",  "naics_2022", "dct_site",    "domain_const_trade"),
    ("23",  "naics_2022", "dct_struct",  "domain_const_trade"),
    ("23",  "naics_2022", "dct_mep",     "domain_const_trade"),
    ("23",  "naics_2022", "dct_finish",  "domain_const_trade"),
    ("23",  "naics_2022", "dcb_resid",   "domain_const_building"),
    ("23",  "naics_2022", "dcb_comm",    "domain_const_building"),
    ("23",  "naics_2022", "dcb_indust",  "domain_const_building"),
    ("23",  "naics_2022", "dcb_inst",    "domain_const_building"),

    # 236 Construction of Buildings -> all building types + finish trades
    ("236",  "naics_2022", "dcb_resid",   "domain_const_building"),
    ("236",  "naics_2022", "dcb_comm",    "domain_const_building"),
    ("236",  "naics_2022", "dct_finish",  "domain_const_trade"),
    ("236",  "naics_2022", "dct_struct",  "domain_const_trade"),

    # 2361 Residential Building Construction
    ("2361", "naics_2022", "dcb_resid",  "domain_const_building"),
    ("2361", "naics_2022", "dct_site",   "domain_const_trade"),
    ("2361", "naics_2022", "dct_struct", "domain_const_trade"),

    # 2362 Nonresidential Building Construction
    ("2362", "naics_2022", "dcb_comm",   "domain_const_building"),
    ("2362", "naics_2022", "dcb_indust", "domain_const_building"),
    ("2362", "naics_2022", "dcb_inst",   "domain_const_building"),

    # 237 Heavy and Civil Engineering Construction -> site work
    ("237",  "naics_2022", "dct_site",   "domain_const_trade"),

    # 238 Specialty Trade Contractors -> MEP and finish trades
    ("238",  "naics_2022", "dct_mep",    "domain_const_trade"),
    ("238",  "naics_2022", "dct_finish", "domain_const_trade"),
    ("238",  "naics_2022", "dct_struct", "domain_const_trade"),

    # 2381 Foundation and Structure -> structural
    ("2381", "naics_2022", "dct_struct", "domain_const_trade"),
    ("2381", "naics_2022", "dct_site",   "domain_const_trade"),

    # 2382 Building Equipment Contractors -> MEP
    ("2382", "naics_2022", "dct_mep",    "domain_const_trade"),

    # 2383 Building Finishing Contractors -> finish
    ("2383", "naics_2022", "dct_finish", "domain_const_trade"),
]


async def ingest_crosswalk_naics23_domains(conn) -> int:
    """Ingest NAICS 23 -> Construction Domain Taxonomy crosswalk.

    Creates equivalence edges linking NAICS Construction sector codes
    to the two construction domain taxonomies.
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
