"""Crosswalk: NAICS 11 (Agriculture) -> Agricultural Domain Taxonomies.

Links NAICS 11xxx Sector 11 nodes to the four Agriculture domain taxonomies:
  - domain_ag_crop      (dac_* nodes) via NAICS 111xxx Crop Production
  - domain_ag_livestock (dal_* nodes) via NAICS 112xxx Animal Production
  - domain_ag_method    (dam_* nodes) via NAICS 11xxx broadly
  - domain_ag_grade     (dag_* nodes) via NAICS 11xxx broadly

Uses the equivalence table with match_type='broad' (domain taxonomies are
orthogonal cross-cutting views, not 1:1 mappings).

Source: Derived. Open.
"""
from __future__ import annotations

from typing import List, Tuple

# (naics_code, naics_system, domain_code, domain_system)
# NAICS 111xxx = Crop Production -> ag_crop domain
# NAICS 112xxx = Animal Production -> ag_livestock domain
# NAICS 1111  = Oilseed and Grain Farming -> crop grain/oil categories
# NAICS 1112  = Vegetable and Melon Farming -> crop veg category
# NAICS 1113  = Fruit and Tree Nut Farming -> crop fruit category
# NAICS 1114  = Greenhouse, Nursery, Floriculture -> crop specialty
# NAICS 1119  = Other Crop Farming -> crop fiber/sugar/forage
# NAICS 1121  = Cattle Ranching and Farming -> livestock cattle
# NAICS 1122  = Hog and Pig Farming -> livestock swine
# NAICS 1123  = Poultry and Egg Production -> livestock poultry
# NAICS 1124  = Sheep and Goat Farming -> livestock small
# NAICS 1125  = Aquaculture -> livestock aqua
# NAICS 1129  = Other Animal Production -> livestock other

NAICS_DOMAIN_LINKS: List[Tuple[str, str, str, str]] = [
    # Sector 11 -> both crop and livestock methods/grades (broad links)
    ("11",   "naics_2022", "dam_sys",   "domain_ag_method"),
    ("11",   "naics_2022", "dam_scale", "domain_ag_method"),
    ("11",   "naics_2022", "dam_irr",   "domain_ag_method"),
    ("11",   "naics_2022", "dam_till",  "domain_ag_method"),
    ("11",   "naics_2022", "dam_cert",  "domain_ag_method"),
    ("11",   "naics_2022", "dag_grain", "domain_ag_grade"),
    ("11",   "naics_2022", "dag_live",  "domain_ag_grade"),
    ("11",   "naics_2022", "dag_prod",  "domain_ag_grade"),
    ("11",   "naics_2022", "dag_dairy", "domain_ag_grade"),
    ("11",   "naics_2022", "dag_egg",   "domain_ag_grade"),

    # 111 Crop Production -> crop domain root categories
    ("111",  "naics_2022", "dac_grain",  "domain_ag_crop"),
    ("111",  "naics_2022", "dac_oil",    "domain_ag_crop"),
    ("111",  "naics_2022", "dac_veg",    "domain_ag_crop"),
    ("111",  "naics_2022", "dac_fruit",  "domain_ag_crop"),
    ("111",  "naics_2022", "dac_fiber",  "domain_ag_crop"),
    ("111",  "naics_2022", "dac_sugar",  "domain_ag_crop"),
    ("111",  "naics_2022", "dac_forage", "domain_ag_crop"),
    ("111",  "naics_2022", "dac_spec",   "domain_ag_crop"),

    # 1111 Oilseed and Grain -> grain and oil categories
    ("1111", "naics_2022", "dac_grain", "domain_ag_crop"),
    ("1111", "naics_2022", "dac_oil",   "domain_ag_crop"),

    # 1112 Vegetable and Melon Farming -> vegetable category
    ("1112", "naics_2022", "dac_veg",  "domain_ag_crop"),

    # 1113 Fruit and Tree Nut Farming -> fruit category
    ("1113", "naics_2022", "dac_fruit", "domain_ag_crop"),

    # 1114 Greenhouse, Nursery, Floriculture -> specialty category
    ("1114", "naics_2022", "dac_spec",  "domain_ag_crop"),

    # 1119 Other Crop Farming -> fiber, sugar, forage
    ("1119", "naics_2022", "dac_fiber",  "domain_ag_crop"),
    ("1119", "naics_2022", "dac_sugar",  "domain_ag_crop"),
    ("1119", "naics_2022", "dac_forage", "domain_ag_crop"),

    # 112 Animal Production -> livestock domain root categories
    ("112",  "naics_2022", "dal_cattle",  "domain_ag_livestock"),
    ("112",  "naics_2022", "dal_swine",   "domain_ag_livestock"),
    ("112",  "naics_2022", "dal_poultry", "domain_ag_livestock"),
    ("112",  "naics_2022", "dal_small",   "domain_ag_livestock"),
    ("112",  "naics_2022", "dal_equine",  "domain_ag_livestock"),
    ("112",  "naics_2022", "dal_aqua",    "domain_ag_livestock"),
    ("112",  "naics_2022", "dal_other",   "domain_ag_livestock"),

    # 1121 Cattle Ranching and Farming -> cattle category
    ("1121", "naics_2022", "dal_cattle", "domain_ag_livestock"),

    # 1122 Hog and Pig Farming -> swine category
    ("1122", "naics_2022", "dal_swine", "domain_ag_livestock"),

    # 1123 Poultry and Egg Production -> poultry + egg grades
    ("1123", "naics_2022", "dal_poultry", "domain_ag_livestock"),
    ("1123", "naics_2022", "dag_egg",     "domain_ag_grade"),

    # 1124 Sheep and Goat Farming -> small ruminants category
    ("1124", "naics_2022", "dal_small", "domain_ag_livestock"),

    # 1125 Aquaculture -> aqua category
    ("1125", "naics_2022", "dal_aqua", "domain_ag_livestock"),

    # 1129 Other Animal Production -> other livestock
    ("1129", "naics_2022", "dal_other", "domain_ag_livestock"),
]


async def ingest_crosswalk_naics11_domains(conn) -> int:
    """Ingest NAICS 11 -> Agriculture Domain Taxonomy crosswalk.

    Creates equivalence edges linking NAICS Agriculture sector codes
    to the four ag domain taxonomies (crop, livestock, method, grade).
    All edges use match_type='broad'.

    Returns count of edges inserted.
    """
    rows = [
        (
            naics_code,
            naics_sys,
            domain_code,
            domain_sys,
            "broad",
        )
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
