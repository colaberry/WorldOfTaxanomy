"""Agriculture Commodity Grade domain taxonomy ingester.

Commodity grade taxonomy organizes USDA quality grades into categories:
  Grain Grades    (dag_grain*)  - US No. 1-5 + sample grade for grains
  Livestock Grades (dag_live*)  - USDA prime/choice/select + yield grades
  Produce Grades  (dag_prod*)   - US Fancy, Extra No.1, No.1, No.2, No.3
  Dairy Grades    (dag_dairy*)  - Grade A, Grade B, Grade AA butter/cheese
  Egg Grades      (dag_egg*)    - Grade AA, Grade A, Grade B

Source: USDA Agricultural Marketing Service (AMS) grading standards. Public domain.
Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
GRADE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Grain Grades category --
    ("dag_grain",           "Grain Grades",                                    1, None),
    ("dag_grain_1",         "US No. 1 (highest quality grain)",               2, "dag_grain"),
    ("dag_grain_2",         "US No. 2",                                        2, "dag_grain"),
    ("dag_grain_3",         "US No. 3",                                        2, "dag_grain"),
    ("dag_grain_4",         "US No. 4",                                        2, "dag_grain"),
    ("dag_grain_5",         "US No. 5",                                        2, "dag_grain"),
    ("dag_grain_sample",    "Sample Grade (does not meet No.5 requirements)", 2, "dag_grain"),

    # -- Livestock Grades category --
    ("dag_live",            "Livestock and Meat Quality Grades",               1, None),
    ("dag_live_prime",      "USDA Prime (highest marbling, top 2%)",          2, "dag_live"),
    ("dag_live_choice",     "USDA Choice",                                     2, "dag_live"),
    ("dag_live_select",     "USDA Select",                                     2, "dag_live"),
    ("dag_live_standard",   "USDA Standard",                                   2, "dag_live"),
    ("dag_live_yield1",     "Yield Grade 1 (most lean, least fat)",           2, "dag_live"),
    ("dag_live_yield2",     "Yield Grade 2",                                   2, "dag_live"),
    ("dag_live_yield3",     "Yield Grade 3",                                   2, "dag_live"),

    # -- Produce Grades category --
    ("dag_prod",            "Produce and Fresh Market Grades",                 1, None),
    ("dag_prod_fancy",      "US Fancy (top quality fresh produce)",           2, "dag_prod"),
    ("dag_prod_extra1",     "US Extra No. 1",                                  2, "dag_prod"),
    ("dag_prod_1",          "US No. 1",                                        2, "dag_prod"),
    ("dag_prod_2",          "US No. 2",                                        2, "dag_prod"),
    ("dag_prod_3",          "US No. 3 (lowest marketable grade)",             2, "dag_prod"),
    ("dag_prod_combo",      "US Combination (mixed grade lots)",               2, "dag_prod"),

    # -- Dairy Grades category --
    ("dag_dairy",           "Dairy Product Grades",                            1, None),
    ("dag_dairy_a",         "Grade A (fluid milk standard, pasteurized)",     2, "dag_dairy"),
    ("dag_dairy_b",         "Grade B (manufacturing grade milk)",             2, "dag_dairy"),
    ("dag_dairy_aa",        "Grade AA Butter (highest USDA butter grade)",    2, "dag_dairy"),
    ("dag_dairy_a_butter",  "Grade A Butter",                                  2, "dag_dairy"),
    ("dag_dairy_b_butter",  "Grade B Butter",                                  2, "dag_dairy"),

    # -- Egg Grades category --
    ("dag_egg",             "Egg Grades",                                       1, None),
    ("dag_egg_aa",          "Grade AA (highest quality, firm yolk)",          2, "dag_egg"),
    ("dag_egg_a",           "Grade A (standard retail quality)",              2, "dag_egg"),
    ("dag_egg_b",           "Grade B (breaking/processing use)",              2, "dag_egg"),
]

_DOMAIN_ROW = (
    "domain_ag_grade",
    "Agricultural Commodity Grades",
    "USDA grain, livestock, produce, dairy and egg quality grade taxonomy",
    "WorldOfTaxonomy",
    None,  # url
)

# NAICS prefixes to link (11xxx = Agriculture broadly)
_NAICS_PREFIXES = ["11"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific grades."""
    parts = code.split("_")
    # 'dag_grain'   -> ['dag', 'grain']        -> level 1
    # 'dag_grain_1' -> ['dag', 'grain', '1']   -> level 2
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_ag_grade(conn) -> int:
    """Ingest Agricultural Commodity Grade domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_ag_grade'), and links NAICS 11xxx nodes
    via node_taxonomy_link.

    Returns total grade node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_ag_grade",
        "Agricultural Commodity Grades",
        "USDA grain, livestock, produce, dairy and egg quality grade taxonomy",
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

    parent_codes = {parent for _, _, _, parent in GRADE_NODES if parent is not None}

    rows = [
        (
            "domain_ag_grade",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in GRADE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(GRADE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_ag_grade'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_ag_grade'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '11%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_ag_grade", "primary") for code in naics_codes],
    )

    return count
