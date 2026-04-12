"""GHG Protocol ingester.

Greenhouse Gas Protocol Corporate Accounting and Reporting Standard.
Source: hand-coded (World Resources Institute / WBCSD)
License: open (WRI/WBCSD open standard)
Reference: https://ghgprotocol.org/corporate-standard

Hierarchy (2 levels):
  Scope      level=1  e.g. "scope_1"          3 nodes
  Category   level=2  e.g. "scope_3_cat_1"   17 nodes (2 + 15)

Total: 20 nodes.

Scope 1: Direct emissions (combustion, process, fugitive, owned transport)
Scope 2: Indirect emissions from purchased energy
Scope 3: All other indirect emissions - 15 standard categories
"""
from __future__ import annotations

# (code, title, level, parent_code)
# Scopes are level 1 with no parent.
# Categories are level 2 with parent = their scope.
GHG_NODES: list[tuple[str, str, int, str | None]] = [
    # Scopes
    ("scope_1", "Scope 1 - Direct Emissions", 1, None),
    ("scope_2", "Scope 2 - Indirect Emissions from Purchased Energy", 1, None),
    ("scope_3", "Scope 3 - All Other Indirect Emissions", 1, None),
    # Scope 1 categories
    ("scope_1_stat", "Stationary Combustion", 2, "scope_1"),
    ("scope_1_mobile", "Mobile Combustion and Transportation", 2, "scope_1"),
    # Scope 3 - 15 standard categories (GHG Protocol Technical Guidance)
    ("scope_3_cat_1",  "Cat 1 - Purchased Goods and Services", 2, "scope_3"),
    ("scope_3_cat_2",  "Cat 2 - Capital Goods", 2, "scope_3"),
    ("scope_3_cat_3",  "Cat 3 - Fuel and Energy-Related Activities", 2, "scope_3"),
    ("scope_3_cat_4",  "Cat 4 - Upstream Transportation and Distribution", 2, "scope_3"),
    ("scope_3_cat_5",  "Cat 5 - Waste Generated in Operations", 2, "scope_3"),
    ("scope_3_cat_6",  "Cat 6 - Business Travel", 2, "scope_3"),
    ("scope_3_cat_7",  "Cat 7 - Employee Commuting", 2, "scope_3"),
    ("scope_3_cat_8",  "Cat 8 - Upstream Leased Assets", 2, "scope_3"),
    ("scope_3_cat_9",  "Cat 9 - Downstream Transportation and Distribution", 2, "scope_3"),
    ("scope_3_cat_10", "Cat 10 - Processing of Sold Products", 2, "scope_3"),
    ("scope_3_cat_11", "Cat 11 - Use of Sold Products", 2, "scope_3"),
    ("scope_3_cat_12", "Cat 12 - End-of-Life Treatment of Sold Products", 2, "scope_3"),
    ("scope_3_cat_13", "Cat 13 - Downstream Leased Assets", 2, "scope_3"),
    ("scope_3_cat_14", "Cat 14 - Franchises", 2, "scope_3"),
    ("scope_3_cat_15", "Cat 15 - Investments", 2, "scope_3"),
]

_SYSTEM_ROW = (
    "ghg_protocol",
    "GHG Protocol",
    "Greenhouse Gas Protocol Corporate Standard - Scope and Category Framework",
    "2015",
    "Global",
    "World Resources Institute / WBCSD",
)


async def ingest_ghg_protocol(conn) -> int:
    """Ingest GHG Protocol scope/category framework.

    Hand-coded from WRI/WBCSD GHG Protocol Corporate Standard.
    Returns 20 (always).
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        *_SYSTEM_ROW,
    )

    # Scopes are not leaves (they have children); categories are leaves
    scope_codes = {code for code, _, level, _ in GHG_NODES if level == 1}

    rows = [
        (
            "ghg_protocol",
            code,
            title,
            level,
            parent,
            code.split("_cat_")[0] if "_cat_" in code else code,  # sector = owning scope
            code not in scope_codes,  # is_leaf
        )
        for code, title, level, parent in GHG_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(GHG_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'ghg_protocol'",
        count,
    )

    return count
