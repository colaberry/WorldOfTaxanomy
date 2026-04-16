"""Industry Classification Benchmark (ICB) ingester.

ICB is an industry classification system developed by FTSE Russell. It is used
to classify companies and securities traded on global stock exchanges including
the London Stock Exchange, Euronext, and many others. It has 11 industries,
20 supersectors, 45 sectors, and 173 subsectors.
"""
from __future__ import annotations

SYSTEM_ID = "icb"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Industries (level 1)
    ("10", "Energy",                             1, None),
    ("15", "Basic Materials",                    1, None),
    ("20", "Industrials",                        1, None),
    ("30", "Consumer Discretionary",             1, None),
    ("35", "Consumer Staples",                   1, None),
    ("40", "Health Care",                        1, None),
    ("45", "Financials",                         1, None),
    ("50", "Technology",                         1, None),
    ("55", "Telecommunications",                 1, None),
    ("60", "Utilities",                          1, None),
    ("65", "Real Estate",                        1, None),
    # Supersectors (level 2) - key examples
    ("1010","Oil, Gas and Coal",                 2, "10"),
    ("1510","Chemicals",                         2, "15"),
    ("1520","Basic Resources",                   2, "15"),
    ("2010","Construction and Materials",        2, "20"),
    ("2020","Industrial Goods and Services",     2, "20"),
    ("3010","Automobiles and Parts",             2, "30"),
    ("3020","Consumer Products and Services",    2, "30"),
    ("3030","Media",                             2, "30"),
    ("3040","Retailers",                         2, "30"),
    ("3050","Travel and Leisure",                2, "30"),
    ("3510","Food, Beverage and Tobacco",        2, "35"),
    ("3520","Personal Care, Drug and Grocery",   2, "35"),
    ("4010","Health Care",                       2, "40"),
    ("4510","Banks",                             2, "45"),
    ("4520","Financial Services",                2, "45"),
    ("4530","Insurance",                         2, "45"),
    ("5010","Software and Computer Services",    2, "50"),
    ("5020","Technology Hardware and Equipment", 2, "50"),
    ("5510","Telecommunications",                2, "55"),
    ("6010","Utilities",                         2, "60"),
    ("6510","Real Estate",                       2, "65"),
]


async def ingest_icb(conn) -> int:
    """Ingest Industry Classification Benchmark (FTSE Russell)."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "ICB",
        "Industry Classification Benchmark",
        "Global",
        "2019",
        "FTSE Russell",
        "#7C3AED",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code[:2]
        is_leaf = code not in codes_with_children
        await conn.execute(
            """
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code,
                 sector_code, is_leaf, seq_order)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (system_id, code) DO UPDATE SET is_leaf = EXCLUDED.is_leaf
            """,
            SYSTEM_ID, code, title, level, parent_code, sector, is_leaf, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, SYSTEM_ID,
    )
    print(f"  Ingested {count} ICB codes")
    return count
