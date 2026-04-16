"""Export Control Classification Number (ECCN) skeleton ingester.

ECCNs are used in the US Export Administration Regulations (EAR) to classify
items subject to export controls. Each ECCN identifies items by their technical
parameters and determines whether an export licence is required. Administered
by the Bureau of Industry and Security (BIS) under the Commerce Control List
(CCL). This is a top-level skeleton of the 10 CCL categories.
"""
from __future__ import annotations

SYSTEM_ID = "eccn"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # CCL Categories (level 1)
    ("CAT0", "Nuclear and Miscellaneous",               1, None),
    ("CAT1", "Materials, Chemicals, Microorganisms, Toxins",1,None),
    ("CAT2", "Materials Processing",                    1, None),
    ("CAT3", "Electronics",                             1, None),
    ("CAT4", "Computers",                               1, None),
    ("CAT5", "Telecommunications and Information Security",1,None),
    ("CAT6", "Sensors and Lasers",                      1, None),
    ("CAT7", "Navigation and Avionics",                 1, None),
    ("CAT8", "Marine",                                  1, None),
    ("CAT9", "Aerospace and Propulsion",                1, None),
    # Product groups per category (level 2)
    ("0A", "Nuclear Test Equipment (Systems and Equipment)",   2, "CAT0"),
    ("0B", "Nuclear Test Equipment (Test/Inspection Equipment)",2,"CAT0"),
    ("0C", "Nuclear Materials (Materials)",                   2, "CAT0"),
    ("0D", "Nuclear Software",                                2, "CAT0"),
    ("0E", "Nuclear Technology",                              2, "CAT0"),
    ("1A", "Materials, Chemicals - Systems and Equipment",    2, "CAT1"),
    ("1B", "Materials, Chemicals - Test/Inspection Equipment",2,"CAT1"),
    ("1C", "Chemicals and Materials",                         2, "CAT1"),
    ("1D", "Materials, Chemicals - Software",                 2, "CAT1"),
    ("1E", "Materials, Chemicals - Technology",               2, "CAT1"),
    ("2A", "Materials Processing - Systems and Equipment",    2, "CAT2"),
    ("2B", "Materials Processing - Test/Inspection Equipment",2,"CAT2"),
    ("2D", "Materials Processing - Software",                 2, "CAT2"),
    ("2E", "Materials Processing - Technology",               2, "CAT2"),
    ("3A", "Electronics - Systems and Equipment",             2, "CAT3"),
    ("3B", "Electronics - Test/Inspection Equipment",         2, "CAT3"),
    ("3C", "Electronics - Materials",                         2, "CAT3"),
    ("3D", "Electronics - Software",                          2, "CAT3"),
    ("3E", "Electronics - Technology",                        2, "CAT3"),
    ("4A", "Computers - Systems and Equipment",               2, "CAT4"),
    ("4D", "Computers - Software",                            2, "CAT4"),
    ("4E", "Computers - Technology",                          2, "CAT4"),
    ("5A1","Telecommunications - Systems and Equipment",      2, "CAT5"),
    ("5A2","Information Security - Systems and Equipment",    2, "CAT5"),
    ("5D1","Telecommunications - Software",                   2, "CAT5"),
    ("5D2","Information Security - Software",                 2, "CAT5"),
    ("5E1","Telecommunications - Technology",                 2, "CAT5"),
    ("5E2","Information Security - Technology",               2, "CAT5"),
    ("6A", "Sensors and Lasers - Systems and Equipment",      2, "CAT6"),
    ("6B", "Sensors and Lasers - Test/Inspection Equipment",  2, "CAT6"),
    ("6C", "Sensors and Lasers - Materials",                  2, "CAT6"),
    ("6D", "Sensors and Lasers - Software",                   2, "CAT6"),
    ("6E", "Sensors and Lasers - Technology",                 2, "CAT6"),
    ("7A", "Navigation - Systems and Equipment",              2, "CAT7"),
    ("7B", "Navigation - Test/Inspection Equipment",          2, "CAT7"),
    ("7D", "Navigation - Software",                           2, "CAT7"),
    ("7E", "Navigation - Technology",                         2, "CAT7"),
    ("8A", "Marine - Systems and Equipment",                  2, "CAT8"),
    ("8B", "Marine - Test/Inspection Equipment",              2, "CAT8"),
    ("8C", "Marine - Materials",                              2, "CAT8"),
    ("8D", "Marine - Software",                               2, "CAT8"),
    ("8E", "Marine - Technology",                             2, "CAT8"),
    ("9A", "Aerospace - Systems and Equipment",               2, "CAT9"),
    ("9B", "Aerospace - Test/Inspection Equipment",           2, "CAT9"),
    ("9C", "Aerospace - Materials",                           2, "CAT9"),
    ("9D", "Aerospace - Software",                            2, "CAT9"),
    ("9E", "Aerospace - Technology",                          2, "CAT9"),
    ("EAR99","Not on Commerce Control List",                  2, "CAT0"),
]


async def ingest_eccn(conn) -> int:
    """Ingest Export Control Classification Number (ECCN) skeleton."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "ECCN",
        "Export Control Classification Number (Commerce Control List)",
        "United States",
        "EAR 2024",
        "Bureau of Industry and Security (BIS), US Dept of Commerce",
        "#1E40AF",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code[:3] if len(code) >= 3 else code
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
    print(f"  Ingested {count} ECCN codes")
    return count
