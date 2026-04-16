"""World Bank Country Income Classification ingester.

The World Bank classifies economies into four income groups based on GNI per
capita (Atlas method). These classifications are used by development banks,
donors, investment funds, and trade agreements to determine eligibility,
concessional terms, and reporting requirements.
"""
from __future__ import annotations

SYSTEM_ID = "wb_income"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Groups (level 1)
    ("LIC",  "Low Income",               1, None),
    ("LMIC", "Lower-Middle Income",      1, None),
    ("UMIC", "Upper-Middle Income",      1, None),
    ("HIC",  "High Income",              1, None),
    # Sub-groups (level 2)
    ("LIC-SSA",   "Low Income: Sub-Saharan Africa",         2, "LIC"),
    ("LIC-SA",    "Low Income: South Asia",                 2, "LIC"),
    ("LIC-ECA",   "Low Income: Europe and Central Asia",    2, "LIC"),
    ("LIC-EAP",   "Low Income: East Asia and Pacific",      2, "LIC"),
    ("LMIC-SSA",  "Lower-Middle: Sub-Saharan Africa",       2, "LMIC"),
    ("LMIC-SA",   "Lower-Middle: South Asia",               2, "LMIC"),
    ("LMIC-MENA", "Lower-Middle: Middle East and N. Africa",2, "LMIC"),
    ("LMIC-EAP",  "Lower-Middle: East Asia and Pacific",    2, "LMIC"),
    ("LMIC-LAC",  "Lower-Middle: Latin America and Caribbean",2,"LMIC"),
    ("UMIC-LAC",  "Upper-Middle: Latin America and Caribbean",2,"UMIC"),
    ("UMIC-ECA",  "Upper-Middle: Europe and Central Asia",  2, "UMIC"),
    ("UMIC-EAP",  "Upper-Middle: East Asia and Pacific",    2, "UMIC"),
    ("UMIC-MENA", "Upper-Middle: Middle East and N. Africa",2, "UMIC"),
    ("HIC-OECD",  "High Income: OECD Members",              2, "HIC"),
    ("HIC-NOEC",  "High Income: Non-OECD",                  2, "HIC"),
    ("HIC-GCC",   "High Income: Gulf Cooperation Council",  2, "HIC"),
    # Special classifications
    ("IDA",  "IDA Eligible",             1, None),
    ("IBRD", "IBRD Eligible",            1, None),
    ("IDA-ONLY",  "IDA-Only Countries",            2, "IDA"),
    ("IDA-BLEND", "IDA-IBRD Blend Countries",      2, "IDA"),
    ("IDA-FCS",   "Fragile and Conflict-Affected", 2, "IDA"),
    ("IBRD-ONLY", "IBRD-Only Borrowers",           2, "IBRD"),
    ("IBRD-GRAD", "Graduated from World Bank",     2, "IBRD"),
]


async def ingest_wb_income(conn) -> int:
    """Ingest World Bank Country Income Classification."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "WB Income Groups",
        "World Bank Country Income Classification",
        "Global",
        "FY2025",
        "World Bank Group",
        "#F59E0B",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = parent_code if parent_code else code
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
    print(f"  Ingested {count} World Bank income codes")
    return count
