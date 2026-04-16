"""UN System of Environmental-Economic Accounting (SEEA) ingester.

The SEEA is the international statistical standard for integrating
economic and environmental statistics. Adopted by the UN Statistical
Commission in 2012 (SEEA Central Framework) and extended with Ecosystem
Accounts (SEEA EA) adopted in 2021.
Used by national statistical offices for natural capital accounts,
environmental tax statistics, and green GDP computation.
Key components: SEEA-CF (Central Framework) covers physical and monetary
flow accounts, asset accounts; SEEA-EA covers ecosystem extent,
condition, and service flow accounts.
"""
from __future__ import annotations

SYSTEM_ID = "seea"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Main frameworks (level 1)
    ("CF",  "SEEA Central Framework (SEEA-CF)",                1, None),
    ("EA",  "SEEA Ecosystem Accounts (SEEA-EA)",               1, None),
    ("W",   "SEEA Water (SEEA-Water)",                         1, None),
    ("E",   "SEEA Energy (SEEA-Energy)",                       1, None),
    ("A",   "SEEA Agriculture, Forestry and Fisheries (SEEA-AFF)", 1, None),
    # SEEA-CF accounts (level 2)
    ("CF1", "Physical Flow Accounts",                          2, "CF"),
    ("CF2", "Monetary Flow Accounts",                          2, "CF"),
    ("CF3", "Environmental Activity Accounts",                 2, "CF"),
    ("CF4", "Asset Accounts",                                  2, "CF"),
    ("CF5", "Cross-Cutting and Other Accounts",                2, "CF"),
    # Physical Flow sub-accounts
    ("CF1.1", "Physical Supply and Use Tables (PSUT)",         2, "CF"),
    ("CF1.2", "Material Flow Accounts (MFA)",                  2, "CF"),
    ("CF1.3", "Energy Flow Accounts",                          2, "CF"),
    ("CF1.4", "Water Flow Accounts",                           2, "CF"),
    ("CF1.5", "Air Emission Accounts",                         2, "CF"),
    ("CF1.6", "Solid Waste Accounts",                          2, "CF"),
    # Monetary Flow sub-accounts
    ("CF2.1", "Environmental Protection Expenditure Accounts (EPEA)", 2, "CF"),
    ("CF2.2", "Environmental Goods and Services Sector (EGSS)", 2, "CF"),
    ("CF2.3", "Environmental Taxes and Subsidies",             2, "CF"),
    # Asset Accounts sub-categories
    ("CF4.1", "Mineral and Energy Resource Assets",            2, "CF"),
    ("CF4.2", "Land Assets",                                   2, "CF"),
    ("CF4.3", "Soil Resource Assets",                          2, "CF"),
    ("CF4.4", "Timber Resource Assets",                        2, "CF"),
    ("CF4.5", "Aquatic Resource Assets",                       2, "CF"),
    ("CF4.6", "Other Biological Resource Assets",              2, "CF"),
    ("CF4.7", "Water Resource Assets",                         2, "CF"),
    # SEEA-EA accounts (level 2)
    ("EA1",  "Ecosystem Extent Account",                       2, "EA"),
    ("EA2",  "Ecosystem Condition Account",                    2, "EA"),
    ("EA3",  "Ecosystem Services - Provisioning Services",     2, "EA"),
    ("EA4",  "Ecosystem Services - Regulating and Maintenance Services", 2, "EA"),
    ("EA5",  "Ecosystem Services - Cultural Services",         2, "EA"),
    ("EA6",  "Monetary Ecosystem Services Accounts",           2, "EA"),
    ("EA7",  "Ecosystem Asset Account - Monetary",             2, "EA"),
    # SEEA-Water accounts (level 2)
    ("W1",  "Water Resource Accounts in Physical Units",       2, "W"),
    ("W2",  "Water Use Accounts in Physical Units",            2, "W"),
    ("W3",  "Water Quality Accounts",                          2, "W"),
    ("W4",  "Water Monetary Accounts",                         2, "W"),
    # SEEA-Energy accounts (level 2)
    ("E1",  "Energy Supply Accounts",                          2, "E"),
    ("E2",  "Energy Use Accounts",                             2, "E"),
    ("E3",  "Energy Asset Accounts",                           2, "E"),
    ("E4",  "Greenhouse Gas Emission Accounts",                2, "E"),
    # SEEA-AFF accounts (level 2)
    ("A1",  "Crop Production Accounts",                        2, "A"),
    ("A2",  "Livestock Production Accounts",                   2, "A"),
    ("A3",  "Forestry Accounts",                               2, "A"),
    ("A4",  "Fisheries and Aquaculture Accounts",              2, "A"),
    ("A5",  "Land Use in Agriculture Accounts",                2, "A"),
    ("A6",  "Soil and Nutrient Accounts",                      2, "A"),
]


async def ingest_seea(conn) -> int:
    """Ingest UN System of Environmental-Economic Accounting (SEEA) framework."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "SEEA",
        "System of Environmental-Economic Accounting (SEEA) - UN Statistical Commission",
        "Global (UN)",
        "2012/2021",
        "United Nations Statistics Division (UNSD)",
        "#065F46",
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
    print(f"  Ingested {count} SEEA codes")
    return count
