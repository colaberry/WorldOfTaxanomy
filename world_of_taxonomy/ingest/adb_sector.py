"""Asian Development Bank (ADB) Sector Classification ingester.

ADB uses sector and subsector codes to classify all loans, grants, technical
assistance and equity investments. These are used in ADB's project database
and annual report to track development impact across Asia and the Pacific.
"""
from __future__ import annotations

SYSTEM_ID = "adb_sector"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Sectors (level 1)
    ("AG",    "Agriculture, Natural Resources and Rural Development", 1, None),
    ("ED",    "Education",                                            1, None),
    ("EN",    "Energy",                                               1, None),
    ("FI",    "Finance",                                              1, None),
    ("HE",    "Health",                                               1, None),
    ("IC",    "Industry and Trade",                                   1, None),
    ("PS",    "Public Sector Management",                             1, None),
    ("TN",    "Transport",                                            1, None),
    ("WS",    "Water and Other Urban Infrastructure and Services",    1, None),
    ("MF",    "Multisector",                                          1, None),
    ("IC2",   "Information and Communication Technology",             1, None),
    # Subsectors (level 2) - key examples
    ("AG-AG", "Agricultural Production",                              2, "AG"),
    ("AG-IR", "Irrigation",                                           2, "AG"),
    ("AG-NR", "Natural Resources",                                    2, "AG"),
    ("AG-RD", "Rural Development",                                    2, "AG"),
    ("ED-GE", "General Education",                                    2, "ED"),
    ("ED-TE", "Technical and Vocational Education and Training",      2, "ED"),
    ("ED-HE", "Higher Education",                                     2, "ED"),
    ("EN-CG", "Conventional Energy Generation",                       2, "EN"),
    ("EN-RE", "Renewable Energy Generation",                          2, "EN"),
    ("EN-TR", "Energy Transmission and Distribution",                 2, "EN"),
    ("EN-EE", "Energy Efficiency and Conservation",                   2, "EN"),
    ("FI-BC", "Banking Systems and Non-Bank Financial Intermediaries",2,"FI"),
    ("FI-CF", "Capital Markets",                                      2, "FI"),
    ("FI-MF", "Microfinance",                                         2, "FI"),
    ("FI-IF", "Infrastructure Finance and Investment Funds",          2, "FI"),
    ("HE-PH", "Disease Control of Communicable and Non-Communicable Diseases",2,"HE"),
    ("HE-HF", "Health Financing",                                     2, "HE"),
    ("HE-HS", "Health System Development",                            2, "HE"),
    ("IC-AG", "Agribusiness and Food Security",                       2, "IC"),
    ("IC-IN", "Industry",                                             2, "IC"),
    ("IC-TR", "Trade and Investment",                                 2, "IC"),
    ("PS-PF", "Public Finance",                                       2, "PS"),
    ("PS-PA", "Public Administration",                                2, "PS"),
    ("PS-DG", "Decentralization and Local Governance",               2, "PS"),
    ("TN-RD", "Road Transport",                                       2, "TN"),
    ("TN-UR", "Urban Public Transport",                               2, "TN"),
    ("TN-RA", "Rail Transport",                                       2, "TN"),
    ("TN-AI", "Air Transport",                                        2, "TN"),
    ("TN-PT", "Ports and Waterways",                                  2, "TN"),
    ("WS-WS", "Urban Water Supply",                                   2, "WS"),
    ("WS-SN", "Urban Sanitation",                                     2, "WS"),
    ("WS-UD", "Urban Development",                                    2, "WS"),
    ("WS-SO", "Solid Waste Management",                               2, "WS"),
    ("IC2-IT","Information Technology",                               2, "IC2"),
    ("IC2-CT","Communication Technology",                             2, "IC2"),
]


async def ingest_adb_sector(conn) -> int:
    """Ingest Asian Development Bank Sector Classification."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "ADB Sector",
        "Asian Development Bank Sector and Subsector Classification",
        "Asia-Pacific",
        "2023",
        "Asian Development Bank (ADB)",
        "#DC2626",
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
    print(f"  Ingested {count} ADB sector codes")
    return count
