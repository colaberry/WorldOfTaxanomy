"""OECD DAC Sector Purpose Codes ingester.

The Development Assistance Committee (DAC) of the OECD uses purpose codes to
classify official development assistance (ODA) by sector. These are used in
the Creditor Reporting System (CRS) by all donor countries. Used by
development banks, aid agencies, NGOs, and impact investors to tag projects.
"""
from __future__ import annotations

SYSTEM_ID = "oecd_dac"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Sectors (level 1)
    ("100", "Social Infrastructure and Services",         1, None),
    ("200", "Economic Infrastructure and Services",       1, None),
    ("300", "Production Sectors",                         1, None),
    ("400", "Multi-Sector / Cross-Cutting",               1, None),
    ("500", "Commodity Aid / General Programme Assistance",1,None),
    ("601", "Action Relating to Debt",                    1, None),
    ("700", "Humanitarian Aid",                           1, None),
    ("900", "Unallocated / Unspecified",                  1, None),
    # Sub-sectors (level 2)
    ("110", "Education",                                  2, "100"),
    ("111", "Education, Level Unspecified",               2, "100"),
    ("112", "Basic Education",                            2, "100"),
    ("113", "Secondary Education",                        2, "100"),
    ("114", "Post-Secondary Education",                   2, "100"),
    ("115", "Early Childhood Education",                  2, "100"),
    ("116", "Education Policy and Administrative Management",2,"100"),
    ("120", "Health",                                     2, "100"),
    ("121", "Health, General",                            2, "100"),
    ("122", "Basic Health",                               2, "100"),
    ("123", "Non-Communicable Diseases (NCDs)",           2, "100"),
    ("130", "Population Policies",                        2, "100"),
    ("131", "Population Policy and Administration",       2, "100"),
    ("140", "Water Supply and Sanitation",                2, "100"),
    ("141", "Water Supply and Sanitation - Large Systems",2, "100"),
    ("142", "Basic Drinking Water Supply and Sanitation", 2, "100"),
    ("143", "River Development",                          2, "100"),
    ("150", "Government and Civil Society",               2, "100"),
    ("151", "Government and Civil Society - General",     2, "100"),
    ("152", "Conflict, Peace and Security",               2, "100"),
    ("160", "Other Social Infrastructure and Services",   2, "100"),
    ("210", "Transport and Storage",                      2, "200"),
    ("220", "Communications",                             2, "200"),
    ("230", "Energy",                                     2, "200"),
    ("231", "Energy Policy",                              2, "200"),
    ("232", "Energy Generation, Renewable Sources",       2, "200"),
    ("233", "Energy Generation, Non-Renewable Sources",   2, "200"),
    ("240", "Banking and Financial Services",             2, "200"),
    ("250", "Business and Other Services",                2, "200"),
    ("310", "Agriculture, Forestry and Fishing",          2, "300"),
    ("311", "Agriculture",                                2, "300"),
    ("312", "Forestry",                                   2, "300"),
    ("313", "Fishing",                                    2, "300"),
    ("320", "Industry, Mining, Construction",             2, "300"),
    ("321", "Industry",                                   2, "300"),
    ("322", "Mineral Resources and Mining",               2, "300"),
    ("323", "Construction",                               2, "300"),
    ("330", "Trade Policies and Regulations",             2, "300"),
    ("331", "Trade Policies and Regulations",             2, "300"),
    ("332", "Tourism",                                    2, "300"),
    ("410", "General Environment Protection",             2, "400"),
    ("430", "Other Multi-Sector",                         2, "400"),
    ("431", "Urban Development and Management",           2, "400"),
    ("432", "Rural Development",                          2, "400"),
    ("510", "General Budget Support",                     2, "500"),
    ("520", "Development Food Assistance",                2, "500"),
    ("530", "Other Commodity Assistance",                 2, "500"),
    ("600", "Action Relating to Debt (Bilateral)",         2, "601"),
    ("720", "Emergency Response",                         2, "700"),
    ("730", "Reconstruction Relief",                      2, "700"),
    ("740", "Disaster Prevention and Preparedness",       2, "700"),
    ("910", "Administrative Costs of Donors",             2, "900"),
    ("920", "Support to NGOs",                            2, "900"),
    ("998", "Unallocated / Unspecified",                  2, "900"),
]


async def ingest_oecd_dac(conn) -> int:
    """Ingest OECD DAC Sector Purpose Codes."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "OECD DAC",
        "OECD DAC Sector Purpose Codes (CRS)",
        "Global (OECD)",
        "2023",
        "OECD Development Assistance Committee",
        "#6366F1",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code[0] + "00"
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
    print(f"  Ingested {count} OECD DAC codes")
    return count
