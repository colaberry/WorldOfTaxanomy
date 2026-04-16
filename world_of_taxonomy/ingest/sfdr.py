"""SFDR Sustainable Finance Disclosure Regulation ingester.

The EU Sustainable Finance Disclosure Regulation (EU 2019/2088) requires
financial market participants to classify financial products by sustainability
ambition. Article 6 = no sustainability focus, Article 8 = promotes ESG
characteristics, Article 9 = has sustainable investment as objective. Used by
asset managers, pension funds and retail funds distributed in the EU.
"""
from __future__ import annotations

SYSTEM_ID = "sfdr"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Article categories (level 1)
    ("SFDR-6",   "Article 6 - No Sustainability Focus",            1, None),
    ("SFDR-8",   "Article 8 - ESG Characteristics Promoted",       1, None),
    ("SFDR-9",   "Article 9 - Sustainable Investment Objective",   1, None),
    ("SFDR-PAI", "Principal Adverse Impacts (PAI)",                1, None),
    ("SFDR-RTS", "Regulatory Technical Standards Disclosures",     1, None),
    # Article 6 sub-types (level 2)
    ("SFDR-6-STD","Standard Article 6 - No sustainability claims", 2, "SFDR-6"),
    ("SFDR-6-EX", "Article 6 with ESG exclusions only",            2, "SFDR-6"),
    # Article 8 sub-types (level 2)
    ("SFDR-8-ENV","Article 8 - Environmental characteristics",     2, "SFDR-8"),
    ("SFDR-8-SOC","Article 8 - Social characteristics",            2, "SFDR-8"),
    ("SFDR-8-GOV","Article 8 - Governance characteristics",        2, "SFDR-8"),
    ("SFDR-8-TAX","Article 8 - EU Taxonomy-aligned portion",       2, "SFDR-8"),
    ("SFDR-8-SI", "Article 8 with partial sustainable investment",  2, "SFDR-8"),
    # Article 9 sub-types (level 2)
    ("SFDR-9-ENV","Article 9 - Environmental objective",           2, "SFDR-9"),
    ("SFDR-9-SOC","Article 9 - Social objective",                  2, "SFDR-9"),
    ("SFDR-9-RED","Article 9 - Carbon reduction objective",        2, "SFDR-9"),
    ("SFDR-9-TAX","Article 9 - EU Taxonomy-aligned (100%)",        2, "SFDR-9"),
    # PAI indicators (level 2)
    ("PAI-1",  "GHG Emissions (Scope 1, 2, 3)",                    2, "SFDR-PAI"),
    ("PAI-2",  "Carbon Footprint",                                  2, "SFDR-PAI"),
    ("PAI-3",  "GHG Intensity of Investee Companies",              2, "SFDR-PAI"),
    ("PAI-4",  "Fossil Fuel Sector Exposure",                      2, "SFDR-PAI"),
    ("PAI-5",  "Non-Renewable Energy Consumption",                 2, "SFDR-PAI"),
    ("PAI-6",  "Energy Consumption Intensity by High-Impact Sector",2,"SFDR-PAI"),
    ("PAI-7",  "Biodiversity-Sensitive Area Activities",           2, "SFDR-PAI"),
    ("PAI-8",  "Emissions to Water",                               2, "SFDR-PAI"),
    ("PAI-9",  "Hazardous Waste Ratio",                            2, "SFDR-PAI"),
    ("PAI-10", "UN Global Compact Violations",                     2, "SFDR-PAI"),
    ("PAI-11", "Lack of UNGC Compliance Processes",                2, "SFDR-PAI"),
    ("PAI-12", "Unexplained Gender Pay Gap",                       2, "SFDR-PAI"),
    ("PAI-13", "Board Gender Diversity",                           2, "SFDR-PAI"),
    ("PAI-14", "Controversial Weapons Exposure",                   2, "SFDR-PAI"),
]


async def ingest_sfdr(conn) -> int:
    """Ingest SFDR Sustainable Finance Disclosure Regulation taxonomy."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "SFDR",
        "EU Sustainable Finance Disclosure Regulation (EU 2019/2088)",
        "European Union",
        "2019/2088 + RTS 2023",
        "European Commission / ESMA",
        "#2563EB",
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
    print(f"  Ingested {count} SFDR codes")
    return count
