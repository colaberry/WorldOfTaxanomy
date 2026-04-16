"""GRI Standards ingester.

The Global Reporting Initiative (GRI) Standards are the most widely used
sustainability reporting standards globally. Used by over 10,000 organisations
in 100+ countries to report on environmental, social and governance (ESG)
impacts. GRI 1-3 are universal standards; GRI 200-400 are topic-specific.
"""
from __future__ import annotations

SYSTEM_ID = "gri_standards"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Universal Standards (level 1)
    ("GRI-U",    "Universal Standards",                            1, None),
    ("GRI-EC",   "Economic Topic Standards",                      1, None),
    ("GRI-ENV",  "Environmental Topic Standards",                  1, None),
    ("GRI-SOC",  "Social Topic Standards",                        1, None),
    # Universal (level 2)
    ("GRI-1",    "GRI 1: Foundation 2021",                        2, "GRI-U"),
    ("GRI-2",    "GRI 2: General Disclosures 2021",               2, "GRI-U"),
    ("GRI-3",    "GRI 3: Material Topics 2021",                   2, "GRI-U"),
    # Economic (level 2)
    ("GRI-201",  "GRI 201: Economic Performance",                 2, "GRI-EC"),
    ("GRI-202",  "GRI 202: Market Presence",                      2, "GRI-EC"),
    ("GRI-203",  "GRI 203: Indirect Economic Impacts",            2, "GRI-EC"),
    ("GRI-204",  "GRI 204: Procurement Practices",                2, "GRI-EC"),
    ("GRI-205",  "GRI 205: Anti-Corruption",                      2, "GRI-EC"),
    ("GRI-206",  "GRI 206: Anti-Competitive Behavior",            2, "GRI-EC"),
    ("GRI-207",  "GRI 207: Tax 2019",                             2, "GRI-EC"),
    # Environmental (level 2)
    ("GRI-301",  "GRI 301: Materials",                            2, "GRI-ENV"),
    ("GRI-302",  "GRI 302: Energy",                               2, "GRI-ENV"),
    ("GRI-303",  "GRI 303: Water and Effluents",                  2, "GRI-ENV"),
    ("GRI-304",  "GRI 304: Biodiversity",                         2, "GRI-ENV"),
    ("GRI-305",  "GRI 305: Emissions",                            2, "GRI-ENV"),
    ("GRI-306",  "GRI 306: Waste",                                2, "GRI-ENV"),
    ("GRI-308",  "GRI 308: Supplier Environmental Assessment",    2, "GRI-ENV"),
    # Social (level 2)
    ("GRI-401",  "GRI 401: Employment",                           2, "GRI-SOC"),
    ("GRI-402",  "GRI 402: Labor/Management Relations",           2, "GRI-SOC"),
    ("GRI-403",  "GRI 403: Occupational Health and Safety",       2, "GRI-SOC"),
    ("GRI-404",  "GRI 404: Training and Education",               2, "GRI-SOC"),
    ("GRI-405",  "GRI 405: Diversity and Equal Opportunity",      2, "GRI-SOC"),
    ("GRI-406",  "GRI 406: Non-Discrimination",                   2, "GRI-SOC"),
    ("GRI-407",  "GRI 407: Freedom of Association and Collective Bargaining",2,"GRI-SOC"),
    ("GRI-408",  "GRI 408: Child Labor",                          2, "GRI-SOC"),
    ("GRI-409",  "GRI 409: Forced or Compulsory Labor",           2, "GRI-SOC"),
    ("GRI-410",  "GRI 410: Security Practices",                   2, "GRI-SOC"),
    ("GRI-411",  "GRI 411: Rights of Indigenous Peoples",         2, "GRI-SOC"),
    ("GRI-413",  "GRI 413: Local Communities",                    2, "GRI-SOC"),
    ("GRI-414",  "GRI 414: Supplier Social Assessment",           2, "GRI-SOC"),
    ("GRI-415",  "GRI 415: Public Policy",                        2, "GRI-SOC"),
    ("GRI-416",  "GRI 416: Customer Health and Safety",           2, "GRI-SOC"),
    ("GRI-417",  "GRI 417: Marketing and Labeling",               2, "GRI-SOC"),
    ("GRI-418",  "GRI 418: Customer Privacy",                     2, "GRI-SOC"),
]


async def ingest_gri_standards(conn) -> int:
    """Ingest GRI Standards taxonomy."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "GRI Standards",
        "Global Reporting Initiative Standards",
        "Global",
        "2021",
        "Global Reporting Initiative (GRI)",
        "#059669",
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
    print(f"  Ingested {count} GRI Standard codes")
    return count
