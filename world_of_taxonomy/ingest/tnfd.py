"""TNFD Nature-related Financial Disclosures framework ingester.

The Taskforce on Nature-related Financial Disclosures (TNFD) framework helps
organisations assess, manage and disclose nature-related dependencies, impacts,
risks and opportunities (DIROs). Released in 2023, it is used by financial
institutions and corporates to align with the Kunming-Montreal Global
Biodiversity Framework (GBF).
"""
from __future__ import annotations

SYSTEM_ID = "tnfd"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Pillars (level 1)
    ("TNFD-GOV",  "Governance",                                      1, None),
    ("TNFD-STR",  "Strategy",                                        1, None),
    ("TNFD-RM",   "Risk and Impact Management",                      1, None),
    ("TNFD-MET",  "Metrics and Targets",                             1, None),
    ("TNFD-REALM","Realms of Nature",                                1, None),
    ("TNFD-BIOME","Biomes",                                          1, None),
    ("TNFD-DIRO", "Dependencies, Impacts, Risks and Opportunities",  1, None),
    # Governance disclosures (level 2)
    ("TNFD-GOV-A","Board oversight of nature-related issues",        2, "TNFD-GOV"),
    ("TNFD-GOV-B","Management role in assessing nature risks",       2, "TNFD-GOV"),
    # Strategy disclosures (level 2)
    ("TNFD-STR-A","Nature-related dependencies and impacts",         2, "TNFD-STR"),
    ("TNFD-STR-B","Nature-related risks and opportunities",          2, "TNFD-STR"),
    ("TNFD-STR-C","Resilience of strategy to nature scenarios",      2, "TNFD-STR"),
    ("TNFD-STR-D","Priority locations with nature interfaces",       2, "TNFD-STR"),
    # Risk and Impact Management (level 2)
    ("TNFD-RM-A", "Identifying nature-related issues",               2, "TNFD-RM"),
    ("TNFD-RM-B", "Managing nature-related risks",                   2, "TNFD-RM"),
    ("TNFD-RM-C", "Integration into overall risk management",        2, "TNFD-RM"),
    # Metrics and Targets (level 2)
    ("TNFD-MET-A","Metrics for nature-related risks and opps",       2, "TNFD-MET"),
    ("TNFD-MET-B","Targets to manage nature-related issues",         2, "TNFD-MET"),
    ("TNFD-MET-C","Performance against targets",                     2, "TNFD-MET"),
    # Realms (level 2)
    ("REALM-LND", "Land",                                            2, "TNFD-REALM"),
    ("REALM-OC",  "Ocean",                                           2, "TNFD-REALM"),
    ("REALM-FW",  "Freshwater",                                      2, "TNFD-REALM"),
    ("REALM-ATM", "Atmosphere",                                      2, "TNFD-REALM"),
    # Key biomes (level 2)
    ("BIOME-TRF", "Tropical and Subtropical Forests",                2, "TNFD-BIOME"),
    ("BIOME-TMP", "Temperate and Boreal Forests",                    2, "TNFD-BIOME"),
    ("BIOME-GRS", "Grasslands, Savannas and Shrublands",             2, "TNFD-BIOME"),
    ("BIOME-WET", "Wetlands",                                        2, "TNFD-BIOME"),
    ("BIOME-CRL", "Coral Reefs",                                     2, "TNFD-BIOME"),
    ("BIOME-MCN", "Marine and Coastal Non-Reef",                     2, "TNFD-BIOME"),
    ("BIOME-FRW", "Rivers, Lakes and Freshwater Ecosystems",         2, "TNFD-BIOME"),
    # DIRO types (level 2)
    ("DIRO-DEP",  "Dependencies on Nature",                          2, "TNFD-DIRO"),
    ("DIRO-IMP",  "Impacts on Nature",                               2, "TNFD-DIRO"),
    ("DIRO-RSK",  "Nature-related Risks",                            2, "TNFD-DIRO"),
    ("DIRO-OPP",  "Nature-related Opportunities",                    2, "TNFD-DIRO"),
]


async def ingest_tnfd(conn) -> int:
    """Ingest TNFD framework taxonomy."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "TNFD",
        "Taskforce on Nature-related Financial Disclosures Framework",
        "Global",
        "v1.0 (2023)",
        "Taskforce on Nature-related Financial Disclosures (TNFD)",
        "#16A34A",
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
    print(f"  Ingested {count} TNFD codes")
    return count
