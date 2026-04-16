"""PRODCOM (PRODuction COMmunautaire) ingester.

Eurostat survey-based production statistics for EU industrial production.
PRODCOM classifies physical quantities produced by EU industry, aligned
to NACE sections B, C, D and E.

This ingester stores 4 NACE-aligned sections and 34 division-level codes.

Source: Eurostat PRODCOM regulation and annual list
License: Eurostat open data (freely reusable with attribution)
"""
from __future__ import annotations

PRODCOM_SECTIONS: list[tuple[str, str]] = [
    ("B", "Mining and Quarrying"),
    ("C", "Manufacturing"),
    ("D", "Electricity, Gas, Steam and Air Conditioning Supply"),
    ("E", "Water Supply, Sewerage, Waste Management"),
]

PRODCOM_DIVISIONS: list[tuple[str, str, str]] = [
    ("05", "Mining of Coal and Lignite", "B"),
    ("06", "Extraction of Crude Petroleum and Natural Gas", "B"),
    ("07", "Mining of Metal Ores", "B"),
    ("08", "Other Mining and Quarrying", "B"),
    ("09", "Mining Support Service Activities", "B"),
    ("10", "Manufacture of Food Products", "C"),
    ("11", "Manufacture of Beverages", "C"),
    ("12", "Manufacture of Tobacco Products", "C"),
    ("13", "Manufacture of Textiles", "C"),
    ("14", "Manufacture of Wearing Apparel", "C"),
    ("15", "Manufacture of Leather and Related Products", "C"),
    ("16", "Manufacture of Wood and Products of Wood and Cork", "C"),
    ("17", "Manufacture of Paper and Paper Products", "C"),
    ("18", "Printing and Reproduction of Recorded Media", "C"),
    ("19", "Manufacture of Coke and Refined Petroleum Products", "C"),
    ("20", "Manufacture of Chemicals and Chemical Products", "C"),
    ("21", "Manufacture of Basic Pharmaceutical Products and Preparations", "C"),
    ("22", "Manufacture of Rubber and Plastic Products", "C"),
    ("23", "Manufacture of Other Non-Metallic Mineral Products", "C"),
    ("24", "Manufacture of Basic Metals", "C"),
    ("25", "Manufacture of Fabricated Metal Products (except Machinery)", "C"),
    ("26", "Manufacture of Computer, Electronic and Optical Products", "C"),
    ("27", "Manufacture of Electrical Equipment", "C"),
    ("28", "Manufacture of Machinery and Equipment NEC", "C"),
    ("29", "Manufacture of Motor Vehicles, Trailers and Semi-Trailers", "C"),
    ("30", "Manufacture of Other Transport Equipment", "C"),
    ("31", "Manufacture of Furniture", "C"),
    ("32", "Other Manufacturing", "C"),
    ("33", "Repair and Installation of Machinery and Equipment", "C"),
    ("35", "Electricity, Gas, Steam and Air Conditioning Supply", "D"),
    ("36", "Water Collection, Treatment and Supply", "E"),
    ("37", "Sewerage", "E"),
    ("38", "Waste Collection, Treatment and Disposal Activities", "E"),
    ("39", "Remediation Activities and Other Waste Management", "E"),
]


async def ingest_prodcom(conn) -> int:
    """Ingest PRODCOM sections and divisions into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "prodcom",
        "PRODCOM",
        "PRODCOM EU Industrial Production Survey Classification",
        "2023",
        "European Union",
        "Eurostat",
    )

    count = 0
    for seq, (code, title) in enumerate(PRODCOM_SECTIONS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "prodcom", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(PRODCOM_DIVISIONS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "prodcom", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'prodcom'",
        count,
    )

    # Crosswalk PRODCOM divisions to NACE Rev 2 (same division codes)
    for div_code, _, _ in PRODCOM_DIVISIONS:
        nace_exists = await conn.fetchval(
            "SELECT 1 FROM classification_node WHERE system_id = 'nace_rev2' AND code LIKE $1",
            div_code + "%",
        )
        if nace_exists:
            await conn.execute(
                """INSERT INTO equivalence
                       (source_system, source_code, target_system, target_code, match_type, notes)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
                "prodcom", div_code, "nace_rev2", div_code,
                "broad", "PRODCOM division to NACE Rev 2 equivalent division",
            )

    return count
