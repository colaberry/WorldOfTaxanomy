"""EU Taxonomy for Sustainable Finance ingester.

EU Taxonomy Regulation (EU) 2020/852 - European Commission.
Classifies economic activities as environmentally sustainable across
6 environmental objectives. Used for green finance disclosure (SFDR, CSRD).

Source: EUR-Lex, EU Taxonomy Regulation and Delegated Acts (public law)
License: European Union open data, reuse permitted (decision 2011/833/EU)
"""
from __future__ import annotations

EU_TAX_OBJECTIVES: list[tuple[str, str]] = [
    ("OBJ1", "Climate Change Mitigation"),
    ("OBJ2", "Climate Change Adaptation"),
    ("OBJ3", "Sustainable Use and Protection of Water and Marine Resources"),
    ("OBJ4", "Transition to a Circular Economy"),
    ("OBJ5", "Pollution Prevention and Control"),
    ("OBJ6", "Protection and Restoration of Biodiversity and Ecosystems"),
]

EU_TAX_ACTIVITIES: list[tuple[str, str, str]] = [
    ("OBJ1-ENE1", "Electricity generation from solar photovoltaic technology", "OBJ1"),
    ("OBJ1-ENE2", "Electricity generation from wind power", "OBJ1"),
    ("OBJ1-ENE3", "Electricity generation from hydropower", "OBJ1"),
    ("OBJ1-ENE4", "Electricity generation from geothermal energy", "OBJ1"),
    ("OBJ1-ENE5", "Electricity generation from ocean energy technologies", "OBJ1"),
    ("OBJ1-ENE6", "Electricity generation from bioenergy", "OBJ1"),
    ("OBJ1-ENE7", "Transmission and distribution of electricity", "OBJ1"),
    ("OBJ1-ENE8", "Storage of electricity", "OBJ1"),
    ("OBJ1-ENE9", "Production of heat and cool from solar thermal energy", "OBJ1"),
    ("OBJ1-ENE10", "Production of heat and cool from geothermal energy", "OBJ1"),
    ("OBJ1-ENE11", "Production of heat and cool from bioenergy", "OBJ1"),
    ("OBJ1-ENE12", "Production of heat and cool from heat pumps", "OBJ1"),
    ("OBJ1-ENE13", "District heating and cooling distribution", "OBJ1"),
    ("OBJ1-TRN1", "Passenger rail transport (inter-urban)", "OBJ1"),
    ("OBJ1-TRN2", "Urban and suburban transport and road passenger transport", "OBJ1"),
    ("OBJ1-TRN3", "Freight rail transport", "OBJ1"),
    ("OBJ1-TRN4", "Road freight transport and road passenger transport services", "OBJ1"),
    ("OBJ1-TRN5", "Inland passenger water transport", "OBJ1"),
    ("OBJ1-TRN6", "Inland freight water transport", "OBJ1"),
    ("OBJ1-TRN7", "Sea and coastal freight water transport", "OBJ1"),
    ("OBJ1-TRN8", "Sea and coastal passenger water transport", "OBJ1"),
    ("OBJ1-BLD1", "Construction of new buildings", "OBJ1"),
    ("OBJ1-BLD2", "Renovation of existing buildings", "OBJ1"),
    ("OBJ1-BLD3", "Installation, maintenance and repair of energy efficiency equipment", "OBJ1"),
    ("OBJ1-BLD4", "Installation, maintenance and repair of charging stations", "OBJ1"),
    ("OBJ1-MFG1", "Manufacturing of batteries and energy storage devices", "OBJ1"),
    ("OBJ1-MFG2", "Manufacturing of renewable energy equipment", "OBJ1"),
    ("OBJ1-MFG3", "Manufacture of electric vehicles", "OBJ1"),
    ("OBJ1-MFG4", "Manufacture of cement", "OBJ1"),
    ("OBJ1-MFG5", "Manufacture of aluminium", "OBJ1"),
    ("OBJ1-MFG6", "Manufacture of iron and steel", "OBJ1"),
    ("OBJ1-MFG7", "Manufacture of hydrogen", "OBJ1"),
    ("OBJ1-AGR1", "Afforestation, reforestation and restoration of forests", "OBJ1"),
    ("OBJ1-AGR2", "Forest management", "OBJ1"),
    ("OBJ1-AGR3", "Restoration of wetlands", "OBJ1"),
    ("OBJ2-INF1", "Adaptation measures for infrastructure systems", "OBJ2"),
    ("OBJ2-AGR1", "Climate adaptation in agriculture and forestry", "OBJ2"),
    ("OBJ2-URB1", "Urban greening and flood risk reduction", "OBJ2"),
    ("OBJ2-HLT1", "Health and adaptation to climate change", "OBJ2"),
    ("OBJ3-WAT1", "Collection, treatment and supply of water", "OBJ3"),
    ("OBJ3-WAT2", "Water management in urban areas", "OBJ3"),
    ("OBJ3-WAT3", "Marine conservation and protection", "OBJ3"),
    ("OBJ3-WAT4", "Sustainable aquaculture", "OBJ3"),
    ("OBJ4-CIR1", "Manufacture of recycled materials", "OBJ4"),
    ("OBJ4-CIR2", "Waste collection, treatment and recovery", "OBJ4"),
    ("OBJ4-CIR3", "Repair, refurbishment and remanufacturing", "OBJ4"),
    ("OBJ4-CIR4", "Data-driven solutions for circular economy", "OBJ4"),
    ("OBJ4-CIR5", "Direct non-sewage waste water treatment", "OBJ4"),
    ("OBJ5-POL1", "Pollution control and remediation of contaminated sites", "OBJ5"),
    ("OBJ5-POL2", "Collection and transport of hazardous waste", "OBJ5"),
    ("OBJ5-POL3", "Reduction of chemical pesticide use in agriculture", "OBJ5"),
    ("OBJ6-BIO1", "Restoration of biodiversity and habitats", "OBJ6"),
    ("OBJ6-BIO2", "Infrastructure enabling biodiversity", "OBJ6"),
    ("OBJ6-BIO3", "Conservation agriculture", "OBJ6"),
]


async def ingest_eu_taxonomy(conn) -> int:
    """Ingest EU Taxonomy into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "eu_taxonomy",
        "EU Taxonomy",
        "EU Taxonomy for Sustainable Finance (Regulation EU 2020/852)",
        "2024",
        "European Union",
        "European Commission",
    )

    count = 0
    for seq, (code, title) in enumerate(EU_TAX_OBJECTIVES, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "eu_taxonomy", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(EU_TAX_ACTIVITIES, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "eu_taxonomy", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'eu_taxonomy'",
        count,
    )

    return count
