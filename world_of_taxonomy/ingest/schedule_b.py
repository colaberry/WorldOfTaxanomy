"""US Schedule B Export Classification (top-level skeleton) ingester.

Schedule B is the US export classification system administered by the US
Census Bureau. It is based on the Harmonized System (HS) and is used to
classify goods for export reporting (Electronic Export Information / EEI in
the Automated Export System). This ingester covers the 21 HS sections at
level 1 and all 97 HS chapters at level 2 as reported by the Census Bureau.
"""
from __future__ import annotations

SYSTEM_ID = "schedule_b"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Sections (level 1)
    ("I",    "Live Animals; Animal Products",                           1, None),
    ("II",   "Vegetable Products",                                      1, None),
    ("III",  "Animal or Vegetable Fats and Oils",                       1, None),
    ("IV",   "Prepared Foodstuffs; Beverages; Tobacco",                 1, None),
    ("V",    "Mineral Products",                                        1, None),
    ("VI",   "Products of the Chemical or Allied Industries",           1, None),
    ("VII",  "Plastics and Articles Thereof; Rubber",                   1, None),
    ("VIII", "Raw Hides, Skins, Leather, Furskins",                     1, None),
    ("IX",   "Wood and Articles of Wood; Cork",                         1, None),
    ("X",    "Pulp of Wood; Paper and Paperboard",                      1, None),
    ("XI",   "Textiles and Textile Articles",                           1, None),
    ("XII",  "Footwear, Headgear, Umbrellas",                           1, None),
    ("XIII", "Articles of Stone, Plaster, Cement, Ceramics, Glass",     1, None),
    ("XIV",  "Natural or Cultured Pearls, Precious Metals, Jewellery",  1, None),
    ("XV",   "Base Metals and Articles of Base Metal",                  1, None),
    ("XVI",  "Machinery and Mechanical Appliances; Electrical Equipment",1,None),
    ("XVII", "Vehicles, Aircraft, Vessels and Transport Equipment",      1, None),
    ("XVIII","Optical, Photographic, Cinematographic, Measuring Instruments",1,None),
    ("XIX",  "Arms and Ammunition",                                     1, None),
    ("XX",   "Miscellaneous Manufactured Articles",                     1, None),
    ("XXI",  "Works of Art, Collectors' Pieces and Antiques",           1, None),
    # Chapters (level 2)
    ("01",   "Live Animals",                                            2, "I"),
    ("02",   "Meat and Edible Meat Offal",                              2, "I"),
    ("03",   "Fish, Crustaceans, Molluscs",                             2, "I"),
    ("04",   "Dairy Produce; Birds' Eggs; Honey",                       2, "I"),
    ("05",   "Products of Animal Origin",                               2, "I"),
    ("06",   "Live Trees and Other Plants",                             2, "II"),
    ("07",   "Edible Vegetables and Certain Roots",                     2, "II"),
    ("08",   "Edible Fruit and Nuts",                                   2, "II"),
    ("09",   "Coffee, Tea, Mate and Spices",                            2, "II"),
    ("10",   "Cereals",                                                 2, "II"),
    ("11",   "Milling Industry Products",                               2, "II"),
    ("12",   "Oil Seeds, Oleaginous Fruits",                            2, "II"),
    ("13",   "Lac; Gums, Resins and Other Vegetable Saps",              2, "II"),
    ("14",   "Vegetable Plaiting Materials",                            2, "II"),
    ("15",   "Animal, Vegetable or Microbial Fats and Oils",            2, "III"),
    ("16",   "Preparations of Meat, Fish or Crustaceans",               2, "IV"),
    ("17",   "Sugars and Sugar Confectionery",                          2, "IV"),
    ("18",   "Cocoa and Cocoa Preparations",                            2, "IV"),
    ("19",   "Preparations of Cereals, Flour, Starch or Milk",          2, "IV"),
    ("20",   "Preparations of Vegetables, Fruit, Nuts",                 2, "IV"),
    ("21",   "Miscellaneous Edible Preparations",                       2, "IV"),
    ("22",   "Beverages, Spirits and Vinegar",                          2, "IV"),
    ("23",   "Residues and Waste from Food Industries",                 2, "IV"),
    ("24",   "Tobacco and Manufactured Tobacco Substitutes",            2, "IV"),
    ("25",   "Salt; Sulphur; Earths and Stone",                         2, "V"),
    ("26",   "Ores, Slag and Ash",                                      2, "V"),
    ("27",   "Mineral Fuels, Oils and Products of Their Distillation",  2, "V"),
    ("28",   "Inorganic Chemicals",                                     2, "VI"),
    ("29",   "Organic Chemicals",                                       2, "VI"),
    ("30",   "Pharmaceutical Products",                                 2, "VI"),
    ("31",   "Fertilisers",                                             2, "VI"),
    ("32",   "Tanning, Dyeing Extracts; Paints; Varnishes",             2, "VI"),
    ("33",   "Essential Oils and Resinoids; Perfumery",                 2, "VI"),
    ("34",   "Soap, Washing Preparations, Lubricating Preparations",    2, "VI"),
    ("35",   "Albuminoidal Substances; Modified Starches; Glues",       2, "VI"),
    ("36",   "Explosives; Pyrotechnic Products; Matches",               2, "VI"),
    ("37",   "Photographic or Cinematographic Goods",                   2, "VI"),
    ("38",   "Miscellaneous Chemical Products",                         2, "VI"),
    ("39",   "Plastics and Articles Thereof",                           2, "VII"),
    ("40",   "Rubber and Articles Thereof",                             2, "VII"),
    ("41",   "Raw Hides and Skins (Other Than Furskins) and Leather",   2, "VIII"),
    ("42",   "Articles of Leather; Saddlery and Harness",               2, "VIII"),
    ("43",   "Furskins and Artificial Fur",                             2, "VIII"),
    ("44",   "Wood and Articles of Wood; Wood Charcoal",                2, "IX"),
    ("45",   "Cork and Articles of Cork",                               2, "IX"),
    ("46",   "Manufactures of Straw, Esparto or Other Plaiting Materials",2,"IX"),
    ("47",   "Pulp of Wood or of Other Fibrous Cellulosic Material",    2, "X"),
    ("48",   "Paper and Paperboard",                                    2, "X"),
    ("49",   "Printed Books, Newspapers, Pictures",                     2, "X"),
    ("50",   "Silk",                                                    2, "XI"),
    ("51",   "Wool, Fine or Coarse Animal Hair",                        2, "XI"),
    ("52",   "Cotton",                                                  2, "XI"),
    ("53",   "Other Vegetable Textile Fibres",                          2, "XI"),
    ("54",   "Man-Made Filaments",                                      2, "XI"),
    ("55",   "Man-Made Staple Fibres",                                  2, "XI"),
    ("56",   "Wadding, Felt and Nonwovens",                             2, "XI"),
    ("57",   "Carpets and Other Textile Floor Coverings",               2, "XI"),
    ("58",   "Special Woven Fabrics",                                   2, "XI"),
    ("59",   "Impregnated, Coated, Covered or Laminated Textile Fabrics",2,"XI"),
    ("60",   "Knitted or Crocheted Fabrics",                            2, "XI"),
    ("61",   "Articles of Apparel and Clothing Accessories, Knitted",   2, "XI"),
    ("62",   "Articles of Apparel and Clothing Accessories, Not Knitted",2,"XI"),
    ("63",   "Other Made Up Textile Articles",                          2, "XI"),
    ("64",   "Footwear, Gaiters and the Like",                          2, "XII"),
    ("65",   "Headgear and Parts Thereof",                              2, "XII"),
    ("66",   "Umbrellas, Sun Umbrellas, Walking-Sticks",                2, "XII"),
    ("67",   "Prepared Feathers and Down",                              2, "XII"),
    ("68",   "Articles of Stone, Plaster, Cement, Asbestos",            2, "XIII"),
    ("69",   "Ceramic Products",                                        2, "XIII"),
    ("70",   "Glass and Glassware",                                     2, "XIII"),
    ("71",   "Natural or Cultured Pearls, Precious Metals",             2, "XIV"),
    ("72",   "Iron and Steel",                                          2, "XV"),
    ("73",   "Articles of Iron or Steel",                               2, "XV"),
    ("74",   "Copper and Articles Thereof",                             2, "XV"),
    ("75",   "Nickel and Articles Thereof",                             2, "XV"),
    ("76",   "Aluminium and Articles Thereof",                          2, "XV"),
    ("78",   "Lead and Articles Thereof",                               2, "XV"),
    ("79",   "Zinc and Articles Thereof",                               2, "XV"),
    ("80",   "Tin and Articles Thereof",                                2, "XV"),
    ("81",   "Other Base Metals; Cermets",                              2, "XV"),
    ("82",   "Tools, Implements, Cutlery, Spoons and Forks",            2, "XV"),
    ("83",   "Miscellaneous Articles of Base Metal",                    2, "XV"),
    ("84",   "Nuclear Reactors, Boilers, Machinery",                    2, "XVI"),
    ("85",   "Electrical Machinery and Equipment",                      2, "XVI"),
    ("86",   "Railway or Tramway Locomotives",                          2, "XVII"),
    ("87",   "Vehicles Other Than Railway",                             2, "XVII"),
    ("88",   "Aircraft, Spacecraft",                                    2, "XVII"),
    ("89",   "Ships, Boats and Floating Structures",                    2, "XVII"),
    ("90",   "Optical, Photographic, Cinematographic Instruments",      2, "XVIII"),
    ("91",   "Clocks and Watches",                                      2, "XVIII"),
    ("92",   "Musical Instruments",                                     2, "XVIII"),
    ("93",   "Arms and Ammunition",                                     2, "XIX"),
    ("94",   "Furniture; Bedding, Mattresses",                          2, "XX"),
    ("95",   "Toys, Games and Sports Requisites",                       2, "XX"),
    ("96",   "Miscellaneous Manufactured Articles",                     2, "XX"),
    ("97",   "Works of Art, Collectors' Pieces and Antiques",           2, "XXI"),
    ("98",   "Special Classification Provisions (US Only)",             2, "XXI"),
    ("99",   "Temporary Legislation, US Gov't Imports",                 2, "XXI"),
]


async def ingest_schedule_b(conn) -> int:
    """Ingest US Schedule B Export Classification skeleton (sections + chapters)."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "Schedule B",
        "Schedule B - Statistical Classification of Domestic and Foreign Commodities Exported from the United States",
        "United States",
        "2022",
        "US Census Bureau, Foreign Trade Division",
        "#65A30D",
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
    print(f"  Ingested {count} Schedule B codes")
    return count
