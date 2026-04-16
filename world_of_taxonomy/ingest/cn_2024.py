"""EU Combined Nomenclature 2024 (CN 2024) skeleton ingester.

The EU Combined Nomenclature is the EU\'s 8-digit trade classification,
built on top of HS 2022 with EU-specific subheadings. Published annually
in the EU Official Journal. Used for EU customs declarations (Intrastat,
Extrastat) and trade statistics reporting.
This skeleton covers 21 HS sections and all active HS chapters (1-97, 99).
Full 8-digit enumeration requires parsing the EU Official Journal XML.
"""
from __future__ import annotations

SYSTEM_ID = "cn_2024"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Sections (level 1)
    ("S01", "Live Animals; Animal Products",                                           1, None),
    ("S02", "Vegetable Products",                                                      1, None),
    ("S03", "Animal or Vegetable Fats and Oils and Prepared Edible Fats",              1, None),
    ("S04", "Prepared Foodstuffs; Beverages, Spirits and Vinegar; Tobacco",            1, None),
    ("S05", "Mineral Products",                                                        1, None),
    ("S06", "Products of the Chemical or Allied Industries",                           1, None),
    ("S07", "Plastics and Articles Thereof; Rubber and Articles Thereof",              1, None),
    ("S08", "Raw Hides and Skins, Leather, Furskins and Articles Thereof",             1, None),
    ("S09", "Wood and Articles of Wood; Wood Charcoal; Cork and Cork Articles",        1, None),
    ("S10", "Pulp of Wood; Paper and Paperboard; Articles Thereof; Printed Books",     1, None),
    ("S11", "Textiles and Textile Articles",                                           1, None),
    ("S12", "Footwear, Headgear, Umbrellas, Walking Sticks and Similar Articles",      1, None),
    ("S13", "Articles of Stone, Plaster, Cement, Ceramics, Glass and Glassware",      1, None),
    ("S14", "Natural or Cultured Pearls, Precious Metals, Jewellery, Coins",           1, None),
    ("S15", "Base Metals and Articles of Base Metal",                                  1, None),
    ("S16", "Machinery and Mechanical Appliances; Electrical Equipment",               1, None),
    ("S17", "Vehicles, Aircraft, Vessels and Associated Transport Equipment",          1, None),
    ("S18", "Optical, Photographic, Medical or Surgical Instruments and Apparatus",   1, None),
    ("S19", "Arms and Ammunition; Parts and Accessories Thereof",                      1, None),
    ("S20", "Miscellaneous Manufactured Articles",                                     1, None),
    ("S21", "Works of Art, Collectors Pieces and Antiques; EU Special Codes",         1, None),
    # Chapters (level 2)
    ("01", "Live animals",                                                             2, "S01"),
    ("02", "Meat and edible meat offal",                                               2, "S01"),
    ("03", "Fish and crustaceans, molluscs and other aquatic invertebrates",           2, "S01"),
    ("04", "Dairy produce; birds eggs; natural honey; edible products of animal origin", 2, "S01"),
    ("05", "Products of animal origin, not elsewhere specified or included",           2, "S01"),
    ("06", "Live trees and other plants; bulbs, roots and similar",                    2, "S02"),
    ("07", "Edible vegetables and certain roots and tubers",                           2, "S02"),
    ("08", "Edible fruit and nuts; peel of citrus fruit or melons",                    2, "S02"),
    ("09", "Coffee, tea, mate and spices",                                             2, "S02"),
    ("10", "Cereals",                                                                  2, "S02"),
    ("11", "Products of the milling industry; malt; starches; inulin; wheat gluten",  2, "S02"),
    ("12", "Oil seeds and oleaginous fruits; miscellaneous grains, seeds and fruit",   2, "S02"),
    ("13", "Lac; gums, resins and other vegetable saps and extracts",                  2, "S02"),
    ("14", "Vegetable plaiting materials; vegetable products not elsewhere specified", 2, "S02"),
    ("15", "Animal, vegetable or microbial fats and oils and their cleavage products", 2, "S03"),
    ("16", "Preparations of meat, fish, crustaceans, molluscs or other aquatic invertebrates", 2, "S04"),
    ("17", "Sugars and sugar confectionery",                                           2, "S04"),
    ("18", "Cocoa and cocoa preparations",                                             2, "S04"),
    ("19", "Preparations of cereals, flour, starch or milk; pastrycooks products",    2, "S04"),
    ("20", "Preparations of vegetables, fruit, nuts or other parts of plants",        2, "S04"),
    ("21", "Miscellaneous edible preparations",                                        2, "S04"),
    ("22", "Beverages, spirits and vinegar",                                           2, "S04"),
    ("23", "Residues and waste from the food industries; prepared animal fodder",      2, "S04"),
    ("24", "Tobacco and manufactured tobacco substitutes; products containing tobacco substitutes", 2, "S04"),
    ("25", "Salt; sulphur; earths and stone; plastering materials, lime and cement",   2, "S05"),
    ("26", "Ores, slag and ash",                                                       2, "S05"),
    ("27", "Mineral fuels, mineral oils and products of their distillation",           2, "S05"),
    ("28", "Inorganic chemicals; organic or inorganic compounds of precious metals",   2, "S06"),
    ("29", "Organic chemicals",                                                        2, "S06"),
    ("30", "Pharmaceutical products",                                                  2, "S06"),
    ("31", "Fertilisers",                                                              2, "S06"),
    ("32", "Tanning or dyeing extracts; tannins and their derivatives; dyes, pigments", 2, "S06"),
    ("33", "Essential oils and resinoids; perfumery, cosmetic or toilet preparations", 2, "S06"),
    ("34", "Soap, organic surface-active agents, washing preparations, lubricating preparations", 2, "S06"),
    ("35", "Albuminoidal substances; modified starches; glues; enzymes",               2, "S06"),
    ("36", "Explosives; pyrotechnic products; matches; pyrophoric alloys; certain combustible preparations", 2, "S06"),
    ("37", "Photographic or cinematographic goods",                                    2, "S06"),
    ("38", "Miscellaneous chemical products",                                          2, "S06"),
    ("39", "Plastics and articles thereof",                                            2, "S07"),
    ("40", "Rubber and articles thereof",                                              2, "S07"),
    ("41", "Raw hides and skins (other than furskins) and leather",                    2, "S08"),
    ("42", "Articles of leather; saddlery and harness; travel goods; handbags",       2, "S08"),
    ("43", "Furskins and artificial fur; manufactures thereof",                        2, "S08"),
    ("44", "Wood and articles of wood; wood charcoal",                                 2, "S09"),
    ("45", "Cork and articles of cork",                                                2, "S09"),
    ("46", "Manufactures of straw, of esparto or of other plaiting materials; basketware", 2, "S09"),
    ("47", "Pulp of wood or of other fibrous cellulosic material; waste and scrap of paper", 2, "S10"),
    ("48", "Paper and paperboard; articles of paper pulp, paper or paperboard",        2, "S10"),
    ("49", "Printed books, newspapers, pictures and other products of the printing industry", 2, "S10"),
    ("50", "Silk",                                                                     2, "S11"),
    ("51", "Wool, fine or coarse animal hair; horsehair yarn and woven fabric",        2, "S11"),
    ("52", "Cotton",                                                                   2, "S11"),
    ("53", "Other vegetable textile fibres; paper yarn and woven fabrics of paper yarn", 2, "S11"),
    ("54", "Man-made filaments; strip and the like of man-made textile materials",     2, "S11"),
    ("55", "Man-made staple fibres",                                                   2, "S11"),
    ("56", "Wadding, felt and nonwovens; special yarns; twine, cordage, ropes and cables", 2, "S11"),
    ("57", "Carpets and other textile floor coverings",                                2, "S11"),
    ("58", "Special woven fabrics; tufted textile fabrics; lace; tapestries; trimmings; embroidery", 2, "S11"),
    ("59", "Impregnated, coated, covered or laminated textile fabrics",                2, "S11"),
    ("60", "Knitted or crocheted fabrics",                                             2, "S11"),
    ("61", "Articles of apparel and clothing accessories, knitted or crocheted",       2, "S11"),
    ("62", "Articles of apparel and clothing accessories, not knitted or crocheted",   2, "S11"),
    ("63", "Other made-up textile articles; sets; worn clothing and worn textile articles", 2, "S11"),
    ("64", "Footwear, gaiters and the like; parts of such articles",                   2, "S12"),
    ("65", "Headgear and parts thereof",                                               2, "S12"),
    ("66", "Umbrellas, sun umbrellas, walking sticks, seat-sticks, whips, riding-crops", 2, "S12"),
    ("67", "Prepared feathers and down and articles made of feathers or of down; artificial flowers", 2, "S12"),
    ("68", "Articles of stone, plaster, cement, asbestos, mica or similar materials", 2, "S13"),
    ("69", "Ceramic products",                                                         2, "S13"),
    ("70", "Glass and glassware",                                                      2, "S13"),
    ("71", "Natural or cultured pearls, precious stones, precious metals, coins",      2, "S14"),
    ("72", "Iron and steel",                                                           2, "S15"),
    ("73", "Articles of iron or steel",                                                2, "S15"),
    ("74", "Copper and articles thereof",                                              2, "S15"),
    ("75", "Nickel and articles thereof",                                              2, "S15"),
    ("76", "Aluminium and articles thereof",                                           2, "S15"),
    ("78", "Lead and articles thereof",                                                2, "S15"),
    ("79", "Zinc and articles thereof",                                                2, "S15"),
    ("80", "Tin and articles thereof",                                                 2, "S15"),
    ("81", "Other base metals; cermets; articles thereof",                             2, "S15"),
    ("82", "Tools, implements, cutlery, spoons and forks, of base metal",              2, "S15"),
    ("83", "Miscellaneous articles of base metal",                                     2, "S15"),
    ("84", "Nuclear reactors, boilers, machinery and mechanical appliances; parts thereof", 2, "S16"),
    ("85", "Electrical machinery and equipment and parts thereof; sound recorders",    2, "S16"),
    ("86", "Railway or tramway locomotives, rolling stock and parts thereof",          2, "S17"),
    ("87", "Vehicles other than railway or tramway rolling stock, and parts",          2, "S17"),
    ("88", "Aircraft, spacecraft, and parts thereof",                                  2, "S17"),
    ("89", "Ships, boats and floating structures",                                     2, "S17"),
    ("90", "Optical, photographic, cinematographic, measuring, checking, medical instruments", 2, "S18"),
    ("91", "Clocks and watches and parts thereof",                                     2, "S18"),
    ("92", "Musical instruments; parts and accessories of such articles",              2, "S18"),
    ("93", "Arms and ammunition; parts and accessories thereof",                       2, "S19"),
    ("94", "Furniture; bedding, mattresses, mattress supports, cushions and similar stuffed furnishings", 2, "S20"),
    ("95", "Toys, games and sports requisites; parts and accessories thereof",         2, "S20"),
    ("96", "Miscellaneous manufactured articles",                                      2, "S20"),
    ("97", "Works of art, collectors pieces and antiques",                             2, "S21"),
    ("99", "Miscellaneous (EU specific codes for trade statistics)",                   2, "S21"),
]


async def ingest_cn_2024(conn) -> int:
    """Ingest EU Combined Nomenclature 2024 skeleton (sections and HS chapters)."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "CN 2024",
        "EU Combined Nomenclature 2024",
        "European Union",
        "2024",
        "European Commission (DG TAXUD)",
        "#3B82F6",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code[:3] if level == 1 else code[:2]
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
    print(f"  Ingested {count} CN 2024 codes")
    return count
