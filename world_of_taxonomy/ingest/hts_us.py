"""HTS (Harmonized Tariff Schedule of the United States) ingester.

The HTS is the US implementation of the Harmonized System (HS) maintained by
the World Customs Organization. This ingester covers 22 sections and 99 chapters
(the 2-digit level of the tariff schedule).

Source: US International Trade Commission (USITC) - public domain US government work
URL: https://hts.usitc.gov
"""
from __future__ import annotations

HTS_SECTIONS: list[tuple[str, str]] = [
    ("I", "Live Animals and Animal Products"),
    ("II", "Vegetable Products"),
    ("III", "Animal or Vegetable Fats and Oils"),
    ("IV", "Prepared Foodstuffs, Beverages, Spirits, Vinegar and Tobacco"),
    ("V", "Mineral Products"),
    ("VI", "Products of the Chemical or Allied Industries"),
    ("VII", "Plastics and Rubber"),
    ("VIII", "Raw Hides and Skins, Leather and Furskins"),
    ("IX", "Wood, Cork and Articles Thereof"),
    ("X", "Pulp of Wood and Paper"),
    ("XI", "Textiles and Textile Articles"),
    ("XII", "Footwear, Headgear, Umbrellas"),
    ("XIII", "Articles of Stone, Plaster, Cement, Asbestos, Mica"),
    ("XIV", "Natural or Cultured Pearls, Precious Metals"),
    ("XV", "Base Metals and Articles Thereof"),
    ("XVI", "Machinery, Mechanical Appliances, Electrical Equipment"),
    ("XVII", "Vehicles, Aircraft, Vessels and Transport Equipment"),
    ("XVIII", "Optical, Photographic, Precision, Medical Instruments"),
    ("XIX", "Arms and Ammunition"),
    ("XX", "Miscellaneous Manufactured Articles"),
    ("XXI", "Works of Art, Collectors' Pieces and Antiques"),
    ("XXII", "Special Classification Provisions"),
]

HTS_CHAPTERS: list[tuple[str, str, str]] = [
    ("01", "Live Animals", "I"),
    ("02", "Meat and Edible Meat Offal", "I"),
    ("03", "Fish and Crustaceans, Molluscs and Other Aquatic Invertebrates", "I"),
    ("04", "Dairy Produce; Birds' Eggs; Honey; Other Edible Animal Products", "I"),
    ("05", "Products of Animal Origin (not elsewhere specified)", "I"),
    ("06", "Live Trees and Other Plants; Bulbs, Roots; Cut Flowers", "II"),
    ("07", "Edible Vegetables and Certain Roots and Tubers", "II"),
    ("08", "Edible Fruit and Nuts; Peel of Citrus Fruit or Melons", "II"),
    ("09", "Coffee, Tea, Mate and Spices", "II"),
    ("10", "Cereals", "II"),
    ("11", "Products of the Milling Industry; Malt; Starches", "II"),
    ("12", "Oil Seeds and Oleaginous Fruits; Miscellaneous Grains, Seeds", "II"),
    ("13", "Lac; Gums, Resins and Other Vegetable Saps", "II"),
    ("14", "Vegetable Plaiting Materials; Vegetable Products NEC", "II"),
    ("15", "Animal, Vegetable or Microbial Fats and Oils", "III"),
    ("16", "Preparations of Meat, Fish or Crustaceans", "IV"),
    ("17", "Sugars and Sugar Confectionery", "IV"),
    ("18", "Cocoa and Cocoa Preparations", "IV"),
    ("19", "Preparations of Cereals, Flour, Starch or Milk; Bakers' Wares", "IV"),
    ("20", "Preparations of Vegetables, Fruit or Nuts", "IV"),
    ("21", "Miscellaneous Edible Preparations", "IV"),
    ("22", "Beverages, Spirits and Vinegar", "IV"),
    ("23", "Residues and Waste from the Food Industries; Animal Fodder", "IV"),
    ("24", "Tobacco and Manufactured Tobacco Substitutes", "IV"),
    ("25", "Salt; Sulfur; Earths and Stone; Plastering Materials; Lime", "V"),
    ("26", "Ores, Slag and Ash", "V"),
    ("27", "Mineral Fuels, Mineral Oils and Products of Distillation", "V"),
    ("28", "Inorganic Chemicals; Precious Metal Compounds; Isotopes", "VI"),
    ("29", "Organic Chemicals", "VI"),
    ("30", "Pharmaceutical Products", "VI"),
    ("31", "Fertilizers", "VI"),
    ("32", "Tanning, Dyeing Extracts; Dyes, Pigments, Paints, Varnishes", "VI"),
    ("33", "Essential Oils; Perfumery, Cosmetic or Toilet Preparations", "VI"),
    ("34", "Soap, Organic Surface-Active Agents, Washing Preparations", "VI"),
    ("35", "Albuminoidal Substances; Modified Starches; Glues; Enzymes", "VI"),
    ("36", "Explosives; Pyrotechnic Products; Matches; Pyrophoric Alloys", "VI"),
    ("37", "Photographic or Cinematographic Goods", "VI"),
    ("38", "Miscellaneous Chemical Products", "VI"),
    ("39", "Plastics and Articles Thereof", "VII"),
    ("40", "Rubber and Articles Thereof", "VII"),
    ("41", "Raw Hides and Skins (other than Furskins) and Leather", "VIII"),
    ("42", "Articles of Leather; Saddlery; Travel Goods; Handbags", "VIII"),
    ("43", "Furskins and Artificial Fur; Manufactures Thereof", "VIII"),
    ("44", "Wood and Articles of Wood; Wood Charcoal", "IX"),
    ("45", "Cork and Articles of Cork", "IX"),
    ("46", "Manufactures of Straw, Esparto or Other Plaiting Materials", "IX"),
    ("47", "Pulp of Wood or of Other Fibrous Cellulosic Material", "X"),
    ("48", "Paper and Paperboard; Articles of Paper Pulp", "X"),
    ("49", "Printed Books, Newspapers, Pictures and Other Products of the Printing Industry", "X"),
    ("50", "Silk", "XI"),
    ("51", "Wool, Fine or Coarse Animal Hair; Horsehair Yarn and Woven Fabric", "XI"),
    ("52", "Cotton", "XI"),
    ("53", "Other Vegetable Textile Fibers; Paper Yarn and Woven Fabrics", "XI"),
    ("54", "Man-Made Filaments; Strip and Like of Man-Made Textile Materials", "XI"),
    ("55", "Man-Made Staple Fibers", "XI"),
    ("56", "Wadding, Felt, Nonwovens; Special Yarns; Twine, Cordage, Ropes", "XI"),
    ("57", "Carpets and Other Textile Floor Coverings", "XI"),
    ("58", "Special Woven Fabrics; Tufted Textile Fabrics; Lace; Tapestries", "XI"),
    ("59", "Impregnated, Coated, Covered or Laminated Textile Fabrics", "XI"),
    ("60", "Knitted or Crocheted Fabrics", "XI"),
    ("61", "Articles of Apparel and Clothing Accessories - Knitted or Crocheted", "XI"),
    ("62", "Articles of Apparel and Clothing Accessories - Not Knitted or Crocheted", "XI"),
    ("63", "Other Made Up Textile Articles; Sets; Worn Clothing and Rags", "XI"),
    ("64", "Footwear, Gaiters and the Like; Parts of Such Articles", "XII"),
    ("65", "Headgear and Parts Thereof", "XII"),
    ("66", "Umbrellas, Sun Umbrellas, Walking Sticks, Seat-Sticks, Whips", "XII"),
    ("67", "Prepared Feathers and Down; Artificial Flowers; Articles of Human Hair", "XII"),
    ("68", "Articles of Stone, Plaster, Cement, Asbestos, Mica or Similar Materials", "XIII"),
    ("69", "Ceramic Products", "XIII"),
    ("70", "Glass and Glassware", "XIII"),
    ("71", "Natural or Cultured Pearls, Precious or Semi-Precious Stones, Precious Metals", "XIV"),
    ("72", "Iron and Steel", "XV"),
    ("73", "Articles of Iron or Steel", "XV"),
    ("74", "Copper and Articles Thereof", "XV"),
    ("75", "Nickel and Articles Thereof", "XV"),
    ("76", "Aluminum and Articles Thereof", "XV"),
    ("78", "Lead and Articles Thereof", "XV"),
    ("79", "Zinc and Articles Thereof", "XV"),
    ("80", "Tin and Articles Thereof", "XV"),
    ("81", "Other Base Metals; Cermets; Articles Thereof", "XV"),
    ("82", "Tools, Implements, Cutlery, Spoons and Forks of Base Metal", "XV"),
    ("83", "Miscellaneous Articles of Base Metal", "XV"),
    ("84", "Nuclear Reactors, Boilers, Machinery and Mechanical Appliances", "XVI"),
    ("85", "Electrical Machinery and Equipment; Sound Recorders; TV Recorders", "XVI"),
    ("86", "Railway or Tramway Locomotives, Rolling Stock and Parts", "XVII"),
    ("87", "Vehicles Other Than Railway or Tramway Rolling Stock, Parts", "XVII"),
    ("88", "Aircraft, Spacecraft and Parts Thereof", "XVII"),
    ("89", "Ships, Boats and Floating Structures", "XVII"),
    ("90", "Optical, Photographic, Cinematographic, Measuring, Checking, Medical Instruments", "XVIII"),
    ("91", "Clocks and Watches and Parts Thereof", "XVIII"),
    ("92", "Musical Instruments; Parts and Accessories of Such Articles", "XVIII"),
    ("93", "Arms and Ammunition; Parts and Accessories Thereof", "XIX"),
    ("94", "Furniture; Bedding, Mattresses, Cushions; Lamps; Prefabricated Buildings", "XX"),
    ("95", "Toys, Games and Sports Requisites; Parts and Accessories", "XX"),
    ("96", "Miscellaneous Manufactured Articles", "XX"),
    ("97", "Works of Art, Collectors' Pieces and Antiques", "XXI"),
    ("98", "Special Classification Provisions - Temporary Imports", "XXII"),
    ("99", "Special Classification Provisions - Trade Program Provisions", "XXII"),
]


async def ingest_hts_us(conn) -> int:
    """Ingest HTS sections and chapters into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "hts_us",
        "HTS (US)",
        "Harmonized Tariff Schedule of the United States",
        "2024",
        "United States",
        "US International Trade Commission (USITC)",
    )

    count = 0
    for seq, (code, title) in enumerate(HTS_SECTIONS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "hts_us", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(HTS_CHAPTERS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "hts_us", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'hts_us'",
        count,
    )

    # Crosswalk HTS chapters to HS 2022 (same chapter codes)
    for chapter_code, _, _ in HTS_CHAPTERS:
        hs_exists = await conn.fetchval(
            "SELECT 1 FROM classification_node WHERE system_id = 'hs_2022' AND code LIKE $1",
            chapter_code + "%",
        )
        if hs_exists:
            await conn.execute(
                """INSERT INTO equivalence
                       (source_system, source_code, target_system, target_code, match_type, notes)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
                "hts_us", chapter_code, "hs_2022", chapter_code,
                "broad", "HTS chapter to HS 2022 equivalent chapter",
            )

    return count
