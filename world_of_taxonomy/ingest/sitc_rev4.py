"""SITC Rev 4 (Standard International Trade Classification, Revision 4) ingester.

UN Statistics Division, 2006. Sections (0-9) plus 2-digit divisions (67 codes).
SITC classifies internationally traded goods for statistical analysis,
complementary to HS (HS is customs-focused, SITC is economic-analysis-focused).

Source: UN Statistics Division. Hand-coded sections and 2-digit divisions.
"""
from typing import List, Tuple

SECTIONS: List[Tuple[str, str]] = [
    ("0", "Food and live animals"),
    ("1", "Beverages and tobacco"),
    ("2", "Crude materials, inedible, except fuels"),
    ("3", "Mineral fuels, lubricants and related materials"),
    ("4", "Animal and vegetable oils, fats and waxes"),
    ("5", "Chemicals and related products, n.e.s."),
    ("6", "Manufactured goods classified chiefly by material"),
    ("7", "Machinery and transport equipment"),
    ("8", "Miscellaneous manufactured articles"),
    ("9", "Commodities and transactions not classified elsewhere"),
]

# (section_code, division_code, title)
DIVISIONS: List[Tuple[str, str, str]] = [
    ("0", "00", "Live animals other than animals of division 03"),
    ("0", "01", "Meat and meat preparations"),
    ("0", "02", "Dairy products and birds' eggs"),
    ("0", "03", "Fish, crustaceans, molluscs and aquatic invertebrates"),
    ("0", "04", "Cereals and cereal preparations"),
    ("0", "05", "Vegetables and fruit"),
    ("0", "06", "Sugars, sugar preparations and honey"),
    ("0", "07", "Coffee, tea, cocoa, spices and manufactures thereof"),
    ("0", "08", "Feeding stuff for animals (excl. unmilled cereals)"),
    ("0", "09", "Miscellaneous edible products and preparations"),
    ("1", "11", "Beverages"),
    ("1", "12", "Tobacco and tobacco manufactures"),
    ("2", "21", "Hides, skins and furskins, raw"),
    ("2", "22", "Oil seeds and oleaginous fruits"),
    ("2", "23", "Crude rubber (incl. synthetic and reclaimed)"),
    ("2", "24", "Cork and wood"),
    ("2", "25", "Pulp and waste paper"),
    ("2", "26", "Textile fibres and their wastes"),
    ("2", "27", "Crude fertilizers and crude minerals"),
    ("2", "28", "Metalliferous ores and metal scrap"),
    ("2", "29", "Crude animal and vegetable materials, n.e.s."),
    ("3", "32", "Coal, coke and briquettes"),
    ("3", "33", "Petroleum, petroleum products and related materials"),
    ("3", "34", "Gas, natural and manufactured"),
    ("3", "35", "Electric current"),
    ("4", "41", "Animal oils and fats"),
    ("4", "42", "Fixed vegetable fats and oils, crude, refined or fractionated"),
    ("4", "43", "Animal or vegetable fats and oils, processed; waxes"),
    ("5", "51", "Organic chemicals"),
    ("5", "52", "Inorganic chemicals"),
    ("5", "53", "Dyeing, tanning and colouring materials"),
    ("5", "54", "Medicinal and pharmaceutical products"),
    ("5", "55", "Essential oils, perfume materials; toilet preparations"),
    ("5", "56", "Fertilizers (other than group 272)"),
    ("5", "57", "Plastics in primary forms"),
    ("5", "58", "Plastics in non-primary forms"),
    ("5", "59", "Chemical materials and products, n.e.s."),
    ("6", "61", "Leather, leather manufactures and dressed furskins"),
    ("6", "62", "Rubber manufactures, n.e.s."),
    ("6", "63", "Cork and wood manufactures (excl. furniture)"),
    ("6", "64", "Paper, paperboard and articles of paper"),
    ("6", "65", "Textile yarn, fabrics and made-up articles"),
    ("6", "66", "Non-metallic mineral manufactures, n.e.s."),
    ("6", "67", "Iron and steel"),
    ("6", "68", "Non-ferrous metals"),
    ("6", "69", "Manufactures of metals, n.e.s."),
    ("7", "71", "Power-generating machinery and equipment"),
    ("7", "72", "Machinery specialised for particular industries"),
    ("7", "73", "Metalworking machinery"),
    ("7", "74", "General industrial machinery and equipment, n.e.s."),
    ("7", "75", "Office machines and automatic data-processing machines"),
    ("7", "76", "Telecommunications and sound-recording apparatus"),
    ("7", "77", "Electrical machinery, apparatus and appliances, n.e.s."),
    ("7", "78", "Road vehicles (including air-cushion vehicles)"),
    ("7", "79", "Other transport equipment"),
    ("8", "81", "Prefabricated buildings; plumbing and heating fixtures"),
    ("8", "82", "Furniture and parts thereof; bedding and cushions"),
    ("8", "83", "Travel goods, handbags and similar containers"),
    ("8", "84", "Articles of apparel and clothing accessories"),
    ("8", "85", "Footwear"),
    ("8", "87", "Professional, scientific and controlling instruments"),
    ("8", "88", "Photographic apparatus, optical goods, watches and clocks"),
    ("8", "89", "Miscellaneous manufactured articles, n.e.s."),
    ("9", "91", "Postal packages not classified according to kind"),
    ("9", "93", "Special transactions and commodities not classified"),
    ("9", "96", "Coin (other than gold coin), not being legal tender"),
    ("9", "97", "Gold, non-monetary (excl. gold ores and concentrates)"),
]

HS_CROSSWALK = {"0":"02","1":"24","2":"14","3":"27","4":"15","5":"29","6":"72","7":"84","8":"61","9":"97"}


async def ingest_sitc_rev4(conn) -> int:
    """Ingest SITC Rev 4 sections and 2-digit divisions. Returns node count."""
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, region, version, authority, tint_color)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "sitc_rev4", "SITC Rev 4",
        "Standard International Trade Classification, Revision 4",
        "Global", "Revision 4 (2006)", "UN Statistics Division", "#06B6D4",
    )

    count = 0
    for seq, (code, title) in enumerate(SECTIONS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ('sitc_rev4', $1, $2, 0, NULL, $1, FALSE, $3)
               ON CONFLICT (system_id, code) DO NOTHING""",
            code, title, seq,
        )
        count += 1

    for seq, (sec, div_code, div_title) in enumerate(DIVISIONS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ('sitc_rev4', $1, $2, 1, $3, $4, TRUE, $5)
               ON CONFLICT (system_id, code) DO NOTHING""",
            div_code, div_title, sec, sec, seq,
        )
        count += 1

    await conn.execute("UPDATE classification_system SET node_count=$1 WHERE id='sitc_rev4'", count)

    for sitc_sec, hs_chap in HS_CROSSWALK.items():
        for s_sys, s_code, t_sys, t_code in [
            ("sitc_rev4", sitc_sec, "hs_2022", hs_chap),
            ("hs_2022", hs_chap, "sitc_rev4", sitc_sec),
        ]:
            await conn.execute(
                """INSERT INTO equivalence (source_system, source_code, target_system, target_code, match_type)
                   VALUES ($1, $2, $3, $4, 'broad') ON CONFLICT DO NOTHING""",
                s_sys, s_code, t_sys, t_code,
            )

    print(f"  {count} SITC Rev 4 nodes ingested")
    return count
