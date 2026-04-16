"""Classification of Individual Consumption According to Purpose (COICOP).

COICOP is the UN statistical classification for classifying individual
consumption expenditure by purpose. Used by national statistical offices
for national accounts, household budget surveys, and CPI construction.
COICOP-2018 (Rev 2018) has 14 divisions, 60 groups, and 168 classes.
"""
from __future__ import annotations

SYSTEM_ID = "coicop"

# (code, title, level, parent_code)
NODES: list[tuple] = [
    # Divisions (level 1)
    ("CP01", "Food and non-alcoholic beverages",                                           1, None),
    ("CP02", "Alcoholic beverages, tobacco and narcotics",                                 1, None),
    ("CP03", "Clothing and footwear",                                                      1, None),
    ("CP04", "Housing, water, electricity, gas and other fuels",                           1, None),
    ("CP05", "Furnishings, household equipment and routine maintenance of the dwelling",   1, None),
    ("CP06", "Health",                                                                     1, None),
    ("CP07", "Transport",                                                                  1, None),
    ("CP08", "Information and communication",                                              1, None),
    ("CP09", "Recreation, sport and culture",                                              1, None),
    ("CP10", "Education services",                                                         1, None),
    ("CP11", "Restaurants and accommodation services",                                     1, None),
    ("CP12", "Insurance and financial services",                                           1, None),
    ("CP13", "Personal care, social protection and miscellaneous goods and services",      1, None),
    ("CP14", "Expenditure of residents abroad and non-resident expenditure",               1, None),
    # Groups (level 2)
    ("CP011", "Food",                                                                      2, "CP01"),
    ("CP012", "Non-alcoholic beverages",                                                   2, "CP01"),
    ("CP021", "Alcoholic beverages",                                                       2, "CP02"),
    ("CP022", "Tobacco",                                                                   2, "CP02"),
    ("CP023", "Narcotics",                                                                 2, "CP02"),
    ("CP031", "Clothing",                                                                  2, "CP03"),
    ("CP032", "Footwear",                                                                  2, "CP03"),
    ("CP041", "Actual and imputed rentals for housing",                                    2, "CP04"),
    ("CP042", "Maintenance and repair of the dwelling",                                    2, "CP04"),
    ("CP043", "Water supply and related services",                                         2, "CP04"),
    ("CP044", "Electricity, gas and other fuels",                                          2, "CP04"),
    ("CP051", "Furniture, furnishings and carpets",                                        2, "CP05"),
    ("CP052", "Household textiles",                                                        2, "CP05"),
    ("CP053", "Household appliances",                                                      2, "CP05"),
    ("CP054", "Glassware, tableware and household utensils",                               2, "CP05"),
    ("CP055", "Tools and equipment for house and garden",                                  2, "CP05"),
    ("CP056", "Goods and services for routine household maintenance",                      2, "CP05"),
    ("CP061", "Pharmaceutical products",                                                   2, "CP06"),
    ("CP062", "Other medical products and equipment",                                      2, "CP06"),
    ("CP063", "Outpatient services",                                                       2, "CP06"),
    ("CP064", "Hospital services",                                                         2, "CP06"),
    ("CP071", "Purchase of vehicles",                                                      2, "CP07"),
    ("CP072", "Operation of personal transport equipment",                                 2, "CP07"),
    ("CP073", "Transport services",                                                        2, "CP07"),
    ("CP081", "Information and communication equipment",                                   2, "CP08"),
    ("CP082", "Telephone and facsimile services",                                          2, "CP08"),
    ("CP083", "Internet access services",                                                  2, "CP08"),
    ("CP091", "Audio-visual, photographic and information processing equipment",           2, "CP09"),
    ("CP092", "Other recreational goods and sports goods",                                 2, "CP09"),
    ("CP093", "Recreational and sporting services",                                        2, "CP09"),
    ("CP094", "Cultural services",                                                         2, "CP09"),
    ("CP095", "Newspapers, books and stationery",                                          2, "CP09"),
    ("CP096", "Package holidays and other package services",                               2, "CP09"),
    ("CP101", "Pre-primary and primary education services",                                2, "CP10"),
    ("CP102", "Secondary education services",                                              2, "CP10"),
    ("CP103", "Post-secondary non-tertiary education services",                            2, "CP10"),
    ("CP104", "Tertiary education services",                                               2, "CP10"),
    ("CP105", "Education services not elsewhere classified",                               2, "CP10"),
    ("CP111", "Food and non-alcoholic beverages serving services",                         2, "CP11"),
    ("CP112", "Accommodation services",                                                    2, "CP11"),
    ("CP121", "Life insurance",                                                            2, "CP12"),
    ("CP122", "Non-life insurance",                                                        2, "CP12"),
    ("CP123", "Financial intermediation services",                                         2, "CP12"),
    ("CP131", "Personal care services",                                                    2, "CP13"),
    ("CP132", "Social protection services",                                                2, "CP13"),
    ("CP133", "Other services not elsewhere classified",                                   2, "CP13"),
    ("CP141", "Expenditure of residents abroad",                                           2, "CP14"),
    ("CP142", "Non-resident expenditure in the domestic territory",                        2, "CP14"),
]


async def ingest_coicop(conn) -> int:
    """Ingest COICOP 2018 (Classification of Individual Consumption by Purpose)."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "COICOP 2018",
        "Classification of Individual Consumption According to Purpose - 2018 Revision",
        "Global (UN)",
        "2018",
        "United Nations Statistics Division (UNSD)",
        "#0EA5E9",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code[:4]
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
    print(f"  Ingested {count} COICOP 2018 codes")
    return count
