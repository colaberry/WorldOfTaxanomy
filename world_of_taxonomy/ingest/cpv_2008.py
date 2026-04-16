"""CPV 2008 (EU Common Procurement Vocabulary) ingester.

The Common Procurement Vocabulary (CPV) is the EU reference nomenclature
for public procurement, established by Regulation (EC) No 213/2008.
This ingester stores ~45 main divisions and ~55 selected groups.

Source: EU Publications Office / TED (Tenders Electronic Daily)
License: European Union open data (reuse permitted)
"""
from __future__ import annotations

CPV_DIVISIONS: list[tuple[str, str]] = [
    ('03', 'Agricultural, farming, fishing, forestry and related products'),
    ('09', 'Petroleum products, fuel, electricity and other energy sources'),
    ('14', 'Mining, basic metals and related products'),
    ('15', 'Food, beverages, tobacco and related products'),
    ('16', 'Agricultural machinery'),
    ('18', 'Clothing, footwear, luggage articles and accessories'),
    ('19', 'Leather and textile fabrics, plastic and rubber materials'),
    ('22', 'Printed matter and related products'),
    ('24', 'Chemical products'),
    ('30', 'Office and computing machinery, equipment and supplies'),
    ('31', 'Electrical machinery, apparatus, equipment and consumables'),
    ('32', 'Radio, television, communication, telecommunication equipment'),
    ('33', 'Medical equipments, pharmaceuticals and personal care products'),
    ('34', 'Transport equipment and auxiliary products to transportation'),
    ('35', 'Security, fire-fighting, police and defence equipment'),
    ('37', 'Musical instruments, sport goods, games, toys, handicraft, art materials'),
    ('38', 'Laboratory, optical and precision equipments (excl. glasses)'),
    ('39', 'Furniture (incl. office furniture), furnishings, domestic appliances'),
    ('41', 'Collected and purified water'),
    ('42', 'Industrial machinery'),
    ('43', 'Machinery for mining, quarrying, construction equipment'),
    ('44', 'Construction structures and materials; auxiliary products to construction'),
    ('45', 'Construction work'),
    ('48', 'Software package and information systems'),
    ('50', 'Repair and maintenance services'),
    ('51', 'Installation services (except software)'),
    ('55', 'Hotel, restaurant and retail trade services'),
    ('60', 'Transport services (excl. waste transport)'),
    ('63', 'Supporting and auxiliary transport services; travel agencies services'),
    ('64', 'Postal and telecommunications services'),
    ('65', 'Public utilities'),
    ('66', 'Financial and insurance services'),
    ('70', 'Real estate services'),
    ('71', 'Architectural, construction, engineering and inspection services'),
    ('72', 'IT services: consulting, software development, Internet and support'),
    ('73', 'Research and development services and related consultancy services'),
    ('75', 'Administration, defence and social security services'),
    ('76', 'Services related to the oil and gas industry'),
    ('77', 'Agricultural, forestry, horticultural, aquacultural and apicultural services'),
    ('79', 'Business services: law, marketing, consulting, recruitment, printing, security'),
    ('80', 'Education and training services'),
    ('85', 'Health and social work services'),
    ('90', 'Sewage, refuse, cleaning and environmental services'),
    ('92', 'Recreational, cultural and sporting services'),
    ('98', 'Other community, social and personal services'),
]

CPV_GROUPS: list[tuple[str, str, str]] = [
    ('03100000', 'Agricultural and horticultural products', '03'),
    ('03200000', 'Cereals and potatoes', '03'),
    ('03300000', 'Fishery and aquaculture products', '03'),
    ('03400000', 'Forestry and logging products', '03'),
    ('09100000', 'Fuels', '09'),
    ('09200000', 'Petroleum products, coal and oil products', '09'),
    ('09300000', 'Electricity, heating, solar and nuclear energy', '09'),
    ('15100000', 'Animal products, meat and meat products', '15'),
    ('15300000', 'Fruit, vegetables and related products', '15'),
    ('15500000', 'Dairy produce', '15'),
    ('15600000', 'Grain mill products', '15'),
    ('15800000', 'Miscellaneous food products', '15'),
    ('24100000', 'Gases', '24'),
    ('24200000', 'Dyes and pigments', '24'),
    ('24300000', 'Basic inorganic chemicals', '24'),
    ('24400000', 'Fertilisers and nitrogen compounds', '24'),
    ('24500000', 'Plastics in primary forms', '24'),
    ('24900000', 'Specialty chemicals', '24'),
    ('30100000', 'Office machinery, equipment and supplies', '30'),
    ('30200000', 'Computer equipment and supplies', '30'),
    ('30232000', 'Peripheral equipment (printers, monitors, storage)', '30'),
    ('33100000', 'Medical equipment', '33'),
    ('33200000', 'Medical devices, instruments, supplies', '33'),
    ('33600000', 'Pharmaceutical products', '33'),
    ('33700000', 'Personal care products', '33'),
    ('45100000', 'Site preparation work', '45'),
    ('45200000', 'Complete or part construction work and civil engineering', '45'),
    ('45300000', 'Building installation work', '45'),
    ('45400000', 'Building completion work', '45'),
    ('48100000', 'Industry specific software packages', '48'),
    ('48200000', 'Network and internet software package', '48'),
    ('48300000', 'Document creation, drawing, imaging and scheduling software', '48'),
    ('48400000', 'Transaction and business software packages', '48'),
    ('48500000', 'Communication and multimedia software packages', '48'),
    ('48600000', 'Database and operating software packages', '48'),
    ('71200000', 'Architectural and related services', '71'),
    ('71300000', 'Engineering services', '71'),
    ('71500000', 'Construction-related services', '71'),
    ('72200000', 'Software programming and consultancy services', '72'),
    ('72300000', 'Data services', '72'),
    ('72400000', 'Internet services', '72'),
    ('72500000', 'Computer-related services', '72'),
    ('72600000', 'Computer support and consultancy services', '72'),
    ('80100000', 'Primary education services', '80'),
    ('80200000', 'Secondary education services', '80'),
    ('80300000', 'Higher education services', '80'),
    ('80400000', 'Adult and other education services', '80'),
    ('80500000', 'Training services', '80'),
    ('85100000', 'Health services', '85'),
    ('85200000', 'Veterinary services', '85'),
    ('85300000', 'Social work and related services', '85'),
]


async def ingest_cpv_2008(conn) -> int:
    """Ingest CPV 2008 into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "cpv_2008",
        "CPV 2008",
        "EU Common Procurement Vocabulary 2008",
        "2008",
        "European Union",
        "EU Publications Office",
    )

    count = 0
    for seq, (code, title) in enumerate(CPV_DIVISIONS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "cpv_2008", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(CPV_GROUPS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "cpv_2008", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'cpv_2008'",
        count,
    )

    return count
