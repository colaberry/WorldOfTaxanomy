"""ROME v4 (Repertoire Operationnel des Metiers et Emplois v4) ingester.

French operational directory of occupations and jobs, published by
France Travail (formerly Pole Emploi). Organizes ~11,000 occupations
into 14 professional families and 110 occupational domains.

This ingester stores the top two levels: 14 families (A-N) and
~78 occupational domains.

Source: France Travail (francetravail.fr), Rome v4 open data
License: Licence Ouverte / Open Licence (Etalab)
"""
from __future__ import annotations

FAMILIES: list[tuple[str, str]] = [
    ("A", "Agriculture and Fishing"),
    ("B", "Arts and Crafts"),
    ("C", "Banking, Insurance and Real Estate"),
    ("D", "Commerce, Retail and Sales"),
    ("E", "Communication, Media and Multimedia"),
    ("F", "Construction and Public Works"),
    ("G", "Tourism, Hospitality and Recreation"),
    ("H", "Industry"),
    ("I", "Installation, Maintenance and Repair"),
    ("J", "Healthcare, Social Work and Social Action"),
    ("K", "Information Services, Research and Administration"),
    ("L", "Culture, Entertainment and Sport"),
    ("M", "Administrative Management and Business Support"),
    ("N", "Transportation and Logistics"),
]

DOMAINS: list[tuple[str, str, str]] = [
    ("A11", "Agricultural Production - Large Crops and Viticulture", "A"),
    ("A12", "Agricultural Production - Livestock", "A"),
    ("A13", "Fishing and Aquaculture", "A"),
    ("A14", "Horticulture and Landscape", "A"),
    ("A15", "Forestry and Arboriculture", "A"),
    ("B11", "Art Craft Fabrication and Restoration", "B"),
    ("B12", "Luxury and Decorative Arts", "B"),
    ("B13", "Glass, Ceramics and Composites Crafts", "B"),
    ("B14", "Jewelry, Watchmaking and Precision Crafts", "B"),
    ("B15", "Textile, Clothing and Leather Crafts", "B"),
    ("B16", "Art and Antiques Trade", "B"),
    ("C11", "Banking, Finance and Financial Advisory", "C"),
    ("C12", "Insurance Operations and Risk Management", "C"),
    ("C13", "Real Estate and Property Management", "C"),
    ("C14", "Treasury, Audit and Financial Control", "C"),
    ("C15", "Actuarial Sciences and Statistics", "C"),
    ("D11", "Commerce in Large Distribution", "D"),
    ("D12", "Technical and Industrial Sales", "D"),
    ("D13", "Wholesale and International Trade", "D"),
    ("D14", "E-Commerce and Digital Commerce", "D"),
    ("D15", "Marketing and Commercial Promotion", "D"),
    ("D16", "Buying and Category Management", "D"),
    ("E11", "Journalism and Editorial", "E"),
    ("E12", "Advertising, Public Relations and Communication", "E"),
    ("E13", "Digital Publishing and Web Content", "E"),
    ("E14", "Audiovisual and Cinema Production", "E"),
    ("E15", "Photography and Imaging", "E"),
    ("F11", "Architecture, Urban Planning and Design", "F"),
    ("F12", "Civil Engineering and Infrastructure Works", "F"),
    ("F13", "Building Construction Trades", "F"),
    ("F14", "Finishing Trades and Interior Works", "F"),
    ("F15", "Energy and Technical Building Systems", "F"),
    ("F16", "Construction Management and Site Supervision", "F"),
    ("G11", "Hotel and Accommodation Management", "G"),
    ("G12", "Catering and Food Service", "G"),
    ("G13", "Tourism and Travel Services", "G"),
    ("G14", "Recreation, Theme Parks and Leisure Activities", "G"),
    ("G15", "Casino and Gaming Industry", "G"),
    ("H11", "Process and Industrial Engineering", "H"),
    ("H12", "Metallurgy, Mechanical Engineering and Manufacturing", "H"),
    ("H13", "Industrial Maintenance and Reliability", "H"),
    ("H14", "Chemical, Pharmaceutical and Biotechnology Industry", "H"),
    ("H15", "Aerospace and Defense Industry", "H"),
    ("H16", "Food and Beverage Manufacturing", "H"),
    ("H17", "Textiles, Leather and Composites Manufacturing", "H"),
    ("H18", "Production Management and Industrial Planning", "H"),
    ("I11", "Electrical and Electronics Installation and Maintenance", "I"),
    ("I12", "Mechanical, Industrial and Fluid Systems Maintenance", "I"),
    ("I13", "HVAC and Building Services Installation", "I"),
    ("I14", "IT and Telecommunications Infrastructure Maintenance", "I"),
    ("J11", "Medical and Surgical Care", "J"),
    ("J12", "Paramedical and Healthcare Support", "J"),
    ("J13", "Mental Health, Psychiatry and Psychology", "J"),
    ("J14", "Social Work and Family Services", "J"),
    ("J15", "Disability and Dependent Persons Services", "J"),
    ("J16", "Early Childhood Education and Care", "J"),
    ("K11", "Public Administration and Government Services", "K"),
    ("K12", "Human Resources and People Management", "K"),
    ("K13", "Research, Studies and Economic Analysis", "K"),
    ("K14", "Information, Documentation and Archives", "K"),
    ("K15", "Legal, Judicial and Compliance Services", "K"),
    ("L11", "Visual Arts, Graphic Arts and Design", "L"),
    ("L12", "Performing Arts - Acting, Dance, Music", "L"),
    ("L13", "Cultural Heritage, Museums and Libraries", "L"),
    ("L14", "Sports Instruction and Athletic Training", "L"),
    ("L15", "Animation and Social Recreation", "L"),
    ("M11", "Business Direction and General Management", "M"),
    ("M12", "Finance, Accounting and Budget Control", "M"),
    ("M13", "Administrative Secretarial and Office Support", "M"),
    ("M14", "Quality, Process and Performance Management", "M"),
    ("M15", "IT Systems Management and Project Management", "M"),
    ("M16", "Purchasing and Supply Chain Management", "M"),
    ("M17", "Corporate Communication and Public Affairs", "M"),
    ("N11", "Freight Transport by Road and Urban Logistics", "N"),
    ("N12", "Passenger Transport - Land and Urban", "N"),
    ("N13", "Air Transport Operations", "N"),
    ("N14", "Maritime and Fluvial Transport", "N"),
    ("N15", "Warehousing, Storage and Inventory Management", "N"),
    ("N16", "Port, Airport and Logistics Hub Operations", "N"),
]


async def ingest_rome_v4(conn) -> int:
    """Ingest ROME v4 into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "rome_v4",
        "ROME v4",
        "Repertoire Operationnel des Metiers et Emplois v4 (France)",
        "4",
        "France",
        "France Travail (ex-Pole Emploi)",
    )

    count = 0
    for seq, (code, title) in enumerate(FAMILIES, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "rome_v4", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(DOMAINS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "rome_v4", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'rome_v4'",
        count,
    )

    return count
