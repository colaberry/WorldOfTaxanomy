"""SASB SICS (Sustainable Industry Classification System) ingester.

Published by the IFRS Foundation (acquired SASB 2022).
Groups companies into 11 sectors and 77 industries based on
sustainability disclosure materiality.

Source: IFRS Foundation / SASB (sasb.org), publicly available sector taxonomy
License: Creative Commons Attribution 4.0 International
"""
from __future__ import annotations

SASB_SECTORS: list[tuple[str, str]] = [
    ("CG", "Consumer Goods"),
    ("EX", "Extractives and Minerals Processing"),
    ("FS", "Financials"),
    ("FC", "Food and Beverage"),
    ("HC", "Health Care"),
    ("IF", "Infrastructure"),
    ("RR", "Renewable Resources and Alternative Energy"),
    ("RE", "Resource Transformation"),
    ("SC", "Services"),
    ("TC", "Technology and Communications"),
    ("TR", "Transportation"),
]

SASB_INDUSTRIES: list[tuple[str, str, str]] = [
    ("CG-AA", "Apparel, Accessories and Footwear", "CG"),
    ("CG-BF", "Building Products and Furnishings", "CG"),
    ("CG-EC", "E-Commerce", "CG"),
    ("CG-HP", "Household and Personal Products", "CG"),
    ("CG-MR", "Multiline and Specialty Retailers and Distributors", "CG"),
    ("CG-TB", "Toys and Sporting Goods", "CG"),
    ("EX-CO", "Coal Operations", "EX"),
    ("EX-CM", "Construction Materials", "EX"),
    ("EX-EM", "Iron and Steel Producers", "EX"),
    ("EX-MM", "Metals and Mining", "EX"),
    ("EX-OG", "Oil and Gas - Exploration and Production", "EX"),
    ("EX-MI", "Oil and Gas - Midstream", "EX"),
    ("EX-RN", "Oil and Gas - Refining and Marketing", "EX"),
    ("EX-SE", "Oil and Gas - Services", "EX"),
    ("FS-AC", "Asset Management and Custody Activities", "FS"),
    ("FS-CB", "Commercial Banks", "FS"),
    ("FS-CF", "Consumer Finance", "FS"),
    ("FS-HF", "Mortgage Finance", "FS"),
    ("FS-IN", "Insurance", "FS"),
    ("FS-IB", "Investment Banking and Brokerage", "FS"),
    ("FS-MF", "Multiline and Specialty Insurance and Reinsurance", "FS"),
    ("FS-RE", "Real Estate", "FS"),
    ("FS-RT", "Real Estate Services", "FS"),
    ("FS-SB", "Security and Commodity Exchanges", "FS"),
    ("FC-FB", "Agricultural Products", "FC"),
    ("FC-AL", "Alcoholic Beverages", "FC"),
    ("FC-FP", "Food Retailers and Distributors", "FC"),
    ("FC-MF", "Meat, Poultry and Dairy", "FC"),
    ("FC-NB", "Non-Alcoholic Beverages", "FC"),
    ("FC-PF", "Processed Foods", "FC"),
    ("FC-RN", "Restaurants", "FC"),
    ("FC-TO", "Tobacco", "FC"),
    ("HC-BP", "Biotechnology and Pharmaceuticals", "HC"),
    ("HC-DI", "Drug Retailers", "HC"),
    ("HC-DV", "Medical Equipment and Supplies", "HC"),
    ("HC-HB", "Health Care Delivery", "HC"),
    ("HC-HM", "Health Care Distributors", "HC"),
    ("HC-MS", "Managed Care", "HC"),
    ("IF-AE", "Airport Services", "IF"),
    ("IF-EG", "Electric Utilities and Power Generators", "IF"),
    ("IF-EN", "Engineering and Construction Services", "IF"),
    ("IF-GU", "Gas Utilities and Distributors", "IF"),
    ("IF-HB", "Home Builders", "IF"),
    ("IF-RE", "Real Estate Owners, Developers and Investment Trusts", "IF"),
    ("IF-RR", "Rail Transportation", "IF"),
    ("IF-TL", "Telecommunications", "IF"),
    ("IF-WM", "Waste Management", "IF"),
    ("IF-WU", "Water Utilities and Services", "IF"),
    ("RR-BI", "Biofuels", "RR"),
    ("RR-FO", "Forestry Management", "RR"),
    ("RR-PP", "Pulp and Paper Products", "RR"),
    ("RR-SO", "Solar Technology and Project Developers", "RR"),
    ("RR-WI", "Wind Technology and Project Developers", "RR"),
    ("RE-CH", "Chemicals", "RE"),
    ("RE-CP", "Containers and Packaging", "RE"),
    ("RE-EE", "Electrical and Electronic Equipment", "RE"),
    ("RE-HC", "Industrial Machinery and Goods", "RE"),
    ("SC-ED", "Education", "SC"),
    ("SC-PS", "Professional and Commercial Services", "SC"),
    ("SC-HT", "Hotels and Lodging", "SC"),
    ("SC-LS", "Leisure Facilities", "SC"),
    ("SC-MS", "Media and Entertainment", "SC"),
    ("TC-HW", "Hardware", "TC"),
    ("TC-IG", "Internet Media and Services", "TC"),
    ("TC-SC", "Semiconductors", "TC"),
    ("TC-SI", "Software and IT Services", "TC"),
    ("TC-TL", "Telecommunication Services", "TC"),
    ("TR-AF", "Air Freight and Logistics", "TR"),
    ("TR-AL", "Airlines", "TR"),
    ("TR-AU", "Auto Parts", "TR"),
    ("TR-CA", "Car Rental and Leasing", "TR"),
    ("TR-CR", "Cruise Lines", "TR"),
    ("TR-MP", "Marine Transportation", "TR"),
    ("TR-RR", "Rail Transportation", "TR"),
    ("TR-RD", "Road Transportation", "TR"),
]


async def ingest_sasb_sics(conn) -> int:
    """Ingest SASB SICS into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "sasb_sics",
        "SASB SICS",
        "SASB Sustainable Industry Classification System",
        "2023",
        "Global",
        "IFRS Foundation / SASB",
    )

    count = 0
    for seq, (code, title) in enumerate(SASB_SECTORS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "sasb_sics", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(SASB_INDUSTRIES, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "sasb_sics", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'sasb_sics'",
        count,
    )

    return count
