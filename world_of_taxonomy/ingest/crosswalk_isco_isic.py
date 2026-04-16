"""ISCO-08 / ISIC Rev 4 crosswalk.

Maps ISCO-08 major occupation groups (1-digit) to ISIC Rev 4 sections
(1-letter, A-U) where those occupations predominantly work.

Based on ILO guidance on occupation-industry relationships and the
ILO/UN joint framework for economic statistics.
Hand-coded ~45 edges.

(isco_code, isic_code, match_type, note)
"""
from __future__ import annotations

from typing import Optional

# ISCO-08 major groups:
#   1 - Managers
#   2 - Professionals
#   3 - Technicians and Associate Professionals
#   4 - Clerical Support Workers
#   5 - Service and Sales Workers
#   6 - Skilled Agricultural, Forestry and Fishery Workers
#   7 - Craft and Related Trades Workers
#   8 - Plant and Machine Operators and Assemblers
#   9 - Elementary Occupations
#   0 - Armed Forces Occupations

# ISIC Rev 4 sections:
#   A - Agriculture, Forestry and Fishing
#   B - Mining and Quarrying
#   C - Manufacturing
#   D - Electricity, Gas, Steam and Air Conditioning Supply
#   E - Water Supply, Sewerage, Waste Management
#   F - Construction
#   G - Wholesale and Retail Trade
#   H - Transportation and Storage
#   I - Accommodation and Food Service Activities
#   J - Information and Communication
#   K - Financial and Insurance Activities
#   L - Real Estate Activities
#   M - Professional, Scientific and Technical Activities
#   N - Administrative and Support Service Activities
#   O - Public Administration and Defence
#   P - Education
#   Q - Human Health and Social Work Activities
#   R - Arts, Entertainment and Recreation
#   S - Other Service Activities
#   T - Activities of Households (domestic workers)
#   U - Activities of Extraterritorial Organizations

# (isco_code, isic_code, match_type, note)
ISCO_ISIC_EDGES: list[tuple[str, str, str, Optional[str]]] = [
    # Managers (1) - cross-industry leadership roles
    ("1", "K", "broad", "Financial services managers"),
    ("1", "M", "broad", "Professional services and consulting managers"),
    ("1", "O", "broad", "Public administration managers"),
    ("1", "P", "broad", "Education administrators"),
    ("1", "Q", "broad", "Healthcare facility managers"),
    ("1", "C", "broad", "Manufacturing plant managers"),
    ("1", "G", "narrow", "Retail and wholesale trade managers"),

    # Professionals (2) - knowledge-intensive industries
    ("2", "M", "broad", "Engineers, scientists, consultants in professional services"),
    ("2", "J", "broad", "ICT professionals, software developers"),
    ("2", "K", "broad", "Financial analysts, actuaries, economists"),
    ("2", "P", "broad", "Academics, teachers, education professionals"),
    ("2", "Q", "broad", "Medical doctors, nurses, clinical professionals"),
    ("2", "O", "narrow", "Government policy analysts and legal professionals"),

    # Technicians and Associate Professionals (3)
    ("3", "C", "broad", "Manufacturing engineering technicians"),
    ("3", "F", "broad", "Construction and building technicians"),
    ("3", "J", "narrow", "ICT support and network technicians"),
    ("3", "Q", "narrow", "Medical and diagnostic technicians"),
    ("3", "B", "narrow", "Mining and petroleum technicians"),
    ("3", "D", "narrow", "Utilities and power systems technicians"),

    # Clerical Support Workers (4)
    ("4", "K", "broad", "Bank clerks, insurance clerks, accounting clerks"),
    ("4", "O", "broad", "Government administrative and clerical workers"),
    ("4", "N", "broad", "Business support and office admin services"),
    ("4", "M", "narrow", "Legal secretaries and professional office staff"),

    # Service and Sales Workers (5)
    ("5", "I", "broad", "Hotel, restaurant and hospitality workers"),
    ("5", "G", "broad", "Retail sales assistants and shop workers"),
    ("5", "Q", "narrow", "Community care, home care and support workers"),
    ("5", "O", "narrow", "Police, firefighters and protective service workers"),
    ("5", "R", "narrow", "Sports instructors, tour guides, entertainers"),
    ("5", "S", "narrow", "Hairdressers and personal service workers"),

    # Skilled Agricultural, Forestry and Fishery Workers (6)
    ("6", "A", "broad", "Farmers, farm workers, foresters, fishers"),

    # Craft and Related Trades Workers (7)
    ("7", "F", "broad", "Construction trades: carpenters, plumbers, electricians"),
    ("7", "C", "broad", "Manufacturing craftsmen: metal workers, machinery mechanics"),
    ("7", "B", "narrow", "Mining and extraction trades workers"),
    ("7", "D", "narrow", "Electrical installation trades in utilities"),

    # Plant and Machine Operators and Assemblers (8)
    ("8", "C", "broad", "Assembly line and factory machine operators"),
    ("8", "A", "narrow", "Agricultural machinery operators"),
    ("8", "H", "broad", "Truck drivers, train drivers, ship operators"),
    ("8", "B", "narrow", "Mining machine operators and drillers"),

    # Elementary Occupations (9)
    ("9", "A", "broad", "Agricultural labourers and farm hands"),
    ("9", "C", "narrow", "Factory and production line labourers"),
    ("9", "F", "narrow", "Construction labourers"),
    ("9", "N", "broad", "Cleaning, waste collection, and support labourers"),
    ("9", "H", "narrow", "Transport and logistics handlers"),

    # Armed Forces (0)
    ("0", "O", "broad", "Military and armed forces under public administration"),
]


async def ingest_crosswalk_isco_isic(conn) -> int:
    """Ingest ISCO-08 / ISIC Rev 4 crosswalk edges.

    Hand-coded based on ILO guidance on occupation-industry relationships.
    Returns count of edges inserted.
    """
    rows = [
        ("isco_08", isco_code, "isic_rev4", isic_code, match_type, note or "")
        for isco_code, isic_code, match_type, note in ISCO_ISIC_EDGES
    ]

    await conn.executemany(
        """INSERT INTO equivalence
               (source_system, source_code, target_system, target_code, match_type, notes)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
        rows,
    )

    return len(rows)
