"""ANZSCO 2022 / ANZSIC 2006 crosswalk.

Maps ANZSCO 2022 occupation major groups to ANZSIC 2006 industry divisions
where those occupations predominantly work.

Based on ABS Labour Force Survey patterns and ABS guidance on
occupation-industry relationships.
Hand-coded ~45 edges linking ANZSCO major groups (1-digit) to
ANZSIC divisions (1-char letter).

(anzsco_code, anzsic_code, match_type, note)
"""
from __future__ import annotations

from typing import Optional

# ANZSCO 2022 major groups:
#   1 - Managers
#   2 - Professionals
#   3 - Technicians and Trades Workers
#   4 - Community and Personal Service Workers
#   5 - Clerical and Administrative Workers
#   6 - Sales Workers
#   7 - Machinery Operators and Drivers
#   8 - Labourers

# ANZSIC 2006 divisions (selected):
#   A - Agriculture, Forestry and Fishing
#   B - Mining
#   C - Manufacturing
#   D - Electricity, Gas, Water and Waste Services
#   E - Construction
#   F - Wholesale Trade
#   G - Retail Trade
#   H - Accommodation and Food Services
#   I - Transport, Postal and Warehousing
#   J - Information Media and Telecommunications
#   K - Financial and Insurance Services
#   L - Rental, Hiring and Real Estate Services
#   M - Professional, Scientific and Technical Services
#   N - Administrative and Support Services
#   O - Public Administration and Safety
#   P - Education and Training
#   Q - Health Care and Social Assistance
#   R - Arts and Recreation Services
#   S - Other Services

# (anzsco_code, anzsic_code, match_type, note)
ANZSCO_ANZSIC_EDGES: list[tuple[str, str, str, Optional[str]]] = [
    # Managers (1) work across all major industries
    ("1", "A", "broad", "Farm and agricultural managers"),
    ("1", "B", "broad", "Mining managers and site supervisors"),
    ("1", "C", "broad", "Manufacturing plant and operations managers"),
    ("1", "E", "broad", "Construction project and site managers"),
    ("1", "F", "broad", "Wholesale trade managers"),
    ("1", "G", "broad", "Retail store and chain managers"),
    ("1", "H", "broad", "Hospitality and accommodation managers"),
    ("1", "K", "broad", "Financial services and insurance managers"),
    ("1", "M", "broad", "Professional services managers and principals"),
    ("1", "O", "broad", "Government department and agency managers"),

    # Professionals (2) concentrate in knowledge industries
    ("2", "J", "broad", "ICT and media professionals"),
    ("2", "K", "broad", "Financial analysts, actuaries, economists"),
    ("2", "M", "broad", "Engineers, scientists, legal and accounting professionals"),
    ("2", "O", "broad", "Policy, regulatory and government professionals"),
    ("2", "P", "broad", "Teachers, academics, education professionals"),
    ("2", "Q", "broad", "Doctors, nurses, allied health professionals"),
    ("2", "B", "narrow", "Mining and petroleum engineers"),
    ("2", "C", "narrow", "Industrial and manufacturing engineers"),

    # Technicians and Trades Workers (3) in trades and technical roles
    ("3", "B", "broad", "Drillers, mining and petroleum technicians"),
    ("3", "C", "broad", "Engineering trades and manufacturing technicians"),
    ("3", "D", "broad", "Electricians and utilities technicians"),
    ("3", "E", "broad", "Construction trades: carpenters, plumbers, electricians"),
    ("3", "J", "narrow", "ICT support technicians"),
    ("3", "Q", "narrow", "Medical and allied health technicians"),

    # Community and Personal Service Workers (4)
    ("4", "H", "broad", "Hospitality workers: chefs, waiters, baristas"),
    ("4", "O", "broad", "Police, corrective services, emergency workers"),
    ("4", "P", "broad", "Education aides and childcare workers"),
    ("4", "Q", "broad", "Aged care, disability and community workers"),
    ("4", "R", "broad", "Sports and recreation workers, tour guides"),
    ("4", "S", "broad", "Personal service workers: hairdressers, beauticians"),

    # Clerical and Administrative Workers (5) across office-based industries
    ("5", "K", "broad", "Banking and insurance clerical workers"),
    ("5", "N", "broad", "Administrative and business support services"),
    ("5", "O", "broad", "Government clerical and administrative workers"),
    ("5", "M", "narrow", "Legal secretaries and accounting clerks"),

    # Sales Workers (6)
    ("6", "G", "broad", "Retail sales assistants and cashiers"),
    ("6", "F", "broad", "Wholesale trade representatives and agents"),
    ("6", "K", "narrow", "Insurance and financial products sales"),
    ("6", "L", "narrow", "Real estate sales agents"),

    # Machinery Operators and Drivers (7)
    ("7", "A", "broad", "Agricultural machinery operators"),
    ("7", "B", "broad", "Mining and extraction machine operators"),
    ("7", "C", "broad", "Manufacturing process and plant operators"),
    ("7", "E", "narrow", "Construction plant operators"),
    ("7", "I", "broad", "Truck drivers, bus drivers, couriers"),

    # Labourers (8) in physically intensive industries
    ("8", "A", "broad", "Farm hands and agricultural labourers"),
    ("8", "B", "broad", "Mining and quarrying labourers"),
    ("8", "C", "broad", "Factory and production line workers"),
    ("8", "E", "broad", "Construction and demolition labourers"),
    ("8", "I", "narrow", "Transport and warehousing handlers"),
]


async def ingest_crosswalk_anzsco_anzsic(conn) -> int:
    """Ingest ANZSCO 2022 / ANZSIC 2006 crosswalk edges.

    Hand-coded based on ABS Labour Force Survey patterns.
    Returns count of edges inserted.
    """
    rows = [
        ("anzsco_2022", anzsco_code, "anzsic_2006", anzsic_code, match_type, note or "")
        for anzsco_code, anzsic_code, match_type, note in ANZSCO_ANZSIC_EDGES
    ]

    await conn.executemany(
        """INSERT INTO equivalence
               (source_system, source_code, target_system, target_code, match_type, notes)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
        rows,
    )

    return len(rows)
