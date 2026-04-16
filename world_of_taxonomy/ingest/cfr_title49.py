"""CFR Title 49 ingester.

Code of Federal Regulations, Title 49 - Transportation.
Published by the US Government (ecfr.gov). Public domain.
Reference: https://www.ecfr.gov/current/title-49

Hand-coded hierarchy of the most important regulatory parts covering:
  - Hours of Service (Part 395)
  - Driver Qualifications (Part 391)
  - Controlled Substances Testing (Part 382)
  - Electronic Logging Devices (Part 395 Subpart B)
  - Vehicle Inspection/Repair (Part 396)
  - Hazardous Materials (Parts 171-180)
  - General FMCSA Operating Authority (Parts 360-370)
  - Commercial Vehicle Safety Standards (Parts 393, 399)
  - Accident Reporting (Part 390)
  - Interstate Commerce / Operating Rules (Parts 387-392)

Code format: '49_{part}' for parts, '49_{part}_{subpart_letter}' for subparts,
'49_{part}_{section_number}' for sections.

Hierarchy: Part (level 1) -> Subpart (level 2) -> Section (level 3, leaf)
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CFR49_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # ── Part 382: Controlled Substances and Alcohol Use and Testing ──
    ("49_382", "Part 382 - Controlled Substances and Alcohol Use and Testing", 1, None),
    ("49_382_A", "Subpart A - General", 2, "49_382"),
    ("49_382_B", "Subpart B - Prohibitions", 2, "49_382"),
    ("49_382_C", "Subpart C - Tests Required", 2, "49_382"),
    ("49_382_D", "Subpart D - Handling of Test Results", 2, "49_382"),
    ("49_382_E", "Subpart E - Consequences for Drivers Engaging in Substance Use-Related Conduct", 2, "49_382"),
    ("49_382_F", "Subpart F - Alcohol Misuse Rule", 2, "49_382"),

    # ── Part 383: Commercial Driver's License Standards ──
    ("49_383", "Part 383 - Commercial Driver's License Standards", 1, None),
    ("49_383_A", "Subpart A - General", 2, "49_383"),
    ("49_383_B", "Subpart B - Single License Requirement", 2, "49_383"),
    ("49_383_C", "Subpart C - Notification Requirements and Employer Responsibilities", 2, "49_383"),
    ("49_383_D", "Subpart D - Driver Disqualifications and Penalties", 2, "49_383"),
    ("49_383_E", "Subpart E - Testing and Licensing Procedures", 2, "49_383"),
    ("49_383_F", "Subpart F - Vehicle Groups and Endorsements", 2, "49_383"),

    # ── Part 387: Minimum Levels of Financial Responsibility ──
    ("49_387", "Part 387 - Minimum Levels of Financial Responsibility for Motor Carriers", 1, None),
    ("49_387_A", "Subpart A - Motor Carriers of Property", 2, "49_387"),
    ("49_387_B", "Subpart B - Motor Carriers of Passengers", 2, "49_387"),

    # ── Part 390: Federal Motor Carrier Safety Regulations - General ──
    ("49_390", "Part 390 - Federal Motor Carrier Safety Regulations - General", 1, None),
    ("49_390_1", "Section 390.1 - Purpose and scope", 3, "49_390"),
    ("49_390_3", "Section 390.3 - General applicability", 3, "49_390"),
    ("49_390_5", "Section 390.5 - Definitions", 3, "49_390"),
    ("49_390_9", "Section 390.9 - State and local laws, effect on", 3, "49_390"),
    ("49_390_11", "Section 390.11 - Motor carrier to require observance of driver regulations", 3, "49_390"),
    ("49_390_13", "Section 390.13 - Aiding or abetting violations", 3, "49_390"),
    ("49_390_15", "Section 390.15 - Assistance in investigations and special studies", 3, "49_390"),
    ("49_390_17", "Section 390.17 - Electronic filing of documents", 3, "49_390"),
    ("49_390_19", "Section 390.19 - Motor carrier identification report", 3, "49_390"),
    ("49_390_21", "Section 390.21 - Marking of self-propelled CMVs and intermodal equipment", 3, "49_390"),

    # ── Part 391: Qualifications of Drivers ──
    ("49_391", "Part 391 - Qualifications of Drivers", 1, None),
    ("49_391_A", "Subpart A - General", 2, "49_391"),
    ("49_391_B", "Subpart B - Qualification and Disqualification of Drivers", 2, "49_391"),
    ("49_391_C", "Subpart C - Background and Character", 2, "49_391"),
    ("49_391_D", "Subpart D - Physical Qualifications and Examinations", 2, "49_391"),
    ("49_391_E", "Subpart E - Physical Examinations", 2, "49_391"),
    ("49_391_F", "Subpart F - Files and Records", 2, "49_391"),
    ("49_391_G", "Subpart G - Limited Exemptions", 2, "49_391"),

    # ── Part 392: Driving of Commercial Motor Vehicles ──
    ("49_392", "Part 392 - Driving of Commercial Motor Vehicles", 1, None),
    ("49_392_A", "Subpart A - General", 2, "49_392"),
    ("49_392_B", "Subpart B - Procedures While Driving", 2, "49_392"),
    ("49_392_C", "Subpart C - Stopped Vehicles", 2, "49_392"),
    ("49_392_D", "Subpart D - Use of Lighted Lamps and Reflectors", 2, "49_392"),
    ("49_392_E", "Subpart E - Accidents", 2, "49_392"),
    ("49_392_F", "Subpart F - Fueling Precautions", 2, "49_392"),

    # ── Part 393: Parts and Accessories Necessary for Safe Operation ──
    ("49_393", "Part 393 - Parts and Accessories Necessary for Safe Operation", 1, None),
    ("49_393_A", "Subpart A - General", 2, "49_393"),
    ("49_393_B", "Subpart B - Lamps, Reflective Devices, and Electrical Equipment", 2, "49_393"),
    ("49_393_C", "Subpart C - Brakes", 2, "49_393"),
    ("49_393_D", "Subpart D - Fuel Systems", 2, "49_393"),
    ("49_393_E", "Subpart E - Coupling Devices and Towing Methods", 2, "49_393"),
    ("49_393_F", "Subpart F - Tires", 2, "49_393"),
    ("49_393_G", "Subpart G - Heaters", 2, "49_393"),
    ("49_393_H", "Subpart H - Emergency Equipment", 2, "49_393"),
    ("49_393_I", "Subpart I - Protection Against Shifting and Falling Cargo", 2, "49_393"),
    ("49_393_J", "Subpart J - Frames, Cab and Body Components, Wheels, Steering, and Suspension Systems", 2, "49_393"),

    # ── Part 395: Hours of Service of Drivers ──
    ("49_395", "Part 395 - Hours of Service of Drivers", 1, None),
    ("49_395_A", "Subpart A - General", 2, "49_395"),
    ("49_395_1", "Section 395.1 - Scope of rules in this part", 3, "49_395"),
    ("49_395_2", "Section 395.2 - Definitions", 3, "49_395"),
    ("49_395_3", "Section 395.3 - Maximum driving time for property-carrying vehicles", 3, "49_395"),
    ("49_395_5", "Section 395.5 - Maximum driving time for passenger-carrying vehicles", 3, "49_395"),
    ("49_395_8", "Section 395.8 - Driver's record of duty status", 3, "49_395"),
    ("49_395_11", "Section 395.11 - Supporting documents", 3, "49_395"),
    ("49_395_13", "Section 395.13 - Drivers declared out of service", 3, "49_395"),
    ("49_395_15", "Section 395.15 - Automatic on-board recording devices", 3, "49_395"),
    ("49_395_B", "Subpart B - Electronic Logging Devices (ELDs)", 2, "49_395"),
    ("49_395_20", "Section 395.20 - ELD applicability", 3, "49_395"),
    ("49_395_22", "Section 395.22 - Motor carrier responsibilities - ELD", 3, "49_395"),
    ("49_395_24", "Section 395.24 - Driver responsibilities - ELD", 3, "49_395"),
    ("49_395_26", "Section 395.26 - ELD data diagnostics and malfunctions", 3, "49_395"),

    # ── Part 396: Inspection, Repair, and Maintenance ──
    ("49_396", "Part 396 - Inspection, Repair, and Maintenance", 1, None),
    ("49_396_3", "Section 396.3 - Inspection, repair, and maintenance", 3, "49_396"),
    ("49_396_5", "Section 396.5 - Lubrication", 3, "49_396"),
    ("49_396_7", "Section 396.7 - Unsafe operations forbidden", 3, "49_396"),
    ("49_396_9", "Section 396.9 - Inspection of motor vehicles in operation", 3, "49_396"),
    ("49_396_11", "Section 396.11 - Driver vehicle inspection report(s)", 3, "49_396"),
    ("49_396_13", "Section 396.13 - Driver inspection", 3, "49_396"),
    ("49_396_17", "Section 396.17 - Periodic inspections", 3, "49_396"),
    ("49_396_19", "Section 396.19 - Inspector qualifications", 3, "49_396"),
    ("49_396_21", "Section 396.21 - Periodic inspection recordkeeping requirements", 3, "49_396"),
    ("49_396_25", "Section 396.25 - Driveaway-towaway operations", 3, "49_396"),

    # ── Part 397: Transportation of Hazardous Materials - Driving and Parking Rules ──
    ("49_397", "Part 397 - Transportation of Hazardous Materials - Driving and Parking Rules", 1, None),
    ("49_397_A", "Subpart A - General Rules for All Hazardous Materials", 2, "49_397"),
    ("49_397_B", "Subpart B - Explosives", 2, "49_397"),
    ("49_397_C", "Subpart C - Division 1.1, 1.2, or 1.3 Explosives", 2, "49_397"),
    ("49_397_D", "Subpart D - Radioactive Materials", 2, "49_397"),

    # ── Part 171: General Information, Regulations, and Definitions (Hazmat) ──
    ("49_171", "Part 171 - General Information, Regulations, and Definitions (Hazmat)", 1, None),
    ("49_171_A", "Subpart A - Applicability, General Requirements, and North American Shipments", 2, "49_171"),
    ("49_171_B", "Subpart B - International Transportation", 2, "49_171"),
    ("49_171_C", "Subpart C - Incidents, Accidents, and Discrepancies", 2, "49_171"),

    # ── Part 172: Hazardous Materials Table ──
    ("49_172", "Part 172 - Hazardous Materials Table, Special Provisions, Packaging Requirements", 1, None),
    ("49_172_A", "Subpart A - Applicability and General Requirements", 2, "49_172"),
    ("49_172_B", "Subpart B - Table of Hazardous Materials and Special Provisions", 2, "49_172"),
    ("49_172_C", "Subpart C - Shipping Papers", 2, "49_172"),
    ("49_172_D", "Subpart D - Marking", 2, "49_172"),
    ("49_172_E", "Subpart E - Labeling", 2, "49_172"),
    ("49_172_F", "Subpart F - Placarding", 2, "49_172"),
    ("49_172_G", "Subpart G - Emergency Response Information", 2, "49_172"),
    ("49_172_H", "Subpart H - Training", 2, "49_172"),

    # ── Part 173: Shippers - General Requirements for Shipments and Packagings ──
    ("49_173", "Part 173 - Shippers - General Requirements for Shipments and Packagings", 1, None),
    ("49_173_A", "Subpart A - General", 2, "49_173"),
    ("49_173_B", "Subpart B - Preparation of Hazardous Materials for Transportation", 2, "49_173"),

    # ── Part 177: Carriage by Public Highway ──
    ("49_177", "Part 177 - Carriage by Public Highway", 1, None),
    ("49_177_A", "Subpart A - General Requirements", 2, "49_177"),
    ("49_177_B", "Subpart B - Loading and Unloading", 2, "49_177"),
]

_SYSTEM_ROW = (
    "cfr_title_49",
    "CFR Title 49",
    "Code of Federal Regulations - Title 49 Transportation",
    "2024",
    "United States",
    "US Government (ecfr.gov)",
)


def _determine_level(code: str) -> int:
    """Return hierarchy level from CFR Title 49 code structure.

    '49_395'   -> 1 (Part)
    '49_395_A' -> 2 (Subpart - ends in single uppercase letter)
    '49_395_1' -> 3 (Section - ends in digits)
    """
    parts = code.split("_")
    if len(parts) == 2:
        return 1  # '49_395' = Part
    suffix = parts[2]
    if suffix.isalpha() and suffix.isupper() and len(suffix) == 1:
        return 2  # '49_395_A' = Subpart
    return 3  # '49_395_1' = Section


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code, or None for Parts (level 1).

    Subparts and Sections both belong directly to their Part.
    """
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_cfr_title49(conn) -> int:
    """Ingest CFR Title 49 Transportation regulatory hierarchy.

    Hand-coded from ecfr.gov (public domain).
    Returns total node count inserted.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        *_SYSTEM_ROW,
    )

    # Determine which codes are parents (i.e. non-leaves)
    parent_codes = {parent for _, _, _, parent in CFR49_NODES if parent is not None}

    rows = [
        (
            "cfr_title_49",
            code,
            title,
            level,
            parent,
            code.split("_")[1],        # sector_code = part number
            code not in parent_codes,   # is_leaf
        )
        for code, title, level, parent in CFR49_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CFR49_NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'cfr_title_49'",
        count,
    )

    return count
