"""Healthcare Common Procedure Coding System Level II ingester.

HCPCS Level II (Healthcare Common Procedure Coding System Level II).
CMS code set for products, supplies and services not included in CPT.
Two-level structure: 18 major sections and selected subsection ranges.
Source: CMS.gov HCPCS codes, released as public domain US government work.
"""
from __future__ import annotations

HCPCS_SECTIONS: list[tuple[str, str]] = [
    ("A", "Transportation Services Including Ambulance, Medical and Surgical Supplies"),
    ("B", "Enteral and Parenteral Therapy"),
    ("C", "Outpatient PPS"),
    ("D", "Dental Procedures"),
    ("E", "Durable Medical Equipment"),
    ("G", "Procedures and Professional Services (Temporary)"),
    ("H", "Alcohol and Drug Abuse Treatment Services"),
    ("J", "Drugs Administered Other Than Oral Method"),
    ("K", "Temporary Codes - Durable Medical Equipment"),
    ("L", "Orthotic and Prosthetic Procedures and Devices"),
    ("M", "Medical Services"),
    ("P", "Pathology and Laboratory Services"),
    ("Q", "Temporary Codes Assigned by CMS"),
    ("R", "Diagnostic Radiology Services"),
    ("S", "Temporary National Codes Non-Medicare"),
    ("T", "State Medicaid Agency Codes"),
    ("U", "Coronavirus Diagnostic Panel Codes"),
    ("V", "Vision, Hearing, Speech-Language Pathology and Audiology Services"),
]

HCPCS_SUBSECTIONS: list[tuple[str, str, str]] = [
    ("A0021", "Ambulance Services - Land", "A"),
    ("A0080", "Air Ambulance", "A"),
    ("A4000", "Medical and Surgical Supplies", "A"),
    ("A6000", "Wound Care Supplies", "A"),
    ("A9000", "Radiopharmaceuticals and Contrast Media", "A"),
    ("B4034", "Enteral Feeding Supplies", "B"),
    ("B4100", "Food Thickeners and Formula", "B"),
    ("B9000", "Parenteral Nutrition Solutions", "B"),
    ("D0100", "Diagnostic Dental Services", "D"),
    ("D1000", "Preventive Dental Services", "D"),
    ("D2000", "Restorative Dental Services", "D"),
    ("D3000", "Endodontic Dental Services", "D"),
    ("D4000", "Periodontic Dental Services", "D"),
    ("D5000", "Prosthodontics - Removable", "D"),
    ("D6000", "Prosthodontics - Fixed", "D"),
    ("D7000", "Oral and Maxillofacial Surgery", "D"),
    ("E0100", "Canes and Crutches", "E"),
    ("E0250", "Hospital Beds and Accessories", "E"),
    ("E0500", "IPPB Machines and Nebulizers", "E"),
    ("E0600", "Suction Pumps", "E"),
    ("E1000", "Wheelchair - Manual", "E"),
    ("E1230", "Power Wheelchairs", "E"),
    ("E1390", "Oxygen Equipment and Supplies", "E"),
    ("J0001", "Injectable Antibiotics", "J"),
    ("J0100", "Cardiovascular Drugs - Injectable", "J"),
    ("J1000", "Immunosuppressants - Injectable", "J"),
    ("J1440", "Filgrastim and Colony Stimulating Factors", "J"),
    ("J2000", "Oncology Drugs - Injectable", "J"),
    ("J3000", "Other Drugs - Injectable", "J"),
    ("J7000", "Biologics and Specialty Drugs", "J"),
    ("L0100", "Cervical Orthoses", "L"),
    ("L0300", "Thoracic Orthoses", "L"),
    ("L1000", "Ankle-Foot Orthoses", "L"),
    ("L1900", "Knee Orthoses", "L"),
    ("L3000", "Foot Inserts and Custom Orthotics", "L"),
    ("L5000", "Lower Limb Prostheses", "L"),
    ("L6000", "Upper Limb Prostheses", "L"),
    ("L7000", "Prosthetic Implants", "L"),
    ("V2000", "Lenses and Frames", "V"),
    ("V5000", "Hearing Aids and Accessories", "V"),
    ("V5010", "Cochlear Implants", "V"),
]


async def ingest_hcpcs_l2(conn) -> int:
    """Ingest HCPCS Level II.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "hcpcs_l2",
        "HCPCS Level II",
        "Healthcare Common Procedure Coding System Level II",
        "2024",
        "United States",
        "Centers for Medicare and Medicaid Services (CMS)",
    )

    count = 0
    for seq, (code, title) in enumerate(HCPCS_SECTIONS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "hcpcs_l2", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(HCPCS_SUBSECTIONS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "hcpcs_l2", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'hcpcs_l2'",
        count,
    )

    return count
