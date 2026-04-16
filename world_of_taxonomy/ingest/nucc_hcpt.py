"""NUCC Health Care Provider Taxonomy ingester.

NUCC Health Care Provider Taxonomy (HCPT) used in NPI registration.
Two-level structure: provider groupings and classifications.
Source: NUCC (nucc.org), released under public domain.
Used in CMS NPI registry and 837 healthcare transactions.
"""
from __future__ import annotations

NPI_GROUPINGS: list[tuple[str, str]] = [
    ("A", "Allopathic and Osteopathic Physicians"),
    ("B", "Behavioral Health and Social Service Providers"),
    ("C", "Chiropractic Providers"),
    ("D", "Dental Providers"),
    ("E", "Dietary and Nutritional Service Providers"),
    ("F", "Emergency Medical Service Providers"),
    ("G", "Eye and Vision Services Providers"),
    ("H", "Group"),
    ("I", "Hospital Units"),
    ("J", "Laboratories"),
    ("K", "Long Term Care Facilities"),
    ("L", "Managed Care Organizations"),
    ("M", "Mass Immunization Roster Billers"),
    ("N", "Nursing Service Providers"),
    ("O", "Nursing Service Related Providers"),
    ("P", "Other Service Providers"),
    ("Q", "Pharmacy Service Providers"),
    ("R", "Physician Assistants and Advanced Practice Nursing Providers"),
    ("S", "Podiatric Medicine and Surgery Service Providers"),
    ("T", "Respiratory, Developmental, Rehabilitative and Restorative"),
    ("U", "Speech, Language and Hearing Service Providers"),
    ("V", "Student, Health Care"),
    ("W", "Technologists, Technicians and Other Technical Service Providers"),
    ("X", "Hospitals"),
    ("Y", "Residential Treatment Facilities"),
]

NPI_CLASSIFICATIONS: list[tuple[str, str, str]] = [
    ("A01", "Allergy and Immunology", "A"),
    ("A02", "Anesthesiology", "A"),
    ("A03", "Colon and Rectal Surgery", "A"),
    ("A04", "Dermatology", "A"),
    ("A05", "Emergency Medicine", "A"),
    ("A06", "Family Medicine", "A"),
    ("A07", "Internal Medicine", "A"),
    ("A08", "Medical Genetics", "A"),
    ("A09", "Neurological Surgery", "A"),
    ("A10", "Neurology", "A"),
    ("A11", "Nuclear Medicine", "A"),
    ("A12", "Obstetrics and Gynecology", "A"),
    ("A13", "Ophthalmology", "A"),
    ("A14", "Orthopedic Surgery", "A"),
    ("A15", "Otolaryngology", "A"),
    ("A16", "Pathology", "A"),
    ("A17", "Pediatrics", "A"),
    ("A18", "Physical Medicine and Rehabilitation", "A"),
    ("A19", "Plastic Surgery", "A"),
    ("A20", "Preventive Medicine", "A"),
    ("A21", "Psychiatry and Neurology", "A"),
    ("A22", "Radiology", "A"),
    ("A23", "Surgery", "A"),
    ("A24", "Thoracic Surgery", "A"),
    ("A25", "Urology", "A"),
    ("B01", "Counselor", "B"),
    ("B02", "Marriage and Family Therapist", "B"),
    ("B03", "Mental Health Counselor", "B"),
    ("B04", "Psychologist", "B"),
    ("B05", "Social Worker", "B"),
    ("B06", "Substance Use Disorder Counselor", "B"),
    ("C01", "Chiropractor", "C"),
    ("D01", "Dentist - General Practice", "D"),
    ("D02", "Oral and Maxillofacial Surgery", "D"),
    ("D03", "Orthodontics and Dentofacial Orthopedics", "D"),
    ("D04", "Periodontics", "D"),
    ("D05", "Endodontics", "D"),
    ("D06", "Pediatric Dentistry", "D"),
    ("D07", "Prosthodontics", "D"),
    ("E01", "Registered Dietitian or Nutrition Professional", "E"),
    ("F01", "Emergency Medical Technician - Basic", "F"),
    ("F02", "Emergency Medical Technician - Paramedic", "F"),
    ("F03", "Emergency Medical Technician - Advanced", "F"),
    ("G01", "Ophthalmologist", "G"),
    ("G02", "Optometrist", "G"),
    ("G03", "Optician", "G"),
    ("N01", "Registered Nurse", "N"),
    ("N02", "Licensed Practical Nurse", "N"),
    ("N03", "Nurse Practitioner", "N"),
    ("N04", "Clinical Nurse Specialist", "N"),
    ("Q01", "Pharmacist", "Q"),
    ("Q02", "Pharmacy Technician", "Q"),
    ("R01", "Physician Assistant", "R"),
    ("R02", "Certified Nurse-Midwife", "R"),
    ("R03", "Certified Registered Nurse Anesthetist", "R"),
    ("T01", "Occupational Therapist", "T"),
    ("T02", "Physical Therapist", "T"),
    ("U01", "Audiologist", "U"),
    ("U02", "Speech-Language Pathologist", "U"),
    ("X01", "General Acute Care Hospital", "X"),
    ("X02", "Critical Access Hospital", "X"),
    ("X03", "Children's Hospital", "X"),
    ("X04", "Psychiatric Hospital", "X"),
    ("X05", "Rehabilitation Hospital", "X"),
    ("X06", "Long Term Acute Care Hospital", "X"),
    ("X07", "Military Hospital", "X"),
    ("Y01", "Psychiatric Residential Treatment Facility", "Y"),
    ("Y02", "Substance Use Rehabilitation Facility", "Y"),
    ("Y03", "Community Based Residential Treatment Facility", "Y"),
]


async def ingest_nucc_hcpt(conn) -> int:
    """Ingest NUCC HCPT.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "nucc_hcpt",
        "NUCC HCPT",
        "NUCC Health Care Provider Taxonomy",
        "24.0",
        "United States",
        "National Uniform Claim Committee (NUCC)",
    )

    count = 0
    for seq, (code, title) in enumerate(NPI_GROUPINGS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "nucc_hcpt", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(NPI_CLASSIFICATIONS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "nucc_hcpt", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'nucc_hcpt'",
        count,
    )

    return count
