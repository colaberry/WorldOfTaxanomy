"""Medicare Severity Diagnosis Related Groups FY2024 ingester.

Medicare Severity Diagnosis Related Groups (MS-DRG) - CMS FY2024.
Payment classification for Medicare inpatient hospital stays.
Two-level structure: 26 Major Diagnostic Categories (MDCs) plus selected DRGs.
Source: CMS.gov - ICD-10 MS-DRG grouper, public domain US government work.
"""
from __future__ import annotations

MDC_LIST: list[tuple[str, str]] = [
    ("MDC00", "Pre-MDC (Transplants, ECMO, Tracheostomy)"),
    ("MDC01", "Diseases and Disorders of the Nervous System"),
    ("MDC02", "Diseases and Disorders of the Eye"),
    ("MDC03", "Ear, Nose, Mouth, Throat Diseases and Disorders"),
    ("MDC04", "Diseases and Disorders of the Respiratory System"),
    ("MDC05", "Diseases and Disorders of the Circulatory System"),
    ("MDC06", "Diseases and Disorders of the Digestive System"),
    ("MDC07", "Diseases and Disorders of the Hepatobiliary System and Pancreas"),
    ("MDC08", "Diseases and Disorders of the Musculoskeletal System and Connective Tissue"),
    ("MDC09", "Diseases and Disorders of the Skin, Subcutaneous Tissue and Breast"),
    ("MDC10", "Endocrine, Nutritional and Metabolic Diseases and Disorders"),
    ("MDC11", "Diseases and Disorders of the Kidney and Urinary Tract"),
    ("MDC12", "Diseases and Disorders of the Male Reproductive System"),
    ("MDC13", "Diseases and Disorders of the Female Reproductive System"),
    ("MDC14", "Pregnancy, Childbirth and the Puerperium"),
    ("MDC15", "Newborns and Other Neonates with Conditions Originating in the Perinatal Period"),
    ("MDC16", "Diseases and Disorders of the Blood and Blood Forming Organs and Immunological Disorders"),
    ("MDC17", "Myeloproliferative Diseases and Disorders, Poorly Differentiated Neoplasms"),
    ("MDC18", "Infectious and Parasitic Diseases, Systemic or Unspecified Sites"),
    ("MDC19", "Mental Diseases and Disorders"),
    ("MDC20", "Alcohol and Drug Use and Alcohol and Drug Induced Organic Mental Disorders"),
    ("MDC21", "Injuries, Poisonings and Toxic Effects of Drugs"),
    ("MDC22", "Burns"),
    ("MDC23", "Factors Influencing Health Status and Other Contacts with Health Services"),
    ("MDC24", "Multiple Significant Trauma"),
    ("MDC25", "Human Immunodeficiency Virus Infections"),
]

DRG_EXAMPLES: list[tuple[str, str, str]] = [
    ("001", "Heart Transplant or Implant of Heart Assist System with MCC", "MDC00"),
    ("002", "Heart Transplant or Implant of Heart Assist System without MCC", "MDC00"),
    ("003", "ECMO or Tracheostomy with MV >96 Hours or PDX Exc Face/Mouth/Neck", "MDC00"),
    ("020", "Intracranial Vascular Procedures with PDX Hemorrhage with MCC", "MDC01"),
    ("023", "Craniotomy with Major Device Implant or Acute CNS PDX with MCC", "MDC01"),
    ("061", "Ischemic Stroke, Precerebral Occlusion or TIA with Thrombolytic Agent MCC", "MDC01"),
    ("064", "Intracranial Hemorrhage or Cerebral Infarction with MCC", "MDC01"),
    ("163", "Major Chest Procedures with MCC", "MDC04"),
    ("177", "Respiratory Infections and Inflammations with MCC", "MDC04"),
    ("193", "Simple Pneumonia and Pleurisy with MCC", "MDC04"),
    ("207", "Respiratory System Diagnosis with Ventilator Support >96 Hours", "MDC04"),
    ("216", "Cardiac Valve and Other Major Cardiothoracic Procedures with Cardiac Catheterization MCC", "MDC05"),
    ("231", "Coronary Bypass with PTCA with MCC", "MDC05"),
    ("246", "Percutaneous Cardiovascular Procedures with Drug-Eluting Stent with MCC", "MDC05"),
    ("280", "Acute Myocardial Infarction, Discharged Alive with MCC", "MDC05"),
    ("291", "Heart Failure and Shock with MCC", "MDC05"),
    ("329", "Major Small and Large Bowel Procedures with MCC", "MDC06"),
    ("377", "GI Hemorrhage with MCC", "MDC06"),
    ("461", "Bilateral or Multiple Major Joint Procedures of Lower Extremity with MCC", "MDC08"),
    ("469", "Major Hip and Knee Joint Replacement or Reattachment of Lower Extremity with MCC", "MDC08"),
    ("470", "Major Hip and Knee Joint Replacement or Reattachment of Lower Extremity without MCC", "MDC08"),
    ("768", "Vaginal Delivery with Sterilization and/or D and C", "MDC14"),
    ("774", "Vaginal Delivery with Complicating Diagnoses", "MDC14"),
    ("807", "Vaginal Delivery without Complicating Diagnoses", "MDC14"),
]


async def ingest_ms_drg(conn) -> int:
    """Ingest MS-DRG.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "ms_drg",
        "MS-DRG",
        "Medicare Severity Diagnosis Related Groups FY2024",
        "FY2024",
        "United States",
        "Centers for Medicare and Medicaid Services (CMS)",
    )

    count = 0
    for seq, (code, title) in enumerate(MDC_LIST, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "ms_drg", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(DRG_EXAMPLES, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "ms_drg", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'ms_drg'",
        count,
    )

    return count
