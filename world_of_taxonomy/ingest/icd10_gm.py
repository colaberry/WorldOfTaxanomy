"""ICD-10-GM (German Modification) skeleton ingester.

ICD-10-GM is the German modification of the International Classification
of Diseases, 10th Revision. Maintained by DIMDI / BfArM and updated annually.
Used for inpatient hospital billing (DRG system), outpatient coding (EBM),
and German health statistics. Structurally identical to ICD-10 at chapter level
but with German-specific subcategory codes (extension codes .G, .Z, .A, .V).
This skeleton covers all 22 chapters.
"""
from __future__ import annotations

SYSTEM_ID = "icd10_gm"

# ICD-10-GM chapters: (chapter_code, range_text, title)
_CHAPTERS = [
    ("GM01", "A00-B99",  "Bestimmte infektioese und parasitaere Krankheiten (Certain infectious and parasitic diseases)"),
    ("GM02", "C00-D48",  "Neubildungen (Neoplasms)"),
    ("GM03", "D50-D90",  "Krankheiten des Blutes und der blutbildenden Organe (Blood and immune disorders)"),
    ("GM04", "E00-E90",  "Endokrine, Ernaehrungs- und Stoffwechselkrankheiten (Endocrine, nutritional and metabolic diseases)"),
    ("GM05", "F00-F99",  "Psychische und Verhaltensstoerungen (Mental and behavioural disorders)"),
    ("GM06", "G00-G99",  "Krankheiten des Nervensystems (Diseases of the nervous system)"),
    ("GM07", "H00-H59",  "Krankheiten des Auges und der Augenanhangsgebilde (Diseases of the eye and adnexa)"),
    ("GM08", "H60-H95",  "Krankheiten des Ohres und des Warzenfortsatzes (Diseases of the ear and mastoid process)"),
    ("GM09", "I00-I99",  "Krankheiten des Kreislaufsystems (Diseases of the circulatory system)"),
    ("GM10", "J00-J99",  "Krankheiten des Atmungssystems (Diseases of the respiratory system)"),
    ("GM11", "K00-K93",  "Krankheiten des Verdauungssystems (Diseases of the digestive system)"),
    ("GM12", "L00-L99",  "Krankheiten der Haut und der Unterhaut (Diseases of the skin and subcutaneous tissue)"),
    ("GM13", "M00-M99",  "Krankheiten des Muskel-Skelett-Systems und des Bindegewebes (Musculoskeletal diseases)"),
    ("GM14", "N00-N99",  "Krankheiten des Urogenitalsystems (Diseases of the genitourinary system)"),
    ("GM15", "O00-O99",  "Schwangerschaft, Geburt und Wochenbett (Pregnancy, childbirth and the puerperium)"),
    ("GM16", "P00-P96",  "Bestimmte Zustande, die ihren Ursprung in der Perinatalperiode haben (Perinatal conditions)"),
    ("GM17", "Q00-Q99",  "Angeborene Fehlbildungen, Deformitaeten und Chromosomenanomalien (Congenital malformations)"),
    ("GM18", "R00-R99",  "Symptome und abnorme klinische und Laborbefunde (Symptoms and signs)"),
    ("GM19", "S00-T98",  "Verletzungen, Vergiftungen und bestimmte andere Folgen aeusserer Ursachen (Injury and poisoning)"),
    ("GM20", "V01-Y98",  "Aeussere Ursachen von Morbidi taet und Mortalitaet (External causes of morbidity)"),
    ("GM21", "Z00-Z99",  "Faktoren, die den Gesundheitszustand beeinflussen (Factors influencing health status)"),
    ("GM22", "U00-U99",  "Schluessel fuer besondere Zwecke - German extension codes (Codes for special purposes)"),
]

# Selected ICD-10-GM specific code blocks (German-specific additions)
_BLOCKS = [
    ("A00-A09", "Intestinale Infektionskrankheiten (Intestinal infectious diseases)", "GM01"),
    ("A15-A19", "Tuberkulose (Tuberculosis)", "GM01"),
    ("A20-A28", "Bestimmte bakterielle Zoonosen (Certain zoonotic bacterial diseases)", "GM01"),
    ("A30-A49", "Sonstige bakterielle Krankheiten (Other bacterial diseases)", "GM01"),
    ("A50-A64", "Infektionen, die vorwiegend durch Geschlechtsverkehr uebertragen werden", "GM01"),
    ("B00-B09", "Viruskrankheiten, die durch Haut- und Schleimhautlaesionen charakterisiert sind", "GM01"),
    ("B20-B24", "HIV-Krankheit (HIV disease)", "GM01"),
    ("C00-C75", "Boesamtige Neubildungen (Malignant neoplasms)", "GM02"),
    ("C76-C80", "Boesamtige Neubildungen ungenau bezeichneter Lokalisationen", "GM02"),
    ("C81-C96", "Boesamtige Neubildungen des lymphatischen Gewebes (Lymphoid malignancies)", "GM02"),
    ("D00-D09", "In-situ-Neubildungen (In situ neoplasms)", "GM02"),
    ("D10-D36", "Gutartige Neubildungen (Benign neoplasms)", "GM02"),
    ("F00-F09", "Organische, einschliesslich symptomatischer psychischer Stoerungen", "GM05"),
    ("F10-F19", "Psychische und Verhaltensstoerungen durch psychotrope Substanzen", "GM05"),
    ("F20-F29", "Schizophrenie, schizotype und wahnhafte Stoerungen", "GM05"),
    ("F30-F39", "Affektive Stoerungen (Mood disorders)", "GM05"),
    ("F40-F48", "Neurotische, Belastungs- und somatoforme Stoerungen", "GM05"),
    ("I00-I02", "Akutes rheumatisches Fieber (Acute rheumatic fever)", "GM09"),
    ("I10-I15", "Hypertonie (Hypertensive diseases)", "GM09"),
    ("I20-I25", "Ischaemische Herzkrankheiten (Ischaemic heart diseases)", "GM09"),
    ("I60-I69", "Zerebrovaskulaere Krankheiten (Cerebrovascular diseases)", "GM09"),
    ("J00-J06", "Akute Infektionen der oberen Atemwege (Acute upper respiratory infections)", "GM10"),
    ("J09-J18", "Grippe und Pneumonie (Influenza and pneumonia)", "GM10"),
    ("J40-J47", "Chronische Krankheiten der unteren Atemwege (Chronic lower respiratory diseases)", "GM10"),
    ("U50-U52", "Funktionseinschraenkungen - German-specific extension codes", "GM22"),
    ("U55",     "Erfolgreiche Reanimation - German-specific resuscitation code", "GM22"),
    ("U60-U61", "Stadieneinteilung der HIV-Infektion - German HIV staging codes", "GM22"),
    ("U69",     "Sonstige sekundaere Schluessel - Secondary key codes", "GM22"),
    ("U80-U85", "Infektionserreger mit Resistenzen - Antimicrobial resistance codes", "GM22"),
]

NODES: list[tuple] = []
for i, (code, title_with_range, title) in enumerate(_CHAPTERS):
    NODES.append((code, title, 1, None))
for code, title, parent in _BLOCKS:
    NODES.append((code, title, 2, parent))


async def ingest_icd10_gm(conn) -> int:
    """Ingest ICD-10-GM (German Modification) chapter and block skeleton."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "ICD-10-GM",
        "ICD-10 German Modification (Internationale statistische Klassifikation der Krankheiten)",
        "Germany",
        "2024",
        "Bundesinstitut fur Arzneimittel und Medizinprodukte (BfArM)",
        "#DC2626",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code[:4] if level == 1 else code[:3]
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
    print(f"  Ingested {count} ICD-10-GM codes")
    return count
