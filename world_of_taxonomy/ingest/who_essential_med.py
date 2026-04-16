"""Ingest WHO Essential Medicines List."""
from __future__ import annotations

_SYSTEM_ROW = ("who_essential_med", "WHO Essential Medicines", "WHO Essential Medicines List", "2023", "Global", "WHO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EML", "EML Sections", 1, None),
    ("EML.01", "Anaesthetics", 2, 'EML'),
    ("EML.02", "Analgesics and antipyretics", 2, 'EML'),
    ("EML.03", "Antiallergics and anaphylaxis", 2, 'EML'),
    ("EML.04", "Antidotes and substances used in poisoning", 2, 'EML'),
    ("EML.05", "Anticonvulsants/antiepileptics", 2, 'EML'),
    ("EML.06", "Anti-infective medicines", 2, 'EML'),
    ("EML.07", "Antimigraine medicines", 2, 'EML'),
    ("EML.08", "Immunomodulators and antineoplastics", 2, 'EML'),
    ("EML.09", "Antiparkinsonism medicines", 2, 'EML'),
    ("EML.10", "Medicines affecting the blood", 2, 'EML'),
    ("EML.11", "Blood products and plasma substitutes", 2, 'EML'),
    ("EML.12", "Cardiovascular medicines", 2, 'EML'),
    ("EML.13", "Dermatological medicines", 2, 'EML'),
    ("EML.14", "Diagnostic agents", 2, 'EML'),
    ("EML.15", "Disinfectants and antiseptics", 2, 'EML'),
    ("EML.16", "Diuretics", 2, 'EML'),
    ("EML.17", "Gastrointestinal medicines", 2, 'EML'),
    ("EML.18", "Hormones and other endocrine", 2, 'EML'),
    ("EML.19", "Immunologicals", 2, 'EML'),
    ("EML.20", "Muscle relaxants", 2, 'EML'),
    ("EML.21", "Ophthalmological preparations", 2, 'EML'),
    ("EML.22", "Oxytocics and antioxytocics", 2, 'EML'),
    ("EML.23", "Peritoneal dialysis solution", 2, 'EML'),
    ("EML.24", "Psychotherapeutic medicines", 2, 'EML'),
    ("EML.25", "Respiratory medicines", 2, 'EML'),
    ("EML.26", "Vitamins and minerals", 2, 'EML'),
]

async def ingest_who_essential_med(conn) -> int:
    sid, short, full, ver, region, authority = _SYSTEM_ROW
    await conn.execute(
        """INSERT INTO classification_system (id, name, full_name, version, region, authority,
                  source_url, source_date, data_provenance, license)
           VALUES ($1,$2,$3,$4,$5,$6,$7,CURRENT_DATE,$8,$9)
           ON CONFLICT (id) DO UPDATE SET name=$2,full_name=$3,version=$4,region=$5,
                  authority=$6,source_url=$7,source_date=CURRENT_DATE,
                  data_provenance=$8,license=$9""",
        sid, short, full, ver, region, authority,
        _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute("DELETE FROM classification_node WHERE system_id = $1", sid)
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1,$2,$3,$4,$5)""",
            sid, code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, sid)
    return count
