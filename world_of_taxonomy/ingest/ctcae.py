"""Ingest CTCAE (Adverse Events)."""
from __future__ import annotations

_SYSTEM_ROW = ("ctcae", "CTCAE", "CTCAE (Adverse Events)", "5.0", "United States", "NCI")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CTCAE", "CTCAE System Organ Classes", 1, None),
    ("CTCAE.01", "Blood and lymphatic system", 2, 'CTCAE'),
    ("CTCAE.02", "Cardiac disorders", 2, 'CTCAE'),
    ("CTCAE.03", "Congenital, familial and genetic", 2, 'CTCAE'),
    ("CTCAE.04", "Ear and labyrinth", 2, 'CTCAE'),
    ("CTCAE.05", "Endocrine disorders", 2, 'CTCAE'),
    ("CTCAE.06", "Eye disorders", 2, 'CTCAE'),
    ("CTCAE.07", "Gastrointestinal disorders", 2, 'CTCAE'),
    ("CTCAE.08", "General and administration site", 2, 'CTCAE'),
    ("CTCAE.09", "Hepatobiliary disorders", 2, 'CTCAE'),
    ("CTCAE.10", "Immune system disorders", 2, 'CTCAE'),
    ("CTCAE.11", "Infections and infestations", 2, 'CTCAE'),
    ("CTCAE.12", "Injury and procedural", 2, 'CTCAE'),
    ("CTCAE.13", "Investigations", 2, 'CTCAE'),
    ("CTCAE.14", "Metabolism and nutrition", 2, 'CTCAE'),
    ("CTCAE.15", "Musculoskeletal and connective", 2, 'CTCAE'),
    ("CTCAE.16", "Neoplasms", 2, 'CTCAE'),
    ("CTCAE.17", "Nervous system", 2, 'CTCAE'),
    ("CTCAE.18", "Psychiatric disorders", 2, 'CTCAE'),
    ("CTCAE.19", "Renal and urinary", 2, 'CTCAE'),
    ("CTCAE.20", "Reproductive and breast", 2, 'CTCAE'),
    ("CTCAE.21", "Respiratory and thoracic", 2, 'CTCAE'),
    ("CTCAE.22", "Skin and subcutaneous", 2, 'CTCAE'),
    ("CTCAE.23", "Social circumstances", 2, 'CTCAE'),
    ("CTCAE.24", "Surgical and medical procedures", 2, 'CTCAE'),
    ("CTCAE.25", "Vascular disorders", 2, 'CTCAE'),
    ("CTCAE.26", "Product issues", 2, 'CTCAE'),
]

async def ingest_ctcae(conn) -> int:
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
