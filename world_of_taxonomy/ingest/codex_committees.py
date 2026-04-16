"""Ingest Codex Alimentarius Committee Structure."""
from __future__ import annotations

_SYSTEM_ROW = ("codex_committees", "Codex Committees", "Codex Alimentarius Committee Structure", "2024", "Global", "FAO/WHO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CX", "Codex Committees", 1, None),
    ("CX.01", "General Principles (CCGP)", 2, 'CX'),
    ("CX.02", "Food Labelling (CCFL)", 2, 'CX'),
    ("CX.03", "Food Hygiene (CCFH)", 2, 'CX'),
    ("CX.04", "Food Additives (CCFA)", 2, 'CX'),
    ("CX.05", "Contaminants in Foods (CCCF)", 2, 'CX'),
    ("CX.06", "Pesticide Residues (CCPR)", 2, 'CX'),
    ("CX.07", "Residues of Vet Drugs (CCRVDF)", 2, 'CX'),
    ("CX.08", "Methods of Analysis (CCMAS)", 2, 'CX'),
    ("CX.09", "Food Import/Export (CCFICS)", 2, 'CX'),
    ("CX.10", "Nutrition and Foods (CCNFSDU)", 2, 'CX'),
    ("CX.11", "Fats and Oils", 2, 'CX'),
    ("CX.12", "Fish and Fishery Products", 2, 'CX'),
    ("CX.13", "Fresh Fruits and Vegetables", 2, 'CX'),
    ("CX.14", "Processed Fruits and Vegetables", 2, 'CX'),
    ("CX.15", "Cereals, Pulses, Legumes", 2, 'CX'),
    ("CX.16", "Milk and Milk Products", 2, 'CX'),
    ("CX.17", "Sugars", 2, 'CX'),
    ("CX.18", "Spices and Culinary Herbs", 2, 'CX'),
]

async def ingest_codex_committees(conn) -> int:
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
