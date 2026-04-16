"""Ingest EDQM Standard Terms for Pharmaceutical Dosage Forms."""
from __future__ import annotations

_SYSTEM_ROW = ("edqm_dosage", "EDQM Dosage Forms", "EDQM Standard Terms for Pharmaceutical Dosage Forms", "2024", "European Union", "EDQM/CoE")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ED", "EDQM Categories", 1, None),
    ("ED.01", "Oral Solid Dosage Forms", 2, 'ED'),
    ("ED.02", "Oral Liquid Dosage Forms", 2, 'ED'),
    ("ED.03", "Parenteral Dosage Forms", 2, 'ED'),
    ("ED.04", "Topical Dosage Forms", 2, 'ED'),
    ("ED.05", "Rectal Dosage Forms", 2, 'ED'),
    ("ED.06", "Vaginal Dosage Forms", 2, 'ED'),
    ("ED.07", "Ophthalmic Dosage Forms", 2, 'ED'),
    ("ED.08", "Nasal Dosage Forms", 2, 'ED'),
    ("ED.09", "Auricular Dosage Forms", 2, 'ED'),
    ("ED.10", "Inhalation Dosage Forms", 2, 'ED'),
    ("ED.11", "Transdermal Dosage Forms", 2, 'ED'),
    ("ED.12", "Implantable Dosage Forms", 2, 'ED'),
    ("ED.13", "Routes of Administration", 2, 'ED'),
    ("ED.14", "Containers and Closures", 2, 'ED'),
    ("ED.15", "Units of Presentation", 2, 'ED'),
    ("ED.16", "Combination Packages", 2, 'ED'),
]

async def ingest_edqm_dosage(conn) -> int:
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
