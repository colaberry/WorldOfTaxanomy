"""Ingest IUCN Red List Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("iucn_red_list", "IUCN Red List", "IUCN Red List Categories", "2024", "Global", "IUCN")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IUCN", "IUCN Red List Categories", 1, None),
    ("IUCN.EX", "Extinct (EX)", 2, 'IUCN'),
    ("IUCN.EW", "Extinct in the Wild (EW)", 2, 'IUCN'),
    ("IUCN.CR", "Critically Endangered (CR)", 2, 'IUCN'),
    ("IUCN.EN", "Endangered (EN)", 2, 'IUCN'),
    ("IUCN.VU", "Vulnerable (VU)", 2, 'IUCN'),
    ("IUCN.NT", "Near Threatened (NT)", 2, 'IUCN'),
    ("IUCN.LC", "Least Concern (LC)", 2, 'IUCN'),
    ("IUCN.DD", "Data Deficient (DD)", 2, 'IUCN'),
    ("IUCN.NE", "Not Evaluated (NE)", 2, 'IUCN'),
    ("IUCN.C1", "Criterion A - Population Reduction", 2, 'IUCN'),
    ("IUCN.C2", "Criterion B - Geographic Range", 2, 'IUCN'),
    ("IUCN.C3", "Criterion C - Small Population", 2, 'IUCN'),
    ("IUCN.C4", "Criterion D - Very Small/Restricted", 2, 'IUCN'),
    ("IUCN.C5", "Criterion E - Quantitative Analysis", 2, 'IUCN'),
]

async def ingest_iucn_red_list(conn) -> int:
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
