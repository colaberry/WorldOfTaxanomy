"""Ingest Stockholm Convention on POPs."""
from __future__ import annotations

_SYSTEM_ROW = ("stockholm_pops", "Stockholm Convention", "Stockholm Convention on POPs", "2024", "Global", "UNEP")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("POPS", "Stockholm Convention Annexes", 1, None),
    ("POPS.A", "Annex A - Elimination", 2, 'POPS'),
    ("POPS.B", "Annex B - Restriction", 2, 'POPS'),
    ("POPS.C", "Annex C - Unintentional Production", 2, 'POPS'),
    ("POPS.01", "Aldrin", 2, 'POPS'),
    ("POPS.02", "Chlordane", 2, 'POPS'),
    ("POPS.03", "DDT", 2, 'POPS'),
    ("POPS.04", "Dieldrin", 2, 'POPS'),
    ("POPS.05", "Endrin", 2, 'POPS'),
    ("POPS.06", "Heptachlor", 2, 'POPS'),
    ("POPS.07", "Hexachlorobenzene", 2, 'POPS'),
    ("POPS.08", "Mirex", 2, 'POPS'),
    ("POPS.09", "Toxaphene", 2, 'POPS'),
    ("POPS.10", "PCBs", 2, 'POPS'),
    ("POPS.11", "Dioxins and Furans", 2, 'POPS'),
    ("POPS.12", "PFOS and derivatives", 2, 'POPS'),
    ("POPS.13", "PFOA and related compounds", 2, 'POPS'),
    ("POPS.14", "PFHxS", 2, 'POPS'),
    ("POPS.NIP", "National Implementation Plans", 2, 'POPS'),
]

async def ingest_stockholm_pops(conn) -> int:
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
