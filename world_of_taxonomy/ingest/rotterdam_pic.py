"""Ingest Rotterdam Convention PIC."""
from __future__ import annotations

_SYSTEM_ROW = ("rotterdam_pic", "Rotterdam Convention", "Rotterdam Convention PIC", "2024", "Global", "UNEP/FAO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PIC", "Rotterdam Convention Annexes", 1, None),
    ("PIC.III", "Annex III - PIC Chemicals", 2, 'PIC'),
    ("PIC.P", "Pesticides Listed", 2, 'PIC'),
    ("PIC.I", "Industrial Chemicals Listed", 2, 'PIC'),
    ("PIC.SH", "Severely Hazardous Pesticide Formulation", 2, 'PIC'),
    ("PIC.01", "Aldrin", 2, 'PIC'),
    ("PIC.02", "Chlordane", 2, 'PIC'),
    ("PIC.03", "DDT", 2, 'PIC'),
    ("PIC.04", "Heptachlor", 2, 'PIC'),
    ("PIC.05", "Hexachlorobenzene", 2, 'PIC'),
    ("PIC.06", "Lindane", 2, 'PIC'),
    ("PIC.07", "Mercury compounds", 2, 'PIC'),
    ("PIC.08", "Asbestos (various forms)", 2, 'PIC'),
    ("PIC.09", "Lead compounds", 2, 'PIC'),
    ("PIC.10", "Tributyltin compounds", 2, 'PIC'),
    ("PIC.DN", "Designated National Authority", 2, 'PIC'),
    ("PIC.IR", "Import Response", 2, 'PIC'),
]

async def ingest_rotterdam_pic(conn) -> int:
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
