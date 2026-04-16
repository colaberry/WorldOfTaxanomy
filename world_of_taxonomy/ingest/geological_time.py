"""Ingest International Chronostratigraphic Chart (Eons/Eras)."""
from __future__ import annotations

_SYSTEM_ROW = ("geological_time", "Geological Timescale", "International Chronostratigraphic Chart (Eons/Eras)", "2024", "Global", "ICS/IUGS")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GT", "Geological Eons/Eras", 1, None),
    ("GT.01", "Hadean Eon", 2, 'GT'),
    ("GT.02", "Archean Eon", 2, 'GT'),
    ("GT.03", "Proterozoic Eon", 2, 'GT'),
    ("GT.04", "Phanerozoic Eon", 2, 'GT'),
    ("GT.05", "Paleozoic Era", 2, 'GT'),
    ("GT.06", "Mesozoic Era", 2, 'GT'),
    ("GT.07", "Cenozoic Era", 2, 'GT'),
    ("GT.08", "Cambrian Period", 2, 'GT'),
    ("GT.09", "Ordovician Period", 2, 'GT'),
    ("GT.10", "Silurian Period", 2, 'GT'),
    ("GT.11", "Devonian Period", 2, 'GT'),
    ("GT.12", "Carboniferous Period", 2, 'GT'),
    ("GT.13", "Permian Period", 2, 'GT'),
    ("GT.14", "Triassic Period", 2, 'GT'),
    ("GT.15", "Jurassic Period", 2, 'GT'),
    ("GT.16", "Cretaceous Period", 2, 'GT'),
    ("GT.17", "Paleogene Period", 2, 'GT'),
    ("GT.18", "Neogene Period", 2, 'GT'),
    ("GT.19", "Quaternary Period", 2, 'GT'),
]

async def ingest_geological_time(conn) -> int:
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
