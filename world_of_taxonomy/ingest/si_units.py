"""Ingest International System of Units (SI)."""
from __future__ import annotations

_SYSTEM_ROW = ("si_units", "SI Units", "International System of Units (SI)", "2019", "Global", "BIPM")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SI", "SI Unit Categories", 1, None),
    ("SI.01", "Metre (length)", 2, 'SI'),
    ("SI.02", "Kilogram (mass)", 2, 'SI'),
    ("SI.03", "Second (time)", 2, 'SI'),
    ("SI.04", "Ampere (electric current)", 2, 'SI'),
    ("SI.05", "Kelvin (temperature)", 2, 'SI'),
    ("SI.06", "Mole (amount of substance)", 2, 'SI'),
    ("SI.07", "Candela (luminous intensity)", 2, 'SI'),
    ("SI.08", "Radian (plane angle)", 2, 'SI'),
    ("SI.09", "Steradian (solid angle)", 2, 'SI'),
    ("SI.10", "Hertz (frequency)", 2, 'SI'),
    ("SI.11", "Newton (force)", 2, 'SI'),
    ("SI.12", "Pascal (pressure)", 2, 'SI'),
    ("SI.13", "Joule (energy)", 2, 'SI'),
    ("SI.14", "Watt (power)", 2, 'SI'),
    ("SI.15", "Coulomb (charge)", 2, 'SI'),
    ("SI.16", "Volt (voltage)", 2, 'SI'),
    ("SI.17", "Ohm (resistance)", 2, 'SI'),
    ("SI.18", "SI Prefixes (yocto to yotta)", 2, 'SI'),
]

async def ingest_si_units(conn) -> int:
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
