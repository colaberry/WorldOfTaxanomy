"""Ingest ITU-T Telecommunication Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("itu_t", "ITU-T Recommendations", "ITU-T Telecommunication Standards", "2024", "Global", "ITU")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IT", "ITU-T Series", 1, None),
    ("IT.A", "Series A - Organization", 2, 'IT'),
    ("IT.D", "Series D - Tariff Principles", 2, 'IT'),
    ("IT.E", "Series E - Telephone Network", 2, 'IT'),
    ("IT.F", "Series F - Non-Telephone Services", 2, 'IT'),
    ("IT.G", "Series G - Transmission Systems", 2, 'IT'),
    ("IT.H", "Series H - Audiovisual Systems", 2, 'IT'),
    ("IT.I", "Series I - ISDN", 2, 'IT'),
    ("IT.J", "Series J - Cable Networks", 2, 'IT'),
    ("IT.K", "Series K - Protection Against Interference", 2, 'IT'),
    ("IT.L", "Series L - Environment and ICTs", 2, 'IT'),
    ("IT.M", "Series M - Telecom Management", 2, 'IT'),
    ("IT.O", "Series O - Measuring Equipment", 2, 'IT'),
    ("IT.P", "Series P - Telephone Quality", 2, 'IT'),
    ("IT.Q", "Series Q - Signaling", 2, 'IT'),
    ("IT.V", "Series V - Data Communication (Telephone)", 2, 'IT'),
    ("IT.X", "Series X - Data Networks/Security", 2, 'IT'),
    ("IT.Y", "Series Y - Global Info Infrastructure/IoT", 2, 'IT'),
    ("IT.Z", "Series Z - Languages and Software", 2, 'IT'),
]

async def ingest_itu_t(conn) -> int:
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
