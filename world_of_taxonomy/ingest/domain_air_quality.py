"""Ingest Air Quality Index Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_air_quality", "Air Quality Index", "Air Quality Index Types", "1.0", "Global", "EPA/WHO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AQ", "Air Quality Index Types", 1, None),
    ("AQ.01", "AQI - Good (0-50)", 2, 'AQ'),
    ("AQ.02", "AQI - Moderate (51-100)", 2, 'AQ'),
    ("AQ.03", "AQI - Unhealthy Sensitive (101-150)", 2, 'AQ'),
    ("AQ.04", "AQI - Unhealthy (151-200)", 2, 'AQ'),
    ("AQ.05", "AQI - Very Unhealthy (201-300)", 2, 'AQ'),
    ("AQ.06", "AQI - Hazardous (301-500)", 2, 'AQ'),
    ("AQ.07", "PM2.5 Measurement", 2, 'AQ'),
    ("AQ.08", "PM10 Measurement", 2, 'AQ'),
    ("AQ.09", "Ozone (O3) Level", 2, 'AQ'),
    ("AQ.10", "NO2 Level", 2, 'AQ'),
    ("AQ.11", "SO2 Level", 2, 'AQ'),
    ("AQ.12", "CO Level", 2, 'AQ'),
    ("AQ.13", "Indoor Air Quality", 2, 'AQ'),
]

async def ingest_domain_air_quality(conn) -> int:
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
