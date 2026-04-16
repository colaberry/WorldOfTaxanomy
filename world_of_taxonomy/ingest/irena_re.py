"""Ingest IRENA Renewable Energy Technology Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("irena_re", "IRENA RE", "IRENA Renewable Energy Technology Categories", "2024", "Global", "IRENA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IR", "IRENA RE Technologies", 1, None),
    ("IR.01", "Solar Photovoltaic", 2, 'IR'),
    ("IR.02", "Concentrated Solar Power", 2, 'IR'),
    ("IR.03", "Onshore Wind", 2, 'IR'),
    ("IR.04", "Offshore Wind", 2, 'IR'),
    ("IR.05", "Hydropower (Large)", 2, 'IR'),
    ("IR.06", "Small Hydropower", 2, 'IR'),
    ("IR.07", "Bioenergy (Solid)", 2, 'IR'),
    ("IR.08", "Biogas", 2, 'IR'),
    ("IR.09", "Liquid Biofuels", 2, 'IR'),
    ("IR.10", "Geothermal Power", 2, 'IR'),
    ("IR.11", "Geothermal Direct Use", 2, 'IR'),
    ("IR.12", "Marine Energy (Tidal/Wave)", 2, 'IR'),
    ("IR.13", "Solar Heating and Cooling", 2, 'IR'),
    ("IR.14", "Renewable Hydrogen", 2, 'IR'),
    ("IR.15", "Battery Storage", 2, 'IR'),
    ("IR.16", "Pumped Hydro Storage", 2, 'IR'),
]

async def ingest_irena_re(conn) -> int:
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
