"""Ingest European Waste Catalogue."""
from __future__ import annotations

_SYSTEM_ROW = ("eu_waste_cat", "EU Waste Catalogue", "European Waste Catalogue", "2014", "European Union", "European Commission")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EWC", "EWC Chapters", 1, None),
    ("EWC.01", "Wastes from exploration and mining", 2, 'EWC'),
    ("EWC.02", "Wastes from agriculture and food", 2, 'EWC'),
    ("EWC.03", "Wastes from wood processing and paper", 2, 'EWC'),
    ("EWC.04", "Wastes from leather and textile", 2, 'EWC'),
    ("EWC.05", "Wastes from petroleum refining", 2, 'EWC'),
    ("EWC.06", "Wastes from inorganic chemical processes", 2, 'EWC'),
    ("EWC.07", "Wastes from organic chemical processes", 2, 'EWC'),
    ("EWC.08", "Wastes from coatings and sealants", 2, 'EWC'),
    ("EWC.09", "Wastes from photographic industry", 2, 'EWC'),
    ("EWC.10", "Wastes from thermal processes", 2, 'EWC'),
    ("EWC.11", "Wastes from chemical surface treatment", 2, 'EWC'),
    ("EWC.12", "Wastes from shaping and treatment of metals", 2, 'EWC'),
    ("EWC.13", "Oil wastes and liquid fuel wastes", 2, 'EWC'),
    ("EWC.14", "Waste organic solvents", 2, 'EWC'),
    ("EWC.15", "Waste packaging and absorbents", 2, 'EWC'),
    ("EWC.16", "Wastes not otherwise specified", 2, 'EWC'),
    ("EWC.17", "Construction and demolition waste", 2, 'EWC'),
    ("EWC.18", "Wastes from human and animal health care", 2, 'EWC'),
    ("EWC.19", "Wastes from waste management facilities", 2, 'EWC'),
    ("EWC.20", "Municipal wastes", 2, 'EWC'),
]

async def ingest_eu_waste_cat(conn) -> int:
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
