"""Ingest Cell Therapy Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_cell_therapy", "Cell Therapy", "Cell Therapy Types", "1.0", "Global", "ISCT")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CT", "Cell Therapy Types", 1, None),
    ("CT.01", "CAR-T Cell Therapy", 2, 'CT'),
    ("CT.02", "CAR-NK Cell Therapy", 2, 'CT'),
    ("CT.03", "TIL Therapy", 2, 'CT'),
    ("CT.04", "TCR-T Cell Therapy", 2, 'CT'),
    ("CT.05", "Mesenchymal Stem Cell", 2, 'CT'),
    ("CT.06", "Hematopoietic Stem Cell", 2, 'CT'),
    ("CT.07", "iPSC-Derived Cell Therapy", 2, 'CT'),
    ("CT.08", "Dendritic Cell Therapy", 2, 'CT'),
    ("CT.09", "Autologous Cell Therapy", 2, 'CT'),
    ("CT.10", "Allogeneic Cell Therapy", 2, 'CT'),
    ("CT.11", "Xenogeneic Cell Therapy", 2, 'CT'),
    ("CT.12", "Cell Expansion Platform", 2, 'CT'),
    ("CT.13", "Cryopreservation Protocol", 2, 'CT'),
    ("CT.14", "Point-of-Care Manufacturing", 2, 'CT'),
]

async def ingest_domain_cell_therapy(conn) -> int:
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
