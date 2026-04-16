"""Ingest CDC Vaccine Schedule."""
from __future__ import annotations

_SYSTEM_ROW = ("cdc_vaccine", "CDC Vaccine Schedule", "CDC Vaccine Schedule", "2024", "United States", "CDC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("VAX", "CDC Vaccine Categories", 1, None),
    ("VAX.01", "Hepatitis B (HepB)", 2, 'VAX'),
    ("VAX.02", "Rotavirus (RV)", 2, 'VAX'),
    ("VAX.03", "DTaP (Diphtheria, Tetanus, Pertussis)", 2, 'VAX'),
    ("VAX.04", "Haemophilus influenzae (Hib)", 2, 'VAX'),
    ("VAX.05", "Pneumococcal (PCV15/PCV20)", 2, 'VAX'),
    ("VAX.06", "Inactivated Poliovirus (IPV)", 2, 'VAX'),
    ("VAX.07", "Influenza (Annual)", 2, 'VAX'),
    ("VAX.08", "MMR (Measles, Mumps, Rubella)", 2, 'VAX'),
    ("VAX.09", "Varicella (VAR)", 2, 'VAX'),
    ("VAX.10", "Hepatitis A (HepA)", 2, 'VAX'),
    ("VAX.11", "Meningococcal (MenACWY)", 2, 'VAX'),
    ("VAX.12", "HPV (Human Papillomavirus)", 2, 'VAX'),
    ("VAX.13", "Tdap (Adolescent/Adult)", 2, 'VAX'),
    ("VAX.14", "COVID-19", 2, 'VAX'),
    ("VAX.15", "Shingles (Zoster)", 2, 'VAX'),
    ("VAX.16", "RSV (Respiratory Syncytial)", 2, 'VAX'),
    ("VAX.17", "Mpox", 2, 'VAX'),
]

async def ingest_cdc_vaccine(conn) -> int:
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
