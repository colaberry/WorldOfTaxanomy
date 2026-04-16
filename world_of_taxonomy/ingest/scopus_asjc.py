"""Ingest Scopus ASJC (Subject Classification)."""
from __future__ import annotations

_SYSTEM_ROW = ("scopus_asjc", "Scopus ASJC", "Scopus ASJC (Subject Classification)", "2024", "Global", "Elsevier")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Elsevier License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ASJC", "ASJC Subject Areas", 1, None),
    ("ASJC.10", "Multidisciplinary", 2, 'ASJC'),
    ("ASJC.11", "Agricultural and Biological Sciences", 2, 'ASJC'),
    ("ASJC.12", "Arts and Humanities", 2, 'ASJC'),
    ("ASJC.13", "Biochemistry, Genetics and Molecular Biology", 2, 'ASJC'),
    ("ASJC.14", "Business, Management and Accounting", 2, 'ASJC'),
    ("ASJC.15", "Chemical Engineering", 2, 'ASJC'),
    ("ASJC.16", "Chemistry", 2, 'ASJC'),
    ("ASJC.17", "Computer Science", 2, 'ASJC'),
    ("ASJC.18", "Decision Sciences", 2, 'ASJC'),
    ("ASJC.19", "Earth and Planetary Sciences", 2, 'ASJC'),
    ("ASJC.20", "Economics, Econometrics and Finance", 2, 'ASJC'),
    ("ASJC.21", "Energy", 2, 'ASJC'),
    ("ASJC.22", "Engineering", 2, 'ASJC'),
    ("ASJC.23", "Environmental Science", 2, 'ASJC'),
    ("ASJC.24", "Immunology and Microbiology", 2, 'ASJC'),
    ("ASJC.25", "Materials Science", 2, 'ASJC'),
    ("ASJC.26", "Mathematics", 2, 'ASJC'),
    ("ASJC.27", "Medicine", 2, 'ASJC'),
    ("ASJC.28", "Neuroscience", 2, 'ASJC'),
    ("ASJC.29", "Nursing", 2, 'ASJC'),
    ("ASJC.30", "Pharmacology, Toxicology and Pharmaceutics", 2, 'ASJC'),
    ("ASJC.31", "Physics and Astronomy", 2, 'ASJC'),
    ("ASJC.32", "Psychology", 2, 'ASJC'),
    ("ASJC.33", "Social Sciences", 2, 'ASJC'),
    ("ASJC.34", "Veterinary", 2, 'ASJC'),
    ("ASJC.35", "Dentistry", 2, 'ASJC'),
    ("ASJC.36", "Health Professions", 2, 'ASJC'),
]

async def ingest_scopus_asjc(conn) -> int:
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
