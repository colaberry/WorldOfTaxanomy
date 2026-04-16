"""Ingest DigComp 2.2 (Digital Competence)."""
from __future__ import annotations

_SYSTEM_ROW = ("digcomp_22", "DigComp 2.2", "DigComp 2.2 (Digital Competence)", "2.2", "European Union", "EU JRC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DC", "DigComp Areas", 1, None),
    ("DC.1", "Information and data literacy", 2, 'DC'),
    ("DC.2", "Communication and collaboration", 2, 'DC'),
    ("DC.3", "Digital content creation", 2, 'DC'),
    ("DC.4", "Safety", 2, 'DC'),
    ("DC.5", "Problem solving", 2, 'DC'),
    ("DC.1.1", "Browsing, searching, filtering", 2, 'DC'),
    ("DC.1.2", "Evaluating data", 2, 'DC'),
    ("DC.1.3", "Managing data", 2, 'DC'),
    ("DC.2.1", "Interacting through technologies", 2, 'DC'),
    ("DC.2.2", "Sharing through technologies", 2, 'DC'),
    ("DC.2.3", "Engaging in citizenship", 2, 'DC'),
    ("DC.2.4", "Collaborating through technologies", 2, 'DC'),
    ("DC.2.5", "Netiquette", 2, 'DC'),
    ("DC.2.6", "Managing digital identity", 2, 'DC'),
    ("DC.3.1", "Developing digital content", 2, 'DC'),
    ("DC.3.2", "Integrating and re-elaborating", 2, 'DC'),
    ("DC.3.3", "Copyright and licenses", 2, 'DC'),
    ("DC.3.4", "Programming", 2, 'DC'),
    ("DC.4.1", "Protecting devices", 2, 'DC'),
    ("DC.4.2", "Protecting personal data", 2, 'DC'),
    ("DC.4.3", "Protecting health and well-being", 2, 'DC'),
    ("DC.4.4", "Protecting the environment", 2, 'DC'),
    ("DC.5.1", "Solving technical problems", 2, 'DC'),
    ("DC.5.2", "Identifying needs and responses", 2, 'DC'),
    ("DC.5.3", "Creatively using technologies", 2, 'DC'),
    ("DC.5.4", "Identifying digital competence gaps", 2, 'DC'),
]

async def ingest_digcomp_22(conn) -> int:
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
