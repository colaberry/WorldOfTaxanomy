"""Ingest ANZSRC Socio-Economic Objective Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("anzsrc_seo", "ANZSRC SEO", "ANZSRC Socio-Economic Objective Classification", "2020", "Australia / New Zealand", "ABS/Stats NZ")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AS", "ANZSRC SEO Divisions", 1, None),
    ("AS.10", "Defence", 2, 'AS'),
    ("AS.11", "Plant Production and Plant Primary Products", 2, 'AS'),
    ("AS.12", "Animal Production and Animal Primary Products", 2, 'AS'),
    ("AS.13", "Mineral Resources", 2, 'AS'),
    ("AS.14", "Energy", 2, 'AS'),
    ("AS.15", "Manufacturing", 2, 'AS'),
    ("AS.16", "Construction", 2, 'AS'),
    ("AS.17", "Transport", 2, 'AS'),
    ("AS.18", "Information and Communication Services", 2, 'AS'),
    ("AS.19", "Health", 2, 'AS'),
    ("AS.20", "Education and Training", 2, 'AS'),
    ("AS.21", "Indigenous", 2, 'AS'),
    ("AS.22", "Law, Politics, Community Services", 2, 'AS'),
    ("AS.23", "Cultural Understanding", 2, 'AS'),
    ("AS.24", "Environment", 2, 'AS'),
    ("AS.25", "Expanding Knowledge", 2, 'AS'),
]

async def ingest_anzsrc_seo(conn) -> int:
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
