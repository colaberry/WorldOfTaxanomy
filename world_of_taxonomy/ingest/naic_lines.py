"""Ingest NAIC Annual Statement Lines of Business."""
from __future__ import annotations

_SYSTEM_ROW = ("naic_lines", "NAIC Lines", "NAIC Annual Statement Lines of Business", "2024", "United States", "NAIC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("NL", "NAIC Lines of Business", 1, None),
    ("NL.01", "Fire", 2, 'NL'),
    ("NL.02", "Allied Lines", 2, 'NL'),
    ("NL.03", "Farmowners Multiple Peril", 2, 'NL'),
    ("NL.04", "Homeowners Multiple Peril", 2, 'NL'),
    ("NL.05", "Commercial Multiple Peril", 2, 'NL'),
    ("NL.06", "Mortgage Guaranty", 2, 'NL'),
    ("NL.08", "Ocean Marine", 2, 'NL'),
    ("NL.09", "Inland Marine", 2, 'NL'),
    ("NL.10", "Financial Guaranty", 2, 'NL'),
    ("NL.11", "Medical Malpractice", 2, 'NL'),
    ("NL.12", "Earthquake", 2, 'NL'),
    ("NL.13", "Group Accident and Health", 2, 'NL'),
    ("NL.14", "Credit Accident and Health", 2, 'NL'),
    ("NL.15", "Other Accident and Health", 2, 'NL'),
    ("NL.16", "Workers Compensation", 2, 'NL'),
    ("NL.17", "Other Liability", 2, 'NL'),
    ("NL.18", "Products Liability", 2, 'NL'),
    ("NL.19", "Private Passenger Auto Liability", 2, 'NL'),
    ("NL.20", "Commercial Auto Liability", 2, 'NL'),
    ("NL.21", "Auto Physical Damage", 2, 'NL'),
    ("NL.22", "Aircraft", 2, 'NL'),
    ("NL.23", "Fidelity", 2, 'NL'),
    ("NL.24", "Surety", 2, 'NL'),
    ("NL.26", "Burglary and Theft", 2, 'NL'),
    ("NL.27", "Boiler and Machinery", 2, 'NL'),
    ("NL.28", "Credit", 2, 'NL'),
    ("NL.29", "International", 2, 'NL'),
    ("NL.30", "Warranty", 2, 'NL'),
    ("NL.34", "Aggregate Other Lines", 2, 'NL'),
]

async def ingest_naic_lines(conn) -> int:
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
