"""Ingest CVE Vulnerability Type Categories (CWE Top)."""
from __future__ import annotations

_SYSTEM_ROW = ("cve_types", "CVE Types", "CVE Vulnerability Type Categories (CWE Top)", "2024", "Global", "MITRE/NVD")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CV", "CVE/CWE Types", 1, None),
    ("CV.01", "CWE-79: Cross-Site Scripting (XSS)", 2, 'CV'),
    ("CV.02", "CWE-89: SQL Injection", 2, 'CV'),
    ("CV.03", "CWE-787: Out-of-bounds Write", 2, 'CV'),
    ("CV.04", "CWE-20: Improper Input Validation", 2, 'CV'),
    ("CV.05", "CWE-125: Out-of-bounds Read", 2, 'CV'),
    ("CV.06", "CWE-78: OS Command Injection", 2, 'CV'),
    ("CV.07", "CWE-416: Use After Free", 2, 'CV'),
    ("CV.08", "CWE-22: Path Traversal", 2, 'CV'),
    ("CV.09", "CWE-352: CSRF", 2, 'CV'),
    ("CV.10", "CWE-434: Unrestricted File Upload", 2, 'CV'),
    ("CV.11", "CWE-862: Missing Authorization", 2, 'CV'),
    ("CV.12", "CWE-476: NULL Pointer Dereference", 2, 'CV'),
    ("CV.13", "CWE-190: Integer Overflow", 2, 'CV'),
    ("CV.14", "CWE-502: Deserialization", 2, 'CV'),
    ("CV.15", "CWE-287: Improper Authentication", 2, 'CV'),
]

async def ingest_cve_types(conn) -> int:
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
