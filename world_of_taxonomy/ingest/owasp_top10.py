"""Ingest OWASP Top 10 Web Application Security Risks."""
from __future__ import annotations

_SYSTEM_ROW = ("owasp_top10", "OWASP Top 10", "OWASP Top 10 Web Application Security Risks", "2021", "Global", "OWASP Foundation")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY-SA 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("OW", "OWASP Top 10", 1, None),
    ("OW.01", "A01: Broken Access Control", 2, 'OW'),
    ("OW.02", "A02: Cryptographic Failures", 2, 'OW'),
    ("OW.03", "A03: Injection", 2, 'OW'),
    ("OW.04", "A04: Insecure Design", 2, 'OW'),
    ("OW.05", "A05: Security Misconfiguration", 2, 'OW'),
    ("OW.06", "A06: Vulnerable and Outdated Components", 2, 'OW'),
    ("OW.07", "A07: Identification and Authentication Failures", 2, 'OW'),
    ("OW.08", "A08: Software and Data Integrity Failures", 2, 'OW'),
    ("OW.09", "A09: Security Logging and Monitoring Failures", 2, 'OW'),
    ("OW.10", "A10: Server-Side Request Forgery (SSRF)", 2, 'OW'),
]

async def ingest_owasp_top10(conn) -> int:
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
