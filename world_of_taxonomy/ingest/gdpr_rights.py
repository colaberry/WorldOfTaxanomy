"""Ingest GDPR Individual Rights Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("gdpr_rights", "GDPR Data Subject Rights", "GDPR Individual Rights Categories", "2018", "European Union", "European Commission")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GR", "GDPR Rights", 1, None),
    ("GR.01", "Right to be Informed (Art. 13-14)", 2, 'GR'),
    ("GR.02", "Right of Access (Art. 15)", 2, 'GR'),
    ("GR.03", "Right to Rectification (Art. 16)", 2, 'GR'),
    ("GR.04", "Right to Erasure (Art. 17)", 2, 'GR'),
    ("GR.05", "Right to Restrict Processing (Art. 18)", 2, 'GR'),
    ("GR.06", "Right to Data Portability (Art. 20)", 2, 'GR'),
    ("GR.07", "Right to Object (Art. 21)", 2, 'GR'),
    ("GR.08", "Rights re: Automated Decision Making (Art. 22)", 2, 'GR'),
    ("GR.09", "Right to Withdraw Consent", 2, 'GR'),
    ("GR.10", "Right to Lodge Complaint (Art. 77)", 2, 'GR'),
    ("GR.11", "Right to Effective Remedy (Art. 79)", 2, 'GR'),
    ("GR.12", "Right to Compensation (Art. 82)", 2, 'GR'),
]

async def ingest_gdpr_rights(conn) -> int:
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
