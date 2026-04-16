"""Ingest GDPR Lawful Basis for Processing."""
from __future__ import annotations

_SYSTEM_ROW = ("gdpr_basis", "GDPR Legal Bases", "GDPR Lawful Basis for Processing", "2018", "European Union", "European Commission")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GB", "GDPR Legal Bases", 1, None),
    ("GB.01", "Consent (Art. 6(1)(a))", 2, 'GB'),
    ("GB.02", "Contract (Art. 6(1)(b))", 2, 'GB'),
    ("GB.03", "Legal Obligation (Art. 6(1)(c))", 2, 'GB'),
    ("GB.04", "Vital Interests (Art. 6(1)(d))", 2, 'GB'),
    ("GB.05", "Public Task (Art. 6(1)(e))", 2, 'GB'),
    ("GB.06", "Legitimate Interests (Art. 6(1)(f))", 2, 'GB'),
    ("GB.07", "Special Category: Explicit Consent", 2, 'GB'),
    ("GB.08", "Special Category: Employment", 2, 'GB'),
    ("GB.09", "Special Category: Vital Interests", 2, 'GB'),
    ("GB.10", "Special Category: Public Interest", 2, 'GB'),
    ("GB.11", "Special Category: Legal Claims", 2, 'GB'),
    ("GB.12", "Special Category: Substantial Public Interest", 2, 'GB'),
    ("GB.13", "Special Category: Health/Social Care", 2, 'GB'),
    ("GB.14", "Special Category: Archiving/Research", 2, 'GB'),
    ("GB.15", "Criminal Convictions (Art. 10)", 2, 'GB'),
]

async def ingest_gdpr_basis(conn) -> int:
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
