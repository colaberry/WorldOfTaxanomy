"""Ingest IRS Tax Form Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("irs_forms", "IRS Form Types", "IRS Tax Form Categories", "2024", "United States", "IRS")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IF", "IRS Form Categories", 1, None),
    ("IF.01", "Individual Income Tax (1040 series)", 2, 'IF'),
    ("IF.02", "Corporate Tax (1120 series)", 2, 'IF'),
    ("IF.03", "Partnership Returns (1065)", 2, 'IF'),
    ("IF.04", "Employment Tax (940/941)", 2, 'IF'),
    ("IF.05", "Excise Tax (720 series)", 2, 'IF'),
    ("IF.06", "Estate and Gift Tax (706/709)", 2, 'IF'),
    ("IF.07", "Information Returns (1099 series)", 2, 'IF'),
    ("IF.08", "Exempt Organizations (990 series)", 2, 'IF'),
    ("IF.09", "International (5471/5472/8865)", 2, 'IF'),
    ("IF.10", "Estimated Tax (1040-ES)", 2, 'IF'),
    ("IF.11", "Withholding (W-2/W-4)", 2, 'IF'),
    ("IF.12", "Retirement Plans (5500)", 2, 'IF'),
    ("IF.13", "Tax Credits (various)", 2, 'IF'),
    ("IF.14", "Extensions (4868/7004)", 2, 'IF'),
]

async def ingest_irs_forms(conn) -> int:
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
