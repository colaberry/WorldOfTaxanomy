"""Ingest Task Force on Climate-Related Financial Disclosures."""
from __future__ import annotations

_SYSTEM_ROW = ("tcfd", "TCFD Recommendations", "Task Force on Climate-Related Financial Disclosures", "2017", "Global", "FSB/TCFD")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("TF", "TCFD Pillars", 1, None),
    ("TF.01", "Governance: Board Oversight", 2, 'TF'),
    ("TF.02", "Governance: Management Role", 2, 'TF'),
    ("TF.03", "Strategy: Risks and Opportunities", 2, 'TF'),
    ("TF.04", "Strategy: Impact on Business", 2, 'TF'),
    ("TF.05", "Strategy: Resilience (Scenarios)", 2, 'TF'),
    ("TF.06", "Risk Management: Identifying Risks", 2, 'TF'),
    ("TF.07", "Risk Management: Managing Risks", 2, 'TF'),
    ("TF.08", "Risk Management: Integration", 2, 'TF'),
    ("TF.09", "Metrics: GHG Emissions", 2, 'TF'),
    ("TF.10", "Metrics: Climate-Related Risks", 2, 'TF'),
    ("TF.11", "Metrics: Targets", 2, 'TF'),
    ("TF.12", "Physical Risk Assessment", 2, 'TF'),
    ("TF.13", "Transition Risk Assessment", 2, 'TF'),
]

async def ingest_tcfd(conn) -> int:
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
