"""Ingest SIEM Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_siem", "SIEM Type", "SIEM Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SI", "SIEM Types", 1, None),
    ("SI.01", "Log Collection and Aggregation", 2, 'SI'),
    ("SI.02", "Real-Time Correlation", 2, 'SI'),
    ("SI.03", "Threat Detection Rules", 2, 'SI'),
    ("SI.04", "User Behavior Analytics (UBA)", 2, 'SI'),
    ("SI.05", "Entity Behavior Analytics", 2, 'SI'),
    ("SI.06", "Security Data Lake", 2, 'SI'),
    ("SI.07", "SIEM as a Service (Cloud)", 2, 'SI'),
    ("SI.08", "On-Premises SIEM", 2, 'SI'),
    ("SI.09", "Hybrid SIEM", 2, 'SI'),
    ("SI.10", "Compliance Reporting", 2, 'SI'),
    ("SI.11", "Incident Timeline Reconstruction", 2, 'SI'),
    ("SI.12", "Forensic Investigation", 2, 'SI'),
    ("SI.13", "Alert Triage and Prioritization", 2, 'SI'),
    ("SI.14", "SIEM Integration (API)", 2, 'SI'),
]

async def ingest_domain_siem(conn) -> int:
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
