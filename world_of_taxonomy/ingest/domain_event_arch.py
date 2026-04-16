"""Ingest Event-Driven Architecture Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_event_arch", "Event-Driven Architecture", "Event-Driven Architecture Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EA", "Event-Driven Architecture Types", 1, None),
    ("EA.01", "Message Queue (Point-to-Point)", 2, 'EA'),
    ("EA.02", "Publish-Subscribe", 2, 'EA'),
    ("EA.03", "Event Streaming (Kafka)", 2, 'EA'),
    ("EA.04", "Event Bus", 2, 'EA'),
    ("EA.05", "Event Sourcing", 2, 'EA'),
    ("EA.06", "Complex Event Processing", 2, 'EA'),
    ("EA.07", "Event Notification", 2, 'EA'),
    ("EA.08", "Event-Carried State Transfer", 2, 'EA'),
    ("EA.09", "Dead Letter Queue", 2, 'EA'),
    ("EA.10", "Event Schema Registry", 2, 'EA'),
    ("EA.11", "Event Replay", 2, 'EA'),
    ("EA.12", "Exactly-Once Delivery", 2, 'EA'),
    ("EA.13", "Fan-Out Pattern", 2, 'EA'),
    ("EA.14", "Event Bridge", 2, 'EA'),
    ("EA.15", "Change Data Capture", 2, 'EA'),
]

async def ingest_domain_event_arch(conn) -> int:
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
