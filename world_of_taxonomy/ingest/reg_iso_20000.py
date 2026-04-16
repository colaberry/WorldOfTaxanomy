"""Ingest ISO/IEC 20000-1:2018 IT Service Management System - Requirements."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_20000",
    "ISO/IEC 20000-1:2018",
    "ISO/IEC 20000-1:2018 IT Service Management System - Requirements",
    "2018",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/70636.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cl_4", "Clause 4 - Context of the Organization", 1, None),
    ("cl_5", "Clause 5 - Leadership", 1, None),
    ("cl_6", "Clause 6 - Planning", 1, None),
    ("cl_7", "Clause 7 - Support of the SMS", 1, None),
    ("cl_8", "Clause 8 - Operation of the SMS", 1, None),
    ("cl_9", "Clause 9 - Performance Evaluation", 1, None),
    ("cl_10", "Clause 10 - Improvement", 1, None),
    ("4_1", "4.1 Understanding the organization and its context", 2, "cl_4"),
    ("4_2", "4.2 Understanding needs and expectations of interested parties", 2, "cl_4"),
    ("5_1", "5.1 Leadership and commitment", 2, "cl_5"),
    ("5_2", "5.2 Policy", 2, "cl_5"),
    ("8_1", "8.1 Operational planning and control", 2, "cl_8"),
    ("8_2", "8.2 Service portfolio", 2, "cl_8"),
    ("8_3", "8.3 Relationship and agreement", 2, "cl_8"),
    ("8_4", "8.4 Supply and demand", 2, "cl_8"),
    ("8_5", "8.5 Service design, build and transition", 2, "cl_8"),
    ("8_6", "8.6 Resolution and fulfilment", 2, "cl_8"),
    ("8_7", "8.7 Service assurance", 2, "cl_8"),
    ("9_1", "9.1 Monitoring, measurement, analysis and evaluation", 2, "cl_9"),
    ("9_2", "9.2 Internal audit", 2, "cl_9"),
    ("9_3", "9.3 Management review", 2, "cl_9"),
    ("10_1", "10.1 Nonconformity and corrective action", 2, "cl_10"),
    ("10_2", "10.2 Continual improvement", 2, "cl_10"),
]


async def ingest_reg_iso_20000(conn) -> int:
    """Insert or update ISO/IEC 20000-1:2018 system and its nodes. Returns node count."""
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0, source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance, license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute(
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_20000"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_20000", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_20000",
    )
    return count
