"""Ingest ISO 14001:2015 Environmental Management Systems - Requirements."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_14001",
    "ISO 14001:2015",
    "ISO 14001:2015 Environmental Management Systems - Requirements",
    "2015",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/60857.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cl_4", "Clause 4 - Context of the Organization", 1, None),
    ("cl_5", "Clause 5 - Leadership", 1, None),
    ("cl_6", "Clause 6 - Planning", 1, None),
    ("cl_7", "Clause 7 - Support", 1, None),
    ("cl_8", "Clause 8 - Operation", 1, None),
    ("cl_9", "Clause 9 - Performance Evaluation", 1, None),
    ("cl_10", "Clause 10 - Improvement", 1, None),
    ("4_1", "4.1 Understanding the organization and its context", 2, "cl_4"),
    ("4_2", "4.2 Understanding needs and expectations of interested parties", 2, "cl_4"),
    ("4_3", "4.3 Determining the scope of the EMS", 2, "cl_4"),
    ("4_4", "4.4 Environmental management system", 2, "cl_4"),
    ("5_1", "5.1 Leadership and commitment", 2, "cl_5"),
    ("5_2", "5.2 Environmental policy", 2, "cl_5"),
    ("5_3", "5.3 Organizational roles, responsibilities and authorities", 2, "cl_5"),
    ("6_1", "6.1 Actions to address risks and opportunities", 2, "cl_6"),
    ("6_2", "6.2 Environmental objectives and planning to achieve them", 2, "cl_6"),
    ("7_1", "7.1 Resources", 2, "cl_7"),
    ("7_2", "7.2 Competence", 2, "cl_7"),
    ("7_3", "7.3 Awareness", 2, "cl_7"),
    ("7_4", "7.4 Communication", 2, "cl_7"),
    ("7_5", "7.5 Documented information", 2, "cl_7"),
    ("8_1", "8.1 Operational planning and control", 2, "cl_8"),
    ("8_2", "8.2 Emergency preparedness and response", 2, "cl_8"),
    ("9_1", "9.1 Monitoring, measurement, analysis and evaluation", 2, "cl_9"),
    ("9_2", "9.2 Internal audit", 2, "cl_9"),
    ("9_3", "9.3 Management review", 2, "cl_9"),
    ("10_1", "10.1 General", 2, "cl_10"),
    ("10_2", "10.2 Nonconformity and corrective action", 2, "cl_10"),
    ("10_3", "10.3 Continual improvement", 2, "cl_10"),
]


async def ingest_reg_iso_14001(conn) -> int:
    """Insert or update ISO 14001:2015 system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_14001"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_14001", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_14001",
    )
    return count
