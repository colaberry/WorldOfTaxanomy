"""Ingest ISO/IEC 42001:2023 Artificial Intelligence Management System."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_42001",
    "ISO/IEC 42001:2023",
    "ISO/IEC 42001:2023 Artificial Intelligence Management System",
    "2023",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/81230.html"
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
    ("annex_a", "Annex A - AI Controls", 1, None),
    ("annex_b", "Annex B - AI Impact Assessment Guidance", 1, None),
    ("4_1", "4.1 Understanding the organization and its context", 2, "cl_4"),
    ("4_2", "4.2 Understanding needs and expectations of interested parties", 2, "cl_4"),
    ("4_3", "4.3 Determining the scope of the AIMS", 2, "cl_4"),
    ("5_1", "5.1 Leadership and commitment", 2, "cl_5"),
    ("5_2", "5.2 AI policy", 2, "cl_5"),
    ("5_3", "5.3 Roles, responsibilities and authorities", 2, "cl_5"),
    ("6_1", "6.1 Actions to address risks and opportunities", 2, "cl_6"),
    ("6_2", "6.2 AI objectives and planning to achieve them", 2, "cl_6"),
    ("8_1", "8.1 Operational planning and control", 2, "cl_8"),
    ("8_2", "8.2 AI risk assessment", 2, "cl_8"),
    ("8_3", "8.3 AI risk treatment", 2, "cl_8"),
    ("8_4", "8.4 AI system impact assessment", 2, "cl_8"),
    ("a2", "A.2 Policies related to AI", 2, "annex_a"),
    ("a3", "A.3 Internal organization", 2, "annex_a"),
    ("a4", "A.4 Resources for AI systems", 2, "annex_a"),
    ("a5", "A.5 Assessing impacts of AI systems", 2, "annex_a"),
    ("a6", "A.6 AI system lifecycle", 2, "annex_a"),
    ("a7", "A.7 Data for AI systems", 2, "annex_a"),
    ("a8", "A.8 Information for interested parties of AI systems", 2, "annex_a"),
    ("a9", "A.9 Use of AI systems", 2, "annex_a"),
    ("a10", "A.10 Third-party and customer relationships", 2, "annex_a"),
    ("b1", "B.1 Overview of AI impact assessment", 2, "annex_b"),
    ("b2", "B.2 AI system impact assessment process", 2, "annex_b"),
]


async def ingest_reg_iso_42001(conn) -> int:
    """Insert or update ISO/IEC 42001:2023 system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_42001"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_42001", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_42001",
    )
    return count
