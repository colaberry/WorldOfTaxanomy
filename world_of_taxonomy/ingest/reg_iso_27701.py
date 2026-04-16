"""Ingest ISO/IEC 27701:2019 Privacy Information Management - Extension to ISO/IEC 27001."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_27701",
    "ISO/IEC 27701:2019",
    "ISO/IEC 27701:2019 Privacy Information Management - Extension to ISO/IEC 27001",
    "2019",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/71670.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cl_5", "Clause 5 - PIMS-Specific Requirements (ISO 27001)", 1, None),
    ("cl_6", "Clause 6 - PIMS-Specific Guidance (ISO 27002)", 1, None),
    ("cl_7", "Clause 7 - Additional Guidance for PII Controllers", 1, None),
    ("cl_8", "Clause 8 - Additional Guidance for PII Processors", 1, None),
    ("annex_a", "Annex A - PIMS-Specific Reference Control Objectives (Controllers)", 1, None),
    ("annex_b", "Annex B - PIMS-Specific Reference Control Objectives (Processors)", 1, None),
    ("5_1", "5.1 General", 2, "cl_5"),
    ("5_2", "5.2 Context of the organization", 2, "cl_5"),
    ("5_3", "5.3 Leadership", 2, "cl_5"),
    ("5_4", "5.4 Planning", 2, "cl_5"),
    ("5_5", "5.5 Support", 2, "cl_5"),
    ("5_6", "5.6 Operation", 2, "cl_5"),
    ("5_7", "5.7 Performance evaluation", 2, "cl_5"),
    ("5_8", "5.8 Improvement", 2, "cl_5"),
    ("7_1", "7.1 General", 2, "cl_7"),
    ("7_2", "7.2 Conditions for collection and processing", 2, "cl_7"),
    ("7_3", "7.3 Obligations to PII principals", 2, "cl_7"),
    ("7_4", "7.4 Privacy by design and privacy by default", 2, "cl_7"),
    ("7_5", "7.5 PII sharing, transfer and disclosure", 2, "cl_7"),
    ("8_1", "8.1 General", 2, "cl_8"),
    ("8_2", "8.2 Conditions for collection and processing", 2, "cl_8"),
    ("8_3", "8.3 Obligations to PII principals", 2, "cl_8"),
    ("8_4", "8.4 Privacy by design and privacy by default", 2, "cl_8"),
    ("8_5", "8.5 PII sharing, transfer and disclosure", 2, "cl_8"),
    ("a1", "A.7 Additional ISO 27002 guidance for PII controllers", 2, "annex_a"),
    ("a2", "A.8 Additional conditions for processing", 2, "annex_a"),
    ("b1", "B.8 Additional ISO 27002 guidance for PII processors", 2, "annex_b"),
]


async def ingest_reg_iso_27701(conn) -> int:
    """Insert or update ISO/IEC 27701:2019 system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_27701"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_27701", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_27701",
    )
    return count
