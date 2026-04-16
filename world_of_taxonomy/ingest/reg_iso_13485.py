"""Ingest ISO 13485:2016 Medical Devices - Quality Management Systems."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_13485",
    "ISO 13485:2016",
    "ISO 13485:2016 Medical Devices - Quality Management Systems",
    "2016",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/59752.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cl_4", "Clause 4 - Quality Management System", 1, None),
    ("cl_5", "Clause 5 - Management Responsibility", 1, None),
    ("cl_6", "Clause 6 - Resource Management", 1, None),
    ("cl_7", "Clause 7 - Product Realization", 1, None),
    ("cl_8", "Clause 8 - Measurement, Analysis and Improvement", 1, None),
    ("4_1", "4.1 General requirements", 2, "cl_4"),
    ("4_2", "4.2 Documentation requirements", 2, "cl_4"),
    ("5_1", "5.1 Management commitment", 2, "cl_5"),
    ("5_2", "5.2 Customer focus", 2, "cl_5"),
    ("5_3", "5.3 Quality policy", 2, "cl_5"),
    ("5_4", "5.4 Planning", 2, "cl_5"),
    ("5_5", "5.5 Responsibility, authority and communication", 2, "cl_5"),
    ("5_6", "5.6 Management review", 2, "cl_5"),
    ("6_1", "6.1 Provision of resources", 2, "cl_6"),
    ("6_2", "6.2 Human resources", 2, "cl_6"),
    ("6_3", "6.3 Infrastructure", 2, "cl_6"),
    ("6_4", "6.4 Work environment and contamination control", 2, "cl_6"),
    ("7_1", "7.1 Planning of product realization", 2, "cl_7"),
    ("7_2", "7.2 Customer-related processes", 2, "cl_7"),
    ("7_3", "7.3 Design and development", 2, "cl_7"),
    ("7_4", "7.4 Purchasing", 2, "cl_7"),
    ("7_5", "7.5 Production and service provision", 2, "cl_7"),
    ("7_6", "7.6 Control of monitoring and measuring equipment", 2, "cl_7"),
    ("8_1", "8.1 General", 2, "cl_8"),
    ("8_2", "8.2 Monitoring and measurement", 2, "cl_8"),
    ("8_3", "8.3 Control of nonconforming product", 2, "cl_8"),
    ("8_4", "8.4 Analysis of data", 2, "cl_8"),
    ("8_5", "8.5 Improvement", 2, "cl_8"),
]


async def ingest_reg_iso_13485(conn) -> int:
    """Insert or update ISO 13485:2016 system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_13485"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_13485", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_13485",
    )
    return count
