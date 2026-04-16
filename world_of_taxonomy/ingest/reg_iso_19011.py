"""Ingest ISO 19011:2018 Guidelines for Auditing Management Systems."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_19011",
    "ISO 19011:2018",
    "ISO 19011:2018 Guidelines for Auditing Management Systems",
    "2018",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/70017.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cl_4", "Clause 4 - Principles of Auditing", 1, None),
    ("cl_5", "Clause 5 - Managing an Audit Programme", 1, None),
    ("cl_6", "Clause 6 - Performing an Audit", 1, None),
    ("cl_7", "Clause 7 - Competence and Evaluation of Auditors", 1, None),
    ("4_1", "4.1 Integrity", 2, "cl_4"),
    ("4_2", "4.2 Fair presentation", 2, "cl_4"),
    ("4_3", "4.3 Due professional care", 2, "cl_4"),
    ("4_4", "4.4 Confidentiality", 2, "cl_4"),
    ("4_5", "4.5 Independence", 2, "cl_4"),
    ("4_6", "4.6 Evidence-based approach", 2, "cl_4"),
    ("4_7", "4.7 Risk-based approach", 2, "cl_4"),
    ("5_1", "5.1 General", 2, "cl_5"),
    ("5_2", "5.2 Establishing audit programme objectives", 2, "cl_5"),
    ("5_3", "5.3 Determining and evaluating audit programme risks and opportunities", 2, "cl_5"),
    ("5_4", "5.4 Establishing the audit programme", 2, "cl_5"),
    ("5_5", "5.5 Implementing the audit programme", 2, "cl_5"),
    ("5_6", "5.6 Monitoring the audit programme", 2, "cl_5"),
    ("5_7", "5.7 Reviewing and improving the audit programme", 2, "cl_5"),
    ("6_1", "6.1 General", 2, "cl_6"),
    ("6_2", "6.2 Initiating the audit", 2, "cl_6"),
    ("6_3", "6.3 Preparing audit activities", 2, "cl_6"),
    ("6_4", "6.4 Conducting audit activities", 2, "cl_6"),
    ("6_5", "6.5 Preparing and distributing the audit report", 2, "cl_6"),
    ("6_6", "6.6 Completing the audit", 2, "cl_6"),
    ("6_7", "6.7 Conducting audit follow-up", 2, "cl_6"),
    ("7_1", "7.1 General", 2, "cl_7"),
    ("7_2", "7.2 Determining auditor competence", 2, "cl_7"),
    ("7_3", "7.3 Selecting appropriate audit team members", 2, "cl_7"),
    ("7_4", "7.4 Evaluating auditors", 2, "cl_7"),
    ("7_5", "7.5 Maintaining and improving auditor competence", 2, "cl_7"),
]


async def ingest_reg_iso_19011(conn) -> int:
    """Insert or update ISO 19011:2018 system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_19011"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_19011", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_19011",
    )
    return count
