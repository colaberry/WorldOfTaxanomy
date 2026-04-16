"""Ingest ISO 26000:2010 Guidance on Social Responsibility."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_iso_26000",
    "ISO 26000:2010",
    "ISO 26000:2010 Guidance on Social Responsibility",
    "2010",
    "Global",
    "International Organization for Standardization (ISO)",
)
_SOURCE_URL = "https://www.iso.org/standard/42546.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary (ISO copyright)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("cl_4", "Clause 4 - Principles of Social Responsibility", 1, None),
    ("cl_5", "Clause 5 - Recognizing Social Responsibility", 1, None),
    ("cl_6", "Clause 6 - Core Subjects of Social Responsibility", 1, None),
    ("cl_7", "Clause 7 - Guidance on Integrating SR", 1, None),
    ("4_1", "4.1 Accountability", 2, "cl_4"),
    ("4_2", "4.2 Transparency", 2, "cl_4"),
    ("4_3", "4.3 Ethical behaviour", 2, "cl_4"),
    ("4_4", "4.4 Respect for stakeholder interests", 2, "cl_4"),
    ("4_5", "4.5 Respect for the rule of law", 2, "cl_4"),
    ("4_6", "4.6 Respect for international norms of behaviour", 2, "cl_4"),
    ("4_7", "4.7 Respect for human rights", 2, "cl_4"),
    ("6_2", "6.2 Organizational governance", 2, "cl_6"),
    ("6_3", "6.3 Human rights", 2, "cl_6"),
    ("6_4", "6.4 Labour practices", 2, "cl_6"),
    ("6_5", "6.5 The environment", 2, "cl_6"),
    ("6_6", "6.6 Fair operating practices", 2, "cl_6"),
    ("6_7", "6.7 Consumer issues", 2, "cl_6"),
    ("6_8", "6.8 Community involvement and development", 2, "cl_6"),
    ("7_1", "7.1 Relationship of an organization to social responsibility", 2, "cl_7"),
    ("7_2", "7.2 Understanding an organization and its context for SR", 2, "cl_7"),
    ("7_3", "7.3 Practices for integrating social responsibility", 2, "cl_7"),
    ("7_4", "7.4 Communication on social responsibility", 2, "cl_7"),
]


async def ingest_reg_iso_26000(conn) -> int:
    """Insert or update ISO 26000:2010 system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_iso_26000"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_iso_26000", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_iso_26000",
    )
    return count
