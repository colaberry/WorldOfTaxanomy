"""Ingest OECD Guidelines for Multinational Enterprises on Responsible Business Conduct."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_oecd_mne",
    "OECD MNE Guidelines",
    "OECD Guidelines for Multinational Enterprises on Responsible Business Conduct",
    "2023",
    "Global",
    "Organisation for Economic Co-operation and Development (OECD)",
)
_SOURCE_URL = "https://www.oecd.org/en/publications/oecd-guidelines-for-multinational-enterprises-on-responsible-business-conduct_81f92357-en.html"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (OECD)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ch_1", "Chapter I - Concepts and Principles", 1, None),
    ("ch_2", "Chapter II - General Policies", 1, None),
    ("ch_3", "Chapter III - Disclosure", 1, None),
    ("ch_4", "Chapter IV - Human Rights", 1, None),
    ("ch_5", "Chapter V - Employment and Industrial Relations", 1, None),
    ("ch_6", "Chapter VI - Environment", 1, None),
    ("ch_7", "Chapter VII - Combating Bribery", 1, None),
    ("ch_8", "Chapter VIII - Consumer Interests", 1, None),
    ("ch_9", "Chapter IX - Science and Technology", 1, None),
    ("ch_10", "Chapter X - Competition", 1, None),
    ("ch_11", "Chapter XI - Taxation", 1, None),
    ("ch_12", "Chapter XII - Digital Considerations", 1, None),
    ("dd", "Due Diligence Guidance", 1, None),
    ("dd_1", "Step 1 - Embed RBC into policies and management systems", 2, "dd"),
    ("dd_2", "Step 2 - Identify and assess adverse impacts", 2, "dd"),
    ("dd_3", "Step 3 - Cease, prevent or mitigate adverse impacts", 2, "dd"),
    ("dd_4", "Step 4 - Track implementation and results", 2, "dd"),
    ("dd_5", "Step 5 - Communicate", 2, "dd"),
    ("dd_6", "Step 6 - Provide for or cooperate in remediation", 2, "dd"),
    ("ncp", "National Contact Points (NCPs)", 1, None),
    ("ncp_1", "NCP Core criteria for functional equivalence", 2, "ncp"),
    ("ncp_2", "NCP Specific instances (complaint mechanism)", 2, "ncp"),
]


async def ingest_reg_oecd_mne(conn) -> int:
    """Insert or update OECD MNE Guidelines system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_oecd_mne"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_oecd_mne", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_oecd_mne",
    )
    return count
