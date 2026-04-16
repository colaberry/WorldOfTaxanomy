"""Ingest Kimberley Process Certification Scheme."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_kimberley",
    "Kimberley Process",
    "Kimberley Process Certification Scheme",
    "2003",
    "Global",
    "Kimberley Process",
)
_SOURCE_URL = "https://www.kimberleyprocess.com/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("sec_1", "Section I - Definitions", 1, None),
    ("sec_2", "Section II - Kimberley Process Certificate", 1, None),
    ("sec_3", "Section III - Undertakings by Participants", 1, None),
    ("sec_4", "Section IV - Internal Controls", 1, None),
    ("sec_5", "Section V - Cooperation and Transparency", 1, None),
    ("sec_6", "Section VI - Administrative Matters", 1, None),
    ("2_1", "Requirements for Kimberley Process Certificate", 2, "sec_2"),
    ("2_2", "Certificate content and format", 2, "sec_2"),
    ("3_1", "Import requirements", 2, "sec_3"),
    ("3_2", "Export requirements", 2, "sec_3"),
    ("3_3", "Shipment controls", 2, "sec_3"),
    ("4_1", "Internal controls for rough diamonds", 2, "sec_4"),
    ("4_2", "System of warranties for polished diamonds", 2, "sec_4"),
    ("4_3", "Record keeping and auditing", 2, "sec_4"),
    ("5_1", "Peer review mechanism", 2, "sec_5"),
    ("5_2", "Voluntary review visits", 2, "sec_5"),
    ("5_3", "Annual reporting", 2, "sec_5"),
]


async def ingest_reg_kimberley(conn) -> int:
    """Insert or update Kimberley Process system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_kimberley"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_kimberley", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_kimberley",
    )
    return count
