"""Ingest Berne Convention for the Protection of Literary and Artistic Works."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_berne",
    "Berne Convention",
    "Berne Convention for the Protection of Literary and Artistic Works",
    "1979",
    "Global",
    "World Intellectual Property Organization (WIPO)",
)
_SOURCE_URL = "https://www.wipo.int/treaties/en/ip/berne/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (WIPO)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("art_1", "Article 1 - Establishment of a Union", 1, None),
    ("art_2", "Article 2 - Protected Works", 1, None),
    ("art_3", "Article 3 - Criteria of Eligibility for Protection", 1, None),
    ("art_5", "Article 5 - Rights Guaranteed (National Treatment and Automatic Protection)", 1, None),
    ("art_6bis", "Article 6bis - Moral Rights", 1, None),
    ("art_7", "Article 7 - Term of Protection", 1, None),
    ("art_8", "Article 8 - Right of Translation", 1, None),
    ("art_9", "Article 9 - Right of Reproduction", 1, None),
    ("art_10", "Article 10 - Exceptions (Quotations, Teaching)", 1, None),
    ("art_11", "Article 11 - Right of Public Performance", 1, None),
    ("art_11bis", "Article 11bis - Right of Broadcasting", 1, None),
    ("art_12", "Article 12 - Right of Adaptation", 1, None),
    ("art_14", "Article 14 - Cinematographic Rights", 1, None),
    ("art_14bis", "Article 14bis - Special Provisions for Cinematographic Works", 1, None),
    ("art_14ter", "Article 14ter - Droit de Suite (Resale Right)", 1, None),
    ("app", "Appendix - Special Provisions for Developing Countries", 1, None),
    ("app_1", "Compulsory license for translation", 2, "app"),
    ("app_2", "Compulsory license for reproduction", 2, "app"),
]


async def ingest_reg_berne(conn) -> int:
    """Insert or update Berne Convention system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_berne"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_berne", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_berne",
    )
    return count
