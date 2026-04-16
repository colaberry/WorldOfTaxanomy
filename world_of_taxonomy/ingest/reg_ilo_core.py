"""Ingest ILO Fundamental Principles and Rights at Work - Core Conventions."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_ilo_core",
    "ILO Core Conventions",
    "ILO Fundamental Principles and Rights at Work - Core Conventions",
    "2022",
    "Global",
    "International Labour Organization (ILO)",
)
_SOURCE_URL = "https://www.ilo.org/global/standards/introduction-to-international-labour-standards/conventions-and-recommendations"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (UN)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("free", "Freedom of Association", 1, None),
    ("forced", "Abolition of Forced Labour", 1, None),
    ("child", "Abolition of Child Labour", 1, None),
    ("disc", "Elimination of Discrimination", 1, None),
    ("ohs", "Occupational Safety and Health", 1, None),
    ("c87", "C087 - Freedom of Association and Protection of the Right to Organise", 2, "free"),
    ("c98", "C098 - Right to Organise and Collective Bargaining", 2, "free"),
    ("c29", "C029 - Forced Labour Convention", 2, "forced"),
    ("c105", "C105 - Abolition of Forced Labour Convention", 2, "forced"),
    ("p29", "P029 - Protocol to the Forced Labour Convention", 2, "forced"),
    ("c138", "C138 - Minimum Age Convention", 2, "child"),
    ("c182", "C182 - Worst Forms of Child Labour Convention", 2, "child"),
    ("c100", "C100 - Equal Remuneration Convention", 2, "disc"),
    ("c111", "C111 - Discrimination (Employment and Occupation) Convention", 2, "disc"),
    ("c155", "C155 - Occupational Safety and Health Convention", 2, "ohs"),
    ("c187", "C187 - Promotional Framework for Occupational Safety and Health Convention", 2, "ohs"),
]


async def ingest_reg_ilo_core(conn) -> int:
    """Insert or update ILO Core Conventions system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_ilo_core"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_ilo_core", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_ilo_core",
    )
    return count
