"""Ingest WHO Framework Convention on Tobacco Control."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_who_fctc",
    "WHO FCTC",
    "WHO Framework Convention on Tobacco Control",
    "2003",
    "Global",
    "World Health Organization (WHO)",
)
_SOURCE_URL = "https://fctc.who.int/"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (WHO)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pt_1", "Part I - Introduction", 1, None),
    ("pt_2", "Part II - Objective and Guiding Principles", 1, None),
    ("pt_3", "Part III - Measures Relating to Reduction of Demand", 1, None),
    ("pt_4", "Part IV - Measures Relating to Reduction of Supply", 1, None),
    ("pt_5", "Part V - Protection of the Environment", 1, None),
    ("art_3", "Art 3 - Objective", 2, "pt_2"),
    ("art_4", "Art 4 - Guiding principles", 2, "pt_2"),
    ("art_6", "Art 6 - Price and tax measures to reduce demand", 2, "pt_3"),
    ("art_8", "Art 8 - Protection from exposure to tobacco smoke", 2, "pt_3"),
    ("art_9", "Art 9 - Regulation of contents of tobacco products", 2, "pt_3"),
    ("art_11", "Art 11 - Packaging and labelling", 2, "pt_3"),
    ("art_12", "Art 12 - Education, communication, training and public awareness", 2, "pt_3"),
    ("art_13", "Art 13 - Tobacco advertising, promotion and sponsorship", 2, "pt_3"),
    ("art_14", "Art 14 - Demand reduction measures concerning tobacco dependence", 2, "pt_3"),
    ("art_15", "Art 15 - Illicit trade in tobacco products", 2, "pt_4"),
    ("art_16", "Art 16 - Sales to and by minors", 2, "pt_4"),
    ("art_17", "Art 17 - Provision of support for economically viable alternatives", 2, "pt_4"),
    ("art_18", "Art 18 - Protection of the environment and health of persons", 2, "pt_5"),
]


async def ingest_reg_who_fctc(conn) -> int:
    """Insert or update WHO FCTC system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_who_fctc"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_who_fctc", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_who_fctc",
    )
    return count
