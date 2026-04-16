"""Ingest Codex Alimentarius - International Food Standards."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_codex",
    "Codex Alimentarius",
    "Codex Alimentarius - International Food Standards",
    "2023",
    "Global",
    "FAO/WHO Codex Alimentarius Commission",
)
_SOURCE_URL = "https://www.fao.org/fao-who-codexalimentarius"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (FAO/WHO)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("gen", "General Subject Committees", 1, None),
    ("comm", "Commodity Committees", 1, None),
    ("coord", "Regional Coordinating Committees", 1, None),
    ("gen_1", "CCFH - Committee on Food Hygiene", 2, "gen"),
    ("gen_2", "CCFL - Committee on Food Labelling", 2, "gen"),
    ("gen_3", "CCFA - Committee on Food Additives", 2, "gen"),
    ("gen_4", "CCPR - Committee on Pesticide Residues", 2, "gen"),
    ("gen_5", "CCRVDF - Committee on Residues of Veterinary Drugs in Foods", 2, "gen"),
    ("gen_6", "CCMAS - Committee on Methods of Analysis and Sampling", 2, "gen"),
    ("gen_7", "CCNFSDU - Committee on Nutrition and Foods for Special Dietary Uses", 2, "gen"),
    ("gen_8", "CCCF - Committee on Contaminants in Foods", 2, "gen"),
    ("gen_9", "CCFICS - Committee on Food Import and Export Inspection and Certification", 2, "gen"),
    ("comm_1", "CCFO - Committee on Fats and Oils", 2, "comm"),
    ("comm_2", "CCFFP - Committee on Fish and Fishery Products", 2, "comm"),
    ("comm_3", "CCS - Committee on Spices and Culinary Herbs", 2, "comm"),
    ("comm_4", "CCSCH - Committee on Sugars", 2, "comm"),
    ("coord_1", "CCAFRICA - Coordinating Committee for Africa", 2, "coord"),
    ("coord_2", "CCASIA - Coordinating Committee for Asia", 2, "coord"),
    ("coord_3", "CCEURO - Coordinating Committee for Europe", 2, "coord"),
    ("coord_4", "CCLAC - Coordinating Committee for Latin America", 2, "coord"),
    ("coord_5", "CCNASWP - Coordinating Committee for North America and South West Pacific", 2, "coord"),
    ("coord_6", "CCNE - Coordinating Committee for Near East", 2, "coord"),
]


async def ingest_reg_codex(conn) -> int:
    """Insert or update Codex Alimentarius system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_codex"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_codex", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_codex",
    )
    return count
