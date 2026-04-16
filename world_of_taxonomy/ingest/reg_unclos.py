"""Ingest United Nations Convention on the Law of the Sea."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_unclos",
    "UNCLOS",
    "United Nations Convention on the Law of the Sea",
    "1982",
    "Global",
    "United Nations",
)
_SOURCE_URL = "https://www.un.org/depts/los/convention_agreements/texts/unclos/unclos_e.pdf"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (UN)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pt_2", "Part II - Territorial Sea and Contiguous Zone", 1, None),
    ("pt_5", "Part V - Exclusive Economic Zone", 1, None),
    ("pt_6", "Part VI - Continental Shelf", 1, None),
    ("pt_7", "Part VII - High Seas", 1, None),
    ("pt_11", "Part XI - The Area (Seabed)", 1, None),
    ("pt_12", "Part XII - Protection and Preservation of the Marine Environment", 1, None),
    ("pt_13", "Part XIII - Marine Scientific Research", 1, None),
    ("pt_15", "Part XV - Settlement of Disputes", 1, None),
    ("2_1", "Limits of the territorial sea (12 nautical miles)", 2, "pt_2"),
    ("2_2", "Innocent passage through territorial sea", 2, "pt_2"),
    ("2_3", "Contiguous zone (24 nautical miles)", 2, "pt_2"),
    ("5_1", "Rights and duties in the EEZ (200 nautical miles)", 2, "pt_5"),
    ("5_2", "Living resources conservation", 2, "pt_5"),
    ("5_3", "Artificial islands and installations in EEZ", 2, "pt_5"),
    ("6_1", "Definition and limits of the continental shelf", 2, "pt_6"),
    ("6_2", "Rights over the continental shelf", 2, "pt_6"),
    ("7_1", "Freedom of the high seas", 2, "pt_7"),
    ("7_2", "Conservation of living resources of the high seas", 2, "pt_7"),
    ("11_1", "Common heritage of mankind principle", 2, "pt_11"),
    ("11_2", "International Seabed Authority", 2, "pt_11"),
    ("12_1", "General obligation to protect and preserve the marine environment", 2, "pt_12"),
    ("12_2", "Pollution from land-based sources", 2, "pt_12"),
    ("12_3", "Pollution from seabed activities", 2, "pt_12"),
    ("15_1", "Compulsory dispute settlement procedures", 2, "pt_15"),
    ("15_2", "International Tribunal for the Law of the Sea", 2, "pt_15"),
]


async def ingest_reg_unclos(conn) -> int:
    """Insert or update UNCLOS system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_unclos"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_unclos", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_unclos",
    )
    return count
