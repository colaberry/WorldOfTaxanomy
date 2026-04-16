"""Ingest UNCITRAL Model Laws on International Commercial Law."""

from __future__ import annotations

_SYSTEM_ROW = (
    "reg_uncitral",
    "UNCITRAL Model Laws",
    "UNCITRAL Model Laws on International Commercial Law",
    "2023",
    "Global",
    "United Nations Commission on International Trade Law (UNCITRAL)",
)
_SOURCE_URL = "https://uncitral.un.org/en/texts"
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain (UN)"

NODES: list[tuple[str, str, int, str | None]] = [
    ("arb", "Model Law on International Commercial Arbitration", 1, None),
    ("ecom", "Model Law on Electronic Commerce", 1, None),
    ("esig", "Model Law on Electronic Signatures", 1, None),
    ("insol", "Model Law on Cross-Border Insolvency", 1, None),
    ("proc", "Model Law on Public Procurement", 1, None),
    ("sec", "Model Law on Secured Transactions", 1, None),
    ("etr", "Model Law on Electronic Transferable Records", 1, None),
    ("arb_1", "Scope and definitions", 2, "arb"),
    ("arb_2", "Arbitration agreement", 2, "arb"),
    ("arb_3", "Composition of arbitral tribunal", 2, "arb"),
    ("arb_4", "Conduct of arbitral proceedings", 2, "arb"),
    ("arb_5", "Award and termination", 2, "arb"),
    ("arb_6", "Recourse against award", 2, "arb"),
    ("arb_7", "Recognition and enforcement", 2, "arb"),
    ("ecom_1", "General provisions", 2, "ecom"),
    ("ecom_2", "Application of legal requirements to data messages", 2, "ecom"),
    ("ecom_3", "Communication of data messages", 2, "ecom"),
    ("proc_1", "General provisions and methods of procurement", 2, "proc"),
    ("proc_2", "Open and restricted tendering", 2, "proc"),
    ("proc_3", "Framework agreements and electronic reverse auctions", 2, "proc"),
]


async def ingest_reg_uncitral(conn) -> int:
    """Insert or update UNCITRAL Model Laws system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "reg_uncitral"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "reg_uncitral", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "reg_uncitral",
    )
    return count
