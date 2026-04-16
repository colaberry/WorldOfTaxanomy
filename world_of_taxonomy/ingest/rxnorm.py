"""Ingest RxNorm (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("rxnorm", "RxNorm", "RxNorm (Skeleton)", "2024", "United States", "NLM")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RX", "RxNorm Term Types", 1, None),
    ("RX.IN", "Ingredient (IN)", 2, 'RX'),
    ("RX.PIN", "Precise Ingredient (PIN)", 2, 'RX'),
    ("RX.BN", "Brand Name (BN)", 2, 'RX'),
    ("RX.SCDC", "Semantic Clinical Drug Component", 2, 'RX'),
    ("RX.SCDF", "Semantic Clinical Drug Form", 2, 'RX'),
    ("RX.SCDG", "Semantic Clinical Drug Group", 2, 'RX'),
    ("RX.SCD", "Semantic Clinical Drug", 2, 'RX'),
    ("RX.SBDC", "Semantic Branded Drug Component", 2, 'RX'),
    ("RX.SBDF", "Semantic Branded Drug Form", 2, 'RX'),
    ("RX.SBDG", "Semantic Branded Drug Group", 2, 'RX'),
    ("RX.SBD", "Semantic Branded Drug", 2, 'RX'),
    ("RX.DF", "Dose Form (DF)", 2, 'RX'),
    ("RX.DFG", "Dose Form Group (DFG)", 2, 'RX'),
    ("RX.GPCK", "Generic Pack", 2, 'RX'),
    ("RX.BPCK", "Branded Pack", 2, 'RX'),
]

async def ingest_rxnorm(conn) -> int:
    sid, short, full, ver, region, authority = _SYSTEM_ROW
    await conn.execute(
        """INSERT INTO classification_system (id, name, full_name, version, region, authority,
                  source_url, source_date, data_provenance, license)
           VALUES ($1,$2,$3,$4,$5,$6,$7,CURRENT_DATE,$8,$9)
           ON CONFLICT (id) DO UPDATE SET name=$2,full_name=$3,version=$4,region=$5,
                  authority=$6,source_url=$7,source_date=CURRENT_DATE,
                  data_provenance=$8,license=$9""",
        sid, short, full, ver, region, authority,
        _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute("DELETE FROM classification_node WHERE system_id = $1", sid)
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1,$2,$3,$4,$5)""",
            sid, code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, sid)
    return count
