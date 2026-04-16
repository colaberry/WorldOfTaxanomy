"""Ingest Online Mendelian Inheritance in Man (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("omim", "OMIM", "Online Mendelian Inheritance in Man (Skeleton)", "2024", "Global", "Johns Hopkins University")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("OM", "OMIM Categories", 1, None),
    ("OM.01", "Autosomal Dominant", 2, 'OM'),
    ("OM.02", "Autosomal Recessive", 2, 'OM'),
    ("OM.03", "X-Linked", 2, 'OM'),
    ("OM.04", "Y-Linked", 2, 'OM'),
    ("OM.05", "Mitochondrial", 2, 'OM'),
    ("OM.06", "Digenic", 2, 'OM'),
    ("OM.07", "Somatic Mutation", 2, 'OM'),
    ("OM.08", "Gene (Asterisk *)", 2, 'OM'),
    ("OM.09", "Phenotype (Number #)", 2, 'OM'),
    ("OM.10", "Suspected Phenotype (%)", 2, 'OM'),
    ("OM.11", "Phenotype Locus/Series", 2, 'OM'),
    ("OM.12", "Gene/Phenotype (Plus +)", 2, 'OM'),
    ("OM.13", "Moved/Removed Entries", 2, 'OM'),
]

async def ingest_omim(conn) -> int:
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
