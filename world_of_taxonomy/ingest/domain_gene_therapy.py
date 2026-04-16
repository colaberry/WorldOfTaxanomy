"""Ingest Gene Therapy Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_gene_therapy", "Gene Therapy", "Gene Therapy Types", "1.0", "Global", "FDA/EMA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GT", "Gene Therapy Types", 1, None),
    ("GT.01", "In Vivo Gene Therapy", 2, 'GT'),
    ("GT.02", "Ex Vivo Gene Therapy", 2, 'GT'),
    ("GT.03", "AAV Vector Therapy", 2, 'GT'),
    ("GT.04", "Lentiviral Vector Therapy", 2, 'GT'),
    ("GT.05", "Retroviral Vector Therapy", 2, 'GT'),
    ("GT.06", "mRNA Therapy", 2, 'GT'),
    ("GT.07", "CRISPR Gene Editing", 2, 'GT'),
    ("GT.08", "Antisense Oligonucleotide", 2, 'GT'),
    ("GT.09", "siRNA Therapy", 2, 'GT'),
    ("GT.10", "Gene Augmentation", 2, 'GT'),
    ("GT.11", "Gene Silencing", 2, 'GT'),
    ("GT.12", "Gene Replacement", 2, 'GT'),
    ("GT.13", "Oncolytic Virus Therapy", 2, 'GT'),
    ("GT.14", "Base Editing", 2, 'GT'),
]

async def ingest_domain_gene_therapy(conn) -> int:
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
