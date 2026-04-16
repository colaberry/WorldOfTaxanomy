"""Ingest Drug Interaction Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_drug_interaction", "Drug Interaction Type", "Drug Interaction Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DI", "Drug Interaction Types", 1, None),
    ("DI.01", "Pharmacokinetic Interaction", 2, 'DI'),
    ("DI.02", "Pharmacodynamic Interaction", 2, 'DI'),
    ("DI.03", "Drug-Drug Interaction", 2, 'DI'),
    ("DI.04", "Drug-Food Interaction", 2, 'DI'),
    ("DI.05", "Drug-Herb Interaction", 2, 'DI'),
    ("DI.06", "Drug-Lab Test Interaction", 2, 'DI'),
    ("DI.07", "CYP450 Enzyme Inhibition", 2, 'DI'),
    ("DI.08", "CYP450 Enzyme Induction", 2, 'DI'),
    ("DI.09", "P-glycoprotein Interaction", 2, 'DI'),
    ("DI.10", "QT Prolongation Risk", 2, 'DI'),
    ("DI.11", "Serotonin Syndrome Risk", 2, 'DI'),
    ("DI.12", "Renal Clearance Interaction", 2, 'DI'),
    ("DI.13", "Protein Binding Displacement", 2, 'DI'),
    ("DI.14", "Additive Toxicity", 2, 'DI'),
]

async def ingest_domain_drug_interaction(conn) -> int:
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
