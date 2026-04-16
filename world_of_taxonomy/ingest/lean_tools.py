"""Ingest Lean Manufacturing Tool Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("lean_tools", "Lean Tools", "Lean Manufacturing Tool Categories", "2024", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LT", "Lean Tools", 1, None),
    ("LT.01", "5S (Sort, Set, Shine, Standardize, Sustain)", 2, 'LT'),
    ("LT.02", "Value Stream Mapping (VSM)", 2, 'LT'),
    ("LT.03", "Kanban", 2, 'LT'),
    ("LT.04", "Kaizen (Continuous Improvement)", 2, 'LT'),
    ("LT.05", "Poka-Yoke (Error Proofing)", 2, 'LT'),
    ("LT.06", "Just-in-Time (JIT)", 2, 'LT'),
    ("LT.07", "Andon (Visual Signal)", 2, 'LT'),
    ("LT.08", "Gemba Walk", 2, 'LT'),
    ("LT.09", "Heijunka (Level Loading)", 2, 'LT'),
    ("LT.10", "SMED (Quick Changeover)", 2, 'LT'),
    ("LT.11", "Total Productive Maintenance (TPM)", 2, 'LT'),
    ("LT.12", "A3 Problem Solving", 2, 'LT'),
    ("LT.13", "Hoshin Kanri (Policy Deployment)", 2, 'LT'),
    ("LT.14", "Takt Time", 2, 'LT'),
]

async def ingest_lean_tools(conn) -> int:
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
