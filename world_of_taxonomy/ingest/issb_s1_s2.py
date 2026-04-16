"""Ingest IFRS Sustainability Disclosure Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("issb_s1_s2", "ISSB S1/S2", "IFRS Sustainability Disclosure Standards", "2023", "Global", "IFRS Foundation")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IS", "ISSB Standards", 1, None),
    ("IS.01", "S1: General Requirements", 2, 'IS'),
    ("IS.02", "S1: Governance Disclosures", 2, 'IS'),
    ("IS.03", "S1: Strategy Disclosures", 2, 'IS'),
    ("IS.04", "S1: Risk Management Disclosures", 2, 'IS'),
    ("IS.05", "S1: Metrics and Targets", 2, 'IS'),
    ("IS.06", "S1: Connected Information", 2, 'IS'),
    ("IS.07", "S2: Climate-Related Physical Risks", 2, 'IS'),
    ("IS.08", "S2: Climate-Related Transition Risks", 2, 'IS'),
    ("IS.09", "S2: Climate-Related Opportunities", 2, 'IS'),
    ("IS.10", "S2: Scope 1 GHG Emissions", 2, 'IS'),
    ("IS.11", "S2: Scope 2 GHG Emissions", 2, 'IS'),
    ("IS.12", "S2: Scope 3 GHG Emissions", 2, 'IS'),
    ("IS.13", "S2: Climate Resilience", 2, 'IS'),
    ("IS.14", "S2: Internal Carbon Price", 2, 'IS'),
    ("IS.15", "S2: Remuneration Linked to Climate", 2, 'IS'),
]

async def ingest_issb_s1_s2(conn) -> int:
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
