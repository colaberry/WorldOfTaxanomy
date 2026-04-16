"""Ingest Customs Classification Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_customs_class", "Customs Classification", "Customs Classification Types", "1.0", "Global", "WCO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CX", "Customs Classification Types", 1, None),
    ("CX.01", "HS Code Classification", 2, 'CX'),
    ("CX.02", "Binding Ruling", 2, 'CX'),
    ("CX.03", "Country of Origin Determination", 2, 'CX'),
    ("CX.04", "Customs Valuation (Transaction)", 2, 'CX'),
    ("CX.05", "Tariff Preference", 2, 'CX'),
    ("CX.06", "Anti-Dumping Duty", 2, 'CX'),
    ("CX.07", "Countervailing Duty", 2, 'CX'),
    ("CX.08", "Tariff Rate Quota", 2, 'CX'),
    ("CX.09", "Generalized System of Preferences", 2, 'CX'),
    ("CX.10", "Section 301 Tariff (US)", 2, 'CX'),
    ("CX.11", "Customs Audit", 2, 'CX'),
    ("CX.12", "Advance Ruling", 2, 'CX'),
    ("CX.13", "Post-Clearance Audit", 2, 'CX'),
]

async def ingest_domain_customs_class(conn) -> int:
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
