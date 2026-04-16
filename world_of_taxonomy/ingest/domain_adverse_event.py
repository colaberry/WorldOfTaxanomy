"""Ingest Adverse Event Classification Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_adverse_event", "Adverse Event Class", "Adverse Event Classification Types", "1.0", "Global", "WHO-UMC")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AE", "Adverse Event Classification Types", 1, None),
    ("AE.01", "Type A (Augmented/Dose-Related)", 2, 'AE'),
    ("AE.02", "Type B (Bizarre/Idiosyncratic)", 2, 'AE'),
    ("AE.03", "Type C (Chronic/Cumulative)", 2, 'AE'),
    ("AE.04", "Type D (Delayed)", 2, 'AE'),
    ("AE.05", "Type E (End-of-Treatment)", 2, 'AE'),
    ("AE.06", "Serious Adverse Event (SAE)", 2, 'AE'),
    ("AE.07", "Unexpected Adverse Reaction", 2, 'AE'),
    ("AE.08", "Medication Error", 2, 'AE'),
    ("AE.09", "Allergic Reaction", 2, 'AE'),
    ("AE.10", "Anaphylaxis", 2, 'AE'),
    ("AE.11", "Black Box Warning Event", 2, 'AE'),
    ("AE.12", "Drug Withdrawal Effect", 2, 'AE'),
    ("AE.13", "Off-Label Use Adverse Event", 2, 'AE'),
    ("AE.14", "Vaccine Adverse Event", 2, 'AE'),
]

async def ingest_domain_adverse_event(conn) -> int:
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
