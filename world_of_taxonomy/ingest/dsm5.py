"""Ingest DSM-5 (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("dsm5", "DSM-5", "DSM-5 (Skeleton)", "2022", "United States", "APA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "APA License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DSM", "DSM-5 Diagnostic Categories", 1, None),
    ("DSM.01", "Neurodevelopmental Disorders", 2, 'DSM'),
    ("DSM.02", "Schizophrenia Spectrum", 2, 'DSM'),
    ("DSM.03", "Bipolar and Related Disorders", 2, 'DSM'),
    ("DSM.04", "Depressive Disorders", 2, 'DSM'),
    ("DSM.05", "Anxiety Disorders", 2, 'DSM'),
    ("DSM.06", "Obsessive-Compulsive", 2, 'DSM'),
    ("DSM.07", "Trauma and Stressor-Related", 2, 'DSM'),
    ("DSM.08", "Dissociative Disorders", 2, 'DSM'),
    ("DSM.09", "Somatic Symptom Disorders", 2, 'DSM'),
    ("DSM.10", "Feeding and Eating Disorders", 2, 'DSM'),
    ("DSM.11", "Elimination Disorders", 2, 'DSM'),
    ("DSM.12", "Sleep-Wake Disorders", 2, 'DSM'),
    ("DSM.13", "Sexual Dysfunctions", 2, 'DSM'),
    ("DSM.14", "Gender Dysphoria", 2, 'DSM'),
    ("DSM.15", "Disruptive and Conduct Disorders", 2, 'DSM'),
    ("DSM.16", "Substance-Related Disorders", 2, 'DSM'),
    ("DSM.17", "Neurocognitive Disorders", 2, 'DSM'),
    ("DSM.18", "Personality Disorders", 2, 'DSM'),
    ("DSM.19", "Paraphilic Disorders", 2, 'DSM'),
    ("DSM.20", "Other Conditions", 2, 'DSM'),
]

async def ingest_dsm5(conn) -> int:
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
