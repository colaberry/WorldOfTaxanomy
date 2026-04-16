"""Ingest G-DRG (Germany)."""
from __future__ import annotations

_SYSTEM_ROW = ("g_drg", "G-DRG", "G-DRG (Germany)", "2024", "Germany", "InEK")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GDRG", "G-DRG Major Diagnostic Categories", 1, None),
    ("MDC01", "Nervous System", 2, 'GDRG'),
    ("MDC02", "Eye", 2, 'GDRG'),
    ("MDC03", "Ear, Nose, Mouth, Throat", 2, 'GDRG'),
    ("MDC04", "Respiratory System", 2, 'GDRG'),
    ("MDC05", "Circulatory System", 2, 'GDRG'),
    ("MDC06", "Digestive System", 2, 'GDRG'),
    ("MDC07", "Hepatobiliary and Pancreas", 2, 'GDRG'),
    ("MDC08", "Musculoskeletal System", 2, 'GDRG'),
    ("MDC09", "Skin, Subcutaneous Tissue, Breast", 2, 'GDRG'),
    ("MDC10", "Endocrine and Metabolic", 2, 'GDRG'),
    ("MDC11", "Kidney and Urinary Tract", 2, 'GDRG'),
    ("MDC12", "Male Reproductive System", 2, 'GDRG'),
    ("MDC13", "Female Reproductive System", 2, 'GDRG'),
    ("MDC14", "Pregnancy and Childbirth", 2, 'GDRG'),
    ("MDC15", "Newborns and Neonates", 2, 'GDRG'),
    ("MDC16", "Blood and Immunological", 2, 'GDRG'),
    ("MDC17", "Neoplastic Disorders", 2, 'GDRG'),
    ("MDC18", "Infectious and Parasitic", 2, 'GDRG'),
    ("MDC19", "Mental Diseases", 2, 'GDRG'),
    ("MDC20", "Alcohol/Drug Abuse", 2, 'GDRG'),
    ("MDC21", "Injuries and Poisoning", 2, 'GDRG'),
    ("MDC22", "Burns", 2, 'GDRG'),
    ("MDC23", "Factors Influencing Health", 2, 'GDRG'),
    ("MDC24", "Multiple Trauma", 2, 'GDRG'),
    ("PRE", "Pre-MDC DRGs", 2, 'GDRG'),
]

async def ingest_g_drg(conn) -> int:
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
