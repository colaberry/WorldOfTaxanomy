"""Ingest Convention on Biological Diversity Aichi Biodiversity Targets."""
from __future__ import annotations

_SYSTEM_ROW = ("cbd_aichi", "CBD Aichi Targets", "Convention on Biological Diversity Aichi Biodiversity Targets", "2010", "Global", "CBD Secretariat")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CA", "CBD Aichi Targets", 1, None),
    ("CA.01", "Target 1: Awareness of Biodiversity", 2, 'CA'),
    ("CA.02", "Target 2: Biodiversity in Planning", 2, 'CA'),
    ("CA.03", "Target 3: Harmful Subsidies Eliminated", 2, 'CA'),
    ("CA.04", "Target 4: Sustainable Production", 2, 'CA'),
    ("CA.05", "Target 5: Habitat Loss Halved", 2, 'CA'),
    ("CA.06", "Target 6: Sustainable Fisheries", 2, 'CA'),
    ("CA.07", "Target 7: Sustainable Agriculture", 2, 'CA'),
    ("CA.08", "Target 8: Pollution Reduced", 2, 'CA'),
    ("CA.09", "Target 9: Invasive Species Controlled", 2, 'CA'),
    ("CA.10", "Target 10: Vulnerable Ecosystems", 2, 'CA'),
    ("CA.11", "Target 11: Protected Areas (17%/10%)", 2, 'CA'),
    ("CA.12", "Target 12: Species Extinction Prevented", 2, 'CA'),
    ("CA.13", "Target 13: Genetic Diversity", 2, 'CA'),
    ("CA.14", "Target 14: Ecosystem Services", 2, 'CA'),
    ("CA.15", "Target 15: Ecosystem Restoration (15%)", 2, 'CA'),
    ("CA.16", "Target 16: Nagoya Protocol", 2, 'CA'),
    ("CA.17", "Target 17: National Strategies", 2, 'CA'),
    ("CA.18", "Target 18: Traditional Knowledge", 2, 'CA'),
    ("CA.19", "Target 19: Knowledge Sharing", 2, 'CA'),
    ("CA.20", "Target 20: Resource Mobilization", 2, 'CA'),
]

async def ingest_cbd_aichi(conn) -> int:
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
