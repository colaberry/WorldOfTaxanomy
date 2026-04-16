"""Ingest PCI-SIG Specification Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("pci_sig", "PCI-SIG", "PCI-SIG Specification Categories", "2024", "Global", "PCI-SIG")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PS", "PCI-SIG Specs", 1, None),
    ("PS.01", "PCI Express 6.0", 2, 'PS'),
    ("PS.02", "PCI Express 5.0", 2, 'PS'),
    ("PS.03", "PCI Express 4.0", 2, 'PS'),
    ("PS.04", "PCI Express 3.0", 2, 'PS'),
    ("PS.05", "CXL (Compute Express Link)", 2, 'PS'),
    ("PS.06", "PCI-X 2.0", 2, 'PS'),
    ("PS.07", "Mini PCI Express", 2, 'PS'),
    ("PS.08", "M.2 Specification", 2, 'PS'),
    ("PS.09", "U.2 (SFF-8639)", 2, 'PS'),
    ("PS.10", "PCI Power Management", 2, 'PS'),
    ("PS.11", "PCI Hot-Plug", 2, 'PS'),
    ("PS.12", "PCI Express Card Electromechanical", 2, 'PS'),
    ("PS.13", "Single Root I/O Virtualization (SR-IOV)", 2, 'PS'),
]

async def ingest_pci_sig(conn) -> int:
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
