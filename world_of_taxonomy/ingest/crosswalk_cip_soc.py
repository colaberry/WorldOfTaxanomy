"""CIP 2020 <-> SOC 2018 crosswalk ingester.

Source: NCES official CIP 2020 / SOC 2018 Crosswalk (public domain)
  https://nces.ed.gov/ipeds/cipcode/Files/CIP2020_SOC2018_Crosswalk.xlsx

Uses the 'CIP-SOC' sheet (CIP2020Code, CIP2020Title, SOC2018Code, SOC2018Title).
Rows where SOC2018Code is 'NO MATCH' or '99.9999' are excluded.
SOC codes are filtered to those present in our soc_2018 classification_node table.
Match type: 'broad' (official many-to-many mapping).

~5,917 valid pairs x 2 = ~11,834 bidirectional edges.
"""
from __future__ import annotations

from typing import Optional

from world_of_taxonomy.ingest.base import ensure_data_file

_DOWNLOAD_URL = (
    "https://nces.ed.gov/ipeds/cipcode/Files/CIP2020_SOC2018_Crosswalk.xlsx"
)
_DEFAULT_PATH = "data/cip2020_soc2018.xlsx"

CHUNK = 500


async def ingest_crosswalk_cip_soc(conn, path: Optional[str] = None) -> int:
    """Insert bidirectional CIP 2020 <-> SOC 2018 equivalence edges.

    Reads the 'CIP-SOC' sheet from the NCES crosswalk XLSX.
    Excludes NO MATCH rows. Filters SOC codes to those present in
    our soc_2018 system.

    Returns total edges inserted (bidirectional, so 2x unique pairs).
    """
    import openpyxl

    local = path or _DEFAULT_PATH
    ensure_data_file(_DOWNLOAD_URL, local)

    # Load SOC 2018 codes present in DB for filtering
    soc_codes = {
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node WHERE system_id = 'soc_2018'"
        )
    }

    wb = openpyxl.load_workbook(local, read_only=True, data_only=True)
    ws = wb["CIP-SOC"]

    rows: list[tuple[str, str, str, str, str]] = []
    header_skipped = False

    for record in ws.iter_rows(values_only=True):
        if not header_skipped:
            header_skipped = True
            continue

        cip_code = str(record[0]).strip() if record[0] else ""
        soc_code = str(record[2]).strip() if record[2] else ""

        if not cip_code or not soc_code:
            continue
        if soc_code in ("NO MATCH", "99.9999"):
            continue
        if soc_code not in soc_codes:
            continue

        rows.append(("cip_2020", cip_code, "soc_2018", soc_code, "broad"))
        rows.append(("soc_2018", soc_code, "cip_2020", cip_code, "broad"))

    wb.close()

    count = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i: i + CHUNK]
        await conn.executemany(
            """INSERT INTO equivalence
                   (source_system, source_code, target_system, target_code, match_type)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
            chunk,
        )
        count += len(chunk)

    return count
