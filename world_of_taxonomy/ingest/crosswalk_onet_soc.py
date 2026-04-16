"""O*NET-SOC <-> SOC 2018 crosswalk ingester.

O*NET base occupations (codes ending '.00') map 1:1 to SOC 2018 detailed
occupations. The mapping is derived by stripping '.00' from the O*NET code.

Example: '11-1011.00' (O*NET) <-> '11-1011' (SOC 2018)

Source: derived from onet_soc data (Occupation Data.txt, CC BY 4.0)
  https://www.onetcenter.org/dl_files/database/db_29_0_text/Occupation%20Data.txt

Edge semantics:
  onet_soc -> soc_2018: match_type='exact'
  soc_2018 -> onet_soc: match_type='exact'

~867 pairs -> ~1,734 bidirectional edges.
"""
from __future__ import annotations

import csv
from typing import Optional

_DEFAULT_PATH = "data/onet_occupation_data.txt"

CHUNK = 500


async def ingest_crosswalk_onet_soc(conn, path: Optional[str] = None) -> int:
    """Insert bidirectional O*NET-SOC <-> SOC 2018 equivalence edges.

    Reads Occupation Data.txt (already downloaded by onet_soc ingester).
    Maps O*NET codes to SOC 2018 by stripping the '.00' suffix.
    Only inserts edges for codes present in both systems.

    Returns total edges inserted (bidirectional, 2x unique pairs).
    """
    local = path or _DEFAULT_PATH

    # Load codes present in DB for filtering
    onet_codes = {
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node WHERE system_id = 'onet_soc'"
        )
    }
    soc_codes = {
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node WHERE system_id = 'soc_2018'"
        )
    }

    rows: list[tuple[str, str, str, str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()

    with open(local, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for record in reader:
            onet_code = record.get("O*NET-SOC Code", "").strip()

            if not onet_code:
                continue
            if not onet_code.endswith(".00"):
                continue

            # Derive SOC 2018 code by stripping decimal suffix
            soc_code = onet_code[:-3]  # remove '.00'

            if onet_code not in onet_codes:
                continue
            if soc_code not in soc_codes:
                continue

            pair = (onet_code, soc_code)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            rows.append(("onet_soc", onet_code, "soc_2018", soc_code, "exact"))
            rows.append(("soc_2018", soc_code, "onet_soc", onet_code, "exact"))

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
