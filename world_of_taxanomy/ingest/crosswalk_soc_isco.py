"""SOC 2018 <-> ISCO-08 crosswalk ingester.

Source: SOC 2010 <-> ISCO-08 correspondence table
  Daniel Russ / codingsystems (public domain)
  https://danielruss.github.io/codingsystems/soc2010_isco2008.csv

SOC codes are filtered to those present in our soc_2018 system (most SOC 2010
detailed codes are unchanged in SOC 2018). Match type is 'broad' to reflect
the SOC 2010 vs SOC 2018 version difference.

~992 valid pairs x 2 = ~1,984 bidirectional edges.
"""
from __future__ import annotations

import csv
from typing import Optional

from world_of_taxanomy.ingest.base import ensure_data_file

_DOWNLOAD_URL = (
    "https://danielruss.github.io/codingsystems/soc2010_isco2008.csv"
)
_DEFAULT_PATH = "data/soc2010_isco08.csv"

CHUNK = 500


async def ingest_crosswalk_soc_isco(conn, path: Optional[str] = None) -> int:
    """Insert bidirectional SOC 2018 <-> ISCO-08 equivalence edges.

    Filters source SOC codes to those present in our soc_2018 classification_node
    table. Uses match_type='broad' (SOC 2010 source, SOC 2018 in DB).

    Returns total edges inserted (bidirectional, so 2x unique pairs).
    """
    local = path or _DEFAULT_PATH
    ensure_data_file(_DOWNLOAD_URL, local)

    # Load all SOC 2018 codes present in the DB for filtering
    soc_codes = {
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node WHERE system_id = 'soc_2018'"
        )
    }

    rows: list[tuple[str, str, str, str, str]] = []

    with open(local, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for record in reader:
            soc_code = record["soc2010"].strip()
            isco_code = record["isco2008"].strip()

            if not soc_code or not isco_code:
                continue
            # Only include SOC codes present in our soc_2018 system
            if soc_code not in soc_codes:
                continue

            rows.append(("soc_2018", soc_code, "isco_08", isco_code, "broad"))
            rows.append(("isco_08", isco_code, "soc_2018", soc_code, "broad"))

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
