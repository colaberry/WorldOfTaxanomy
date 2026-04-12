"""ESCO Occupations <-> ISCO-08 crosswalk ingester.

ESCO occupations are specialisations of ISCO-08 unit groups.
The mapping is derived directly from the `code` column in occupations_en.csv,
which contains the ISCO-08 unit group code for each ESCO occupation.

Source: occupations_en.csv from ESCO v1.1.1 bulk download (CC BY 4.0)
  https://esco.ec.europa.eu/en/use-esco/download

Edge semantics:
  esco_occupations -> isco_08: match_type='narrow' (ESCO is more specific)
  isco_08 -> esco_occupations: match_type='broad' (ISCO is the broader concept)

~2,942 occupations -> ~5,884 bidirectional edges.
"""
from __future__ import annotations

import csv
from typing import Optional

_DEFAULT_PATH = "data/esco_occupations_en.csv"

CHUNK = 500


async def ingest_crosswalk_esco_isco(conn, path: Optional[str] = None) -> int:
    """Insert bidirectional ESCO Occupations <-> ISCO-08 equivalence edges.

    Reads occupations_en.csv (already downloaded by esco_occupations ingester).
    Only inserts edges for ESCO codes present in the esco_occupations system
    and ISCO codes present in the isco_08 system.

    Returns total edges inserted (bidirectional, 2x unique pairs).
    """
    local = path or _DEFAULT_PATH

    # Load codes present in DB for filtering
    esco_codes = {
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node WHERE system_id = 'esco_occupations'"
        )
    }
    isco_codes = {
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node WHERE system_id = 'isco_08'"
        )
    }

    rows: list[tuple[str, str, str, str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()

    with open(local, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for record in reader:
            concept_uri = record.get("conceptUri", "").strip()
            isco_code = record.get("code", "").strip()

            if not concept_uri or not isco_code:
                continue

            # Skip ISCO group rows bundled in the CSV
            in_scheme = record.get("inScheme", "")
            if "iscoGroup" in in_scheme:
                continue

            esco_code = concept_uri.rstrip("/").split("/")[-1]

            if esco_code not in esco_codes:
                continue
            if isco_code not in isco_codes:
                continue

            pair = (esco_code, isco_code)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            # Forward: ESCO -> ISCO (narrow: ESCO is more specific)
            rows.append(("esco_occupations", esco_code, "isco_08", isco_code, "narrow"))
            # Reverse: ISCO -> ESCO (broad: ISCO is broader concept)
            rows.append(("isco_08", isco_code, "esco_occupations", esco_code, "broad"))

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
