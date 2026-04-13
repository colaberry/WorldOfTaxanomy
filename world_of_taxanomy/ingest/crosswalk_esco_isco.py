"""ESCO Occupations <-> ISCO-08 crosswalk ingester.

ESCO occupations are specialisations of ISCO-08 unit groups.
The mapping is derived from:
  - The `code` column in occupations_en.csv (v1.1.1), OR
  - The `iscoGroup` URI field in the JSON-LD ZIP (v1.2.1)

Source: ESCO dataset CC BY 4.0
  https://esco.ec.europa.eu/en/use-esco/download

Edge semantics:
  esco_occupations -> isco_08: match_type='narrow' (ESCO is more specific)
  isco_08 -> esco_occupations: match_type='broad' (ISCO is the broader concept)

~3,045 occupations -> ~6,090 bidirectional edges (v1.2.1).
"""
from __future__ import annotations

import csv
import json
import zipfile
from pathlib import Path
from typing import Optional

_DEFAULT_CSV_PATH = "data/esco_occupations_en.csv"
_JSONLD_ZIP_PATH = "data/ESCO dataset - v1.2.1 - classification -  - json-ld.zip"
_JSONLD_MEMBER = "/esco-v1.2.1.json-ld"

CHUNK = 500


def _load_pairs_from_jsonld(zip_path: str) -> list[tuple[str, str]]:
    """Extract (esco_code, isco_unit_group_code) pairs from JSON-LD ZIP.

    Uses the `iscoGroup` URI field:
      'http://data.europa.eu/esco/iscoGroup/2654' -> isco_code='2654'
    Falls back to `notation` field (e.g. '2654.1.7' -> '2654') if iscoGroup absent.

    Returns list of (esco_uuid, isco_unit_group) pairs.
    """
    pairs: list[tuple[str, str]] = []

    with zipfile.ZipFile(zip_path) as zf:
        member = _JSONLD_MEMBER
        if member not in zf.namelist():
            members = [m for m in zf.namelist() if m.endswith(".json-ld") or m.endswith(".jsonld")]
            if not members:
                return pairs
            member = members[0]
        with zf.open(member) as f:
            data = json.load(f)

    graph = data.get("@graph", [])
    for item in graph:
        types = item.get("type", "")
        if isinstance(types, str):
            types = [types]
        if "esco:Occupation" not in types:
            continue

        uri = item.get("uri", "")
        if not uri:
            continue
        esco_code = uri.rstrip("/").split("/")[-1]
        if not esco_code:
            continue

        isco_group = item.get("iscoGroup") or ""
        if isinstance(isco_group, str) and isco_group:
            isco_code = isco_group.rstrip("/").split("/")[-1]
        else:
            notation = item.get("notation", "") or ""
            isco_code = notation.split(".")[0] if notation else ""

        if esco_code and isco_code:
            pairs.append((esco_code, isco_code))

    return pairs


def _load_pairs_from_csv(csv_path: str) -> list[tuple[str, str]]:
    """Extract (esco_code, isco_unit_group_code) pairs from CSV file."""
    pairs: list[tuple[str, str]] = []

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for record in reader:
            concept_uri = record.get("conceptUri", "").strip()
            isco_code = record.get("code", "").strip()

            if not concept_uri or not isco_code:
                continue

            in_scheme = record.get("inScheme", "")
            if "iscoGroup" in in_scheme:
                continue

            esco_code = concept_uri.rstrip("/").split("/")[-1]
            if esco_code and isco_code:
                pairs.append((esco_code, isco_code))

    return pairs


async def ingest_crosswalk_esco_isco(conn, path: Optional[str] = None) -> int:
    """Insert bidirectional ESCO Occupations <-> ISCO-08 equivalence edges.

    Auto-detects source in priority order:
      1. JSON-LD ZIP at _JSONLD_ZIP_PATH (v1.2.1, user-provided)
      2. CSV at path argument or _DEFAULT_CSV_PATH (v1.1.1, extracted)

    Only inserts edges for ESCO codes present in the esco_occupations system
    and ISCO codes present in the isco_08 system.

    Returns total edges inserted (bidirectional, 2x unique pairs).
    """
    # Load pairs from source
    if path:
        if path.endswith(".zip") and "json-ld" in path.lower():
            raw_pairs = _load_pairs_from_jsonld(path)
        else:
            raw_pairs = _load_pairs_from_csv(path)
    elif Path(_JSONLD_ZIP_PATH).exists():
        raw_pairs = _load_pairs_from_jsonld(_JSONLD_ZIP_PATH)
    else:
        raw_pairs = _load_pairs_from_csv(_DEFAULT_CSV_PATH)

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

    for esco_code, isco_code in raw_pairs:
        if esco_code not in esco_codes:
            continue
        if isco_code not in isco_codes:
            continue

        pair = (esco_code, isco_code)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        rows.append(("esco_occupations", esco_code, "isco_08", isco_code, "narrow"))
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
