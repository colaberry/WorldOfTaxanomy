"""ESCO Occupations ingester.

European Skills, Competences, Qualifications and Occupations (ESCO).
Published by the European Commission.
License: CC BY 4.0
Reference: https://esco.ec.europa.eu/en/use-esco/download

ESCO occupations are stored as a flat classification system:
  ~3,046 occupation nodes (v1.2.1) / ~2,942 nodes (v1.1.1)
  All nodes are level=1, parent_code=None, is_leaf=True.
  sector_code = first digit of the associated ISCO-08 group code.

The relationship between ESCO occupations and ISCO-08 unit groups is
handled separately by the crosswalk ingester (Phase 7-C).

Download options (auto-detected in priority order):
  1. JSON-LD ZIP: 'data/ESCO dataset - v1.2.1 - classification -  - json-ld.zip'
  2. CSV file: 'data/esco_occupations_en.csv' (extracted from v1.1.1 CSV bundle)
  3. Auto-download CSV from EU server (may be slow)
"""
from __future__ import annotations

import csv
import json
import zipfile
from pathlib import Path
from typing import Optional

from world_of_taxanomy.ingest.base import ensure_data_file_zip

_DOWNLOAD_URL = (
    "https://ec.europa.eu/esco/portal/escopedia_api"
    "?downloadFile=true&file=ESCO+dataset+v1.1.1+-+classification+-+en+-+csv.zip"
)
_ZIP_MEMBER = "occupations_en.csv"
_DEFAULT_CSV_PATH = "data/esco_occupations_en.csv"
_JSONLD_ZIP_PATH = "data/ESCO dataset - v1.2.1 - classification -  - json-ld.zip"
_JSONLD_MEMBER = "/esco-v1.2.1.json-ld"

CHUNK = 500

_SYSTEM_ROW = (
    "esco_occupations",
    "ESCO Occupations",
    "European Skills, Competences, Qualifications and Occupations - Occupations",
    "1.2.1",
    "Europe / Global",
    "European Commission",
)

# ISCO major group labels for sector assignment
_ISCO_MAJOR_LABELS: dict[str, str] = {
    "0": "Armed Forces Occupations",
    "1": "Managers",
    "2": "Professionals",
    "3": "Technicians and Associate Professionals",
    "4": "Clerical Support Workers",
    "5": "Service and Sales Workers",
    "6": "Skilled Agricultural, Forestry and Fishery Workers",
    "7": "Craft and Related Trades Workers",
    "8": "Plant and Machine Operators and Assemblers",
    "9": "Elementary Occupations",
}


def _extract_code(concept_uri: str) -> str:
    """Extract occupation code (UUID/slug) from ESCO conceptUri.

    Example:
      'http://data.europa.eu/esco/occupation/14a21b3e-8d10-49a7-a7fb-b2e2e61ebb13'
      -> '14a21b3e-8d10-49a7-a7fb-b2e2e61ebb13'
    """
    return concept_uri.rstrip("/").split("/")[-1]


def _determine_sector(isco_code: str) -> str:
    """Return ISCO major group (first digit) as sector code.

    Returns '0' if isco_code is empty or missing.
    """
    stripped = isco_code.strip() if isco_code else ""
    if not stripped:
        return "0"
    return stripped[0]


def _extract_en_label(preferred_labels) -> str:
    """Extract English preferred label from JSON-LD preferredLabel list.

    The JSON-LD format wraps labels as objects with 'literalForm' dicts
    keyed by language code. This extracts the 'en' value.

    Returns empty string if no English label is found.
    """
    if not preferred_labels:
        return ""
    if isinstance(preferred_labels, list):
        for label_obj in preferred_labels:
            if not isinstance(label_obj, dict):
                continue
            lf = label_obj.get("literalForm", {})
            if isinstance(lf, dict) and "en" in lf:
                return lf["en"]
    return ""


def _ingest_from_jsonld(zip_path: str) -> list[tuple]:
    """Parse ESCO occupation nodes from the v1.2.1 JSON-LD ZIP.

    Returns list of (system_id, code, title, level, parent_code, sector_code, is_leaf) tuples.
    """
    records: list[tuple] = []
    seen_codes: set[str] = set()

    with zipfile.ZipFile(zip_path) as zf:
        member = _JSONLD_MEMBER
        # Fallback: find first json-ld file if the expected name is absent
        if member not in zf.namelist():
            members = [m for m in zf.namelist() if m.endswith(".json-ld") or m.endswith(".jsonld")]
            if not members:
                return records
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

        code = _extract_code(uri)
        if not code or code in seen_codes:
            continue

        title = _extract_en_label(item.get("preferredLabel"))
        if not title:
            continue

        # Sector from notation (e.g. "2654.1.7" -> first digit "2")
        # or from iscoGroup URI if present
        isco_group = item.get("iscoGroup") or ""
        if isinstance(isco_group, str) and isco_group:
            # URI like http://data.europa.eu/esco/iscoGroup/2654
            isco_code = isco_group.rstrip("/").split("/")[-1]
        else:
            # Use notation as fallback (e.g. "2654.1.7")
            notation = item.get("notation", "") or ""
            isco_code = notation.split(".")[0] if notation else ""

        sector = _determine_sector(isco_code)

        seen_codes.add(code)
        records.append((
            "esco_occupations",
            code,
            title,
            1,       # level: flat
            None,    # parent_code: crosswalk handles ISCO relationship
            sector,  # sector_code: first digit of ISCO group
            True,    # is_leaf: all occupations are leaves
        ))

    return records


def _ingest_from_csv(csv_path: str) -> list[tuple]:
    """Parse ESCO occupation nodes from CSV file (v1.1.1 format)."""
    records: list[tuple] = []
    seen_codes: set[str] = set()

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            concept_uri = row.get("conceptUri", "").strip()
            if not concept_uri:
                continue
            in_scheme = row.get("inScheme", "")
            if "iscoGroup" in in_scheme:
                continue
            code = _extract_code(concept_uri)
            if not code or code in seen_codes:
                continue
            title = row.get("preferredLabel", "").strip()
            if not title:
                continue
            isco_code = row.get("code", "").strip()
            sector = _determine_sector(isco_code)
            seen_codes.add(code)
            records.append((
                "esco_occupations", code, title, 1, None, sector, True,
            ))

    return records


async def ingest_esco_occupations(conn, path: Optional[str] = None) -> int:
    """Ingest ESCO Occupations.

    Auto-detects source in priority order:
      1. JSON-LD ZIP at _JSONLD_ZIP_PATH (v1.2.1, user-provided)
      2. CSV at path argument or _DEFAULT_CSV_PATH (v1.1.1, extracted)
      3. Downloads CSV from EU server (fallback, may be slow)

    Returns count of nodes inserted (or already present on re-run).
    """
    # Determine source
    if path:
        if path.endswith(".zip") and "json-ld" in path.lower():
            records = _ingest_from_jsonld(path)
        else:
            records = _ingest_from_csv(path)
    elif Path(_JSONLD_ZIP_PATH).exists():
        records = _ingest_from_jsonld(_JSONLD_ZIP_PATH)
    else:
        csv_path = _DEFAULT_CSV_PATH
        ensure_data_file_zip(_DOWNLOAD_URL, csv_path, _ZIP_MEMBER)
        records = _ingest_from_csv(csv_path)

    # Register system
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        *_SYSTEM_ROW,
    )

    count = 0
    for i in range(0, len(records), CHUNK):
        chunk = records[i: i + CHUNK]
        await conn.executemany(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (system_id, code) DO NOTHING""",
            chunk,
        )
        count += len(chunk)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 "
        "WHERE id = 'esco_occupations'",
        count,
    )

    return count
