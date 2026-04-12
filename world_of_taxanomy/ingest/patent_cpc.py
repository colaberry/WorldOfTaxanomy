"""Patent CPC ingester.

Cooperative Patent Classification (CPC) - EPO / USPTO joint system.
License: open (EPO)
Reference: https://www.cooperativepatentclassification.org/

CPC hierarchy (5 levels):
  Section   (1 letter,  level 1, e.g. 'A')           9 sections
  Class     (3 chars,   level 2, e.g. 'A01')         ~650 classes
  Subclass  (4 chars,   level 3, e.g. 'A01B')        ~9,000 subclasses
  Group     (with '/00',level 4, e.g. 'A01B 1/00')   ~70,000 groups (main groups)
  Subgroup  (non-/00,   level 5, leaf, e.g. 'A01B 1/02') ~180,000 subgroups

Total: ~260,000 nodes.

Data source: Bulk XML files from CPC scheme download page.
  Section ZIPs: https://www.cooperativepatentclassification.org/cpcSchemeAndDefinitions/bulk
  Each section is one ZIP containing an XML file.

WARNING: Ingesting ~260K CPC codes takes several minutes.
The ingester downloads 9 ZIP files (one per section A-H and Y).

Codes with spaces are stored as-is (e.g. 'A01B 1/00').
"""
from __future__ import annotations

import os
import io
import ssl
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Optional

CHUNK = 500

_SYSTEM_ROW = (
    "patent_cpc",
    "Patent CPC",
    "Cooperative Patent Classification - EPO/USPTO",
    "2024",
    "Global",
    "European Patent Office / USPTO",
)

# CPC sections A through H plus Y (cross-sectional)
_SECTIONS = ["A", "B", "C", "D", "E", "F", "G", "H", "Y"]

# Base URL for CPC bulk download (one ZIP per section)
_CPC_BULK_URL = (
    "https://www.cooperativepatentclassification.org/cpcSchemeAndDefinitions/bulk"
)
_SECTION_ZIP_URL_TEMPLATE = (
    "https://www.cooperativepatentclassification.org"
    "/cpcSchemeAndDefinitions/bulk/cpc-scheme-{section}.zip"
)

_DEFAULT_DATA_DIR = "data/cpc"


def _determine_level(code: str) -> int:
    """Return CPC hierarchy level from code structure.

    Rules:
      Level 1: single letter (section, e.g. 'A')
      Level 2: 3 chars, no space (class, e.g. 'A01')
      Level 3: 4 chars, no space (subclass, e.g. 'A01B')
      Level 4: contains space + '/', suffix after '/' == '00' (main group)
      Level 5: contains space + '/', suffix after '/' != '00' (subgroup)
    """
    if " " not in code:
        length = len(code)
        if length == 1:
            return 1
        if length == 3:
            return 2
        if length == 4:
            return 3
        # Fallback for unusual lengths
        return 3

    # Has space - it's a group or subgroup
    if "/" not in code:
        return 4  # unusual, treat as group

    after_slash = code.split("/", 1)[1]
    if after_slash == "00":
        return 4  # main group
    return 5  # subgroup (leaf)


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code for a CPC node.

    Section (level 1): no parent
    Class (level 2): parent = first letter (section)
    Subclass (level 3): parent = first 3 chars (class)
    Main group (level 4): parent = first 4 chars (subclass)
    Subgroup (level 5): parent = same prefix + '/00' (main group)
    """
    if " " not in code:
        if len(code) <= 1:
            return None
        if len(code) == 3:
            return code[0]
        if len(code) == 4:
            return code[:3]
        return code[:4]

    # Group or subgroup: 'A01B 1/02' -> parts = ['A01B 1', '02']
    parts = code.split("/", 1)
    after_slash = parts[1]
    if after_slash == "00":
        # Main group: parent is subclass ('A01B 1/00' -> 'A01B')
        return code.split(" ")[0]
    # Subgroup: parent is the main group (replace suffix with '00')
    return parts[0] + "/00"


def _determine_sector(code: str) -> str:
    """Return CPC section (first letter) as sector code."""
    return code[0]


def _download_section_zip(section: str, data_dir: str) -> Optional[str]:
    """Download CPC section ZIP and extract XML, return path to XML file."""
    xml_path = os.path.join(data_dir, f"cpc-scheme-{section}.xml")
    if os.path.exists(xml_path):
        return xml_path

    url = _SECTION_ZIP_URL_TEMPLATE.format(section=section)
    print(f"  Downloading CPC section {section}: {url}")

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "WorldOfTaxanomy/0.1"})
        with urllib.request.urlopen(req, context=ctx, timeout=120) as response:
            zip_bytes = response.read()
    except Exception as exc:
        print(f"  WARNING: Could not download section {section}: {exc}")
        return None

    Path(data_dir).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # XML file inside is named like 'cpc-scheme-A.xml'
        xml_members = [m for m in zf.namelist() if m.endswith(".xml")]
        if not xml_members:
            print(f"  WARNING: No XML file found in section {section} ZIP")
            return None
        xml_data = zf.read(xml_members[0])
        Path(xml_path).write_bytes(xml_data)
        size_kb = len(xml_data) / 1024
        print(f"  Extracted {xml_members[0]}: {size_kb:.1f} KB")

    return xml_path


def _parse_cpc_xml(xml_path: str) -> list[tuple[str, str]]:
    """Parse CPC scheme XML and return list of (code, title) tuples."""
    nodes: list[tuple[str, str]] = []
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Remove namespace prefixes for easier tag matching
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]

    # Walk all classification-item elements
    for item in root.iter("classification-item"):
        symbol = item.get("classification-symbol", "").strip()
        if not symbol:
            continue

        # Get English title
        title = ""
        for title_elem in item.iter("class-title"):
            for text_part in title_elem.iter("title-part"):
                parts = []
                for child in text_part:
                    if child.text:
                        parts.append(child.text.strip())
                if parts:
                    title = " ".join(parts)
                    break
            if title:
                break

        if not title:
            # Fallback: get any text content from title elements
            for title_elem in item.iter("class-title"):
                text = "".join(title_elem.itertext()).strip()
                if text:
                    title = text
                    break

        if symbol and title:
            nodes.append((symbol, title))

    return nodes


async def ingest_patent_cpc(conn, data_dir: Optional[str] = None) -> int:
    """Ingest CPC patent classification hierarchy from EPO bulk XML.

    WARNING: This ingests ~260K codes across 9 sections. It takes several minutes.

    Downloads 9 section ZIPs (A-H, Y) from the EPO CPC bulk download page.
    Parses the XML scheme files and inserts all nodes.

    Returns total node count inserted.
    """
    print("  WARNING: Ingesting ~260K CPC codes - this will take several minutes.")

    local_dir = data_dir or _DEFAULT_DATA_DIR
    Path(local_dir).mkdir(parents=True, exist_ok=True)

    # Register system
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        *_SYSTEM_ROW,
    )

    total_count = 0

    for section in _SECTIONS:
        xml_path = _download_section_zip(section, local_dir)
        if not xml_path or not os.path.exists(xml_path):
            print(f"  Skipping section {section} (download failed)")
            continue

        print(f"  Parsing section {section}...")
        raw_nodes = _parse_cpc_xml(xml_path)

        if not raw_nodes:
            print(f"  No nodes found in section {section}")
            continue

        # Build set of all codes for leaf detection
        all_codes = {code for code, _ in raw_nodes}
        parent_codes = set()
        for code, _ in raw_nodes:
            parent = _determine_parent(code)
            if parent:
                parent_codes.add(parent)

        records = []
        for code, title in raw_nodes:
            level = _determine_level(code)
            parent = _determine_parent(code)
            sector = _determine_sector(code)
            is_leaf = code not in parent_codes

            records.append((
                "patent_cpc",
                code,
                title,
                level,
                parent,
                sector,
                is_leaf,
            ))

        section_count = 0
        for i in range(0, len(records), CHUNK):
            chunk = records[i: i + CHUNK]
            await conn.executemany(
                """INSERT INTO classification_node
                       (system_id, code, title, level, parent_code, sector_code, is_leaf)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (system_id, code) DO NOTHING""",
                chunk,
            )
            section_count += len(chunk)

        total_count += section_count
        print(f"  Section {section}: {section_count} nodes")

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'patent_cpc'",
        total_count,
    )

    return total_count
