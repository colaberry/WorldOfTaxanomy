"""ISIC Rev 4 ingester.

Parses ISIC Rev 4 structure from an Excel file.
Source: https://unstats.un.org/unsd/classifications/Econ/isic
Alternative: ILO provides clean Excel at various URLs.
"""

from pathlib import Path
from typing import Optional

import openpyxl

from world_of_taxanomy.ingest.base import ensure_data_file

# ISIC Rev 4 structure file URLs (try multiple sources)
ISIC_REV4_URLS = [
    "https://unstats.un.org/unsd/classifications/Econ/Download/In%20Text/ISIC_Rev_4_english_structure.Txt",
]

ISIC_REV4_LOCAL = Path("data/isic/ISIC_Rev_4_structure.txt")

# Section to division mapping for ISIC Rev 4
# Sections use letter codes, divisions are 2-digit numeric
ISIC_SECTION_DIVISIONS = {
    "A": range(1, 4),      # 01-03
    "B": range(5, 10),     # 05-09
    "C": range(10, 34),    # 10-33
    "D": range(35, 36),    # 35
    "E": range(36, 40),    # 36-39
    "F": range(41, 44),    # 41-43
    "G": range(45, 48),    # 45-47
    "H": range(49, 54),    # 49-53
    "I": range(55, 57),    # 55-56
    "J": range(58, 64),    # 58-63
    "K": range(64, 67),    # 64-66
    "L": range(68, 69),    # 68
    "M": range(69, 76),    # 69-75
    "N": range(77, 83),    # 77-82
    "O": range(84, 85),    # 84
    "P": range(85, 86),    # 85
    "Q": range(86, 89),    # 86-88
    "R": range(90, 94),    # 90-93
    "S": range(94, 97),    # 94-96
    "T": range(97, 99),    # 97-98
    "U": range(99, 100),   # 99
}

# Build reverse lookup: division number -> section letter
_DIV_TO_SECTION = {}
for section, divs in ISIC_SECTION_DIVISIONS.items():
    for d in divs:
        _DIV_TO_SECTION[d] = section


def _get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


def _determine_level(code: str) -> int:
    """Determine hierarchy level for ISIC code.

    Letter = level 0 (section)
    2-digit = level 1 (division)
    3-digit = level 2 (group)
    4-digit = level 3 (class)
    """
    if code.isalpha():
        return 0
    return len(code) - 1


def _determine_parent(code: str) -> Optional[str]:
    """Determine parent code for ISIC code."""
    if code.isalpha():
        return None  # Sections have no parent

    if len(code) == 2:
        # Division -> Section
        div_num = int(code)
        return _DIV_TO_SECTION.get(div_num)

    # Group (3-digit) -> Division (2-digit)
    # Class (4-digit) -> Group (3-digit)
    return code[:-1]


def _determine_sector(code: str) -> str:
    """Determine top-level section for an ISIC code."""
    if code.isalpha():
        return code
    div_num = int(code[:2])
    return _DIV_TO_SECTION.get(div_num, "?")


async def ingest_isic_rev4(conn, file_path: Optional[Path] = None) -> int:
    """Ingest ISIC Rev 4 codes.

    Tries to parse from the UN's text file format first (tab-separated),
    falls back to Excel if available.

    Args:
        conn: asyncpg connection
        file_path: Path to data file. Downloads if None.

    Returns:
        Number of codes ingested.
    """
    # Register the classification system
    await conn.execute("""
        INSERT INTO classification_system (id, name, full_name, region, version, authority, url, tint_color)
        VALUES ('isic_rev4', 'ISIC Rev 4',
                'International Standard Industrial Classification of All Economic Activities, Rev.4',
                'Global (UN)', 'Rev 4', 'United Nations Statistics Division',
                'https://unstats.un.org/unsd/classifications/Econ/isic', NULL)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
    """)

    if file_path is None:
        file_path = ensure_data_file(
            ISIC_REV4_URLS[0],
            _get_project_root() / ISIC_REV4_LOCAL,
        )

    # Parse the text file (tab-separated: Code\tDescription)
    nodes = []
    seq = 0
    seen_codes = set()

    with open(file_path, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Try tab-separated first, then comma
            if '\t' in line:
                parts = line.split('\t', 1)
            elif ',' in line:
                parts = line.split(',', 1)
            else:
                continue

            if len(parts) < 2:
                continue

            code = parts[0].strip().strip('"')
            title = parts[1].strip().strip('"')

            # Skip headers
            if code.lower() in ('code', 'isic4', 'isic'):
                continue

            # Validate code format
            if not (code.isalpha() and len(code) == 1) and not code.isdigit():
                continue

            if code in seen_codes:
                continue
            seen_codes.add(code)

            seq += 1
            level = _determine_level(code)
            parent = _determine_parent(code)
            sector = _determine_sector(code)

            nodes.append((code, title, level, parent, sector, seq))

    # Determine leaf status
    parent_set = {n[3] for n in nodes if n[3] is not None}

    count = 0
    for code, title, level, parent, sector, seq_order in nodes:
        is_leaf = code not in parent_set
        await conn.execute("""
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
            VALUES ('isic_rev4', $1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (system_id, code) DO NOTHING
        """, code, title, level, parent, sector, is_leaf, seq_order)
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'isic_rev4'",
        count,
    )

    print(f"  Ingested {count} ISIC Rev 4 codes")
    return count
