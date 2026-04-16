"""ISO 3166-1 Countries ingester.

Source: https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv
License: CC0 (public domain)

Hierarchy:
  L0 - Continent (UN M.49 region code, e.g. "002" = Africa)
  L1 - Sub-region (UN M.49 sub-region code, e.g. "014" = Eastern Africa)
  L2 - Country (ISO 3166-1 alpha-2 code, e.g. "KE" = Kenya)

Countries with no region assignment (e.g. Antarctica) are placed at L2
with parent_code=None and sector_code="ZZ".
"""
import csv
import re
from typing import Optional

from world_of_taxonomy.ingest.base import ensure_data_file

DATA_URL = (
    "https://raw.githubusercontent.com/lukes/"
    "ISO-3166-Countries-with-Regional-Codes/master/all/all.csv"
)
DATA_PATH = "data/iso3166_all.csv"

# UN M.49 top-level continental region codes
_CONTINENT_CODES = frozenset({"002", "019", "142", "150", "009"})

# Continent display names keyed by M.49 code
_CONTINENT_NAMES = {
    "002": "Africa",
    "019": "Americas",
    "142": "Asia",
    "150": "Europe",
    "009": "Oceania",
}


def _clean_code(code) -> str:
    """Zero-pad numeric region codes to 3 digits. Pass alpha-2 codes through unchanged."""
    s = str(code).strip()
    if re.match(r"^\d+$", s):
        return str(int(s)).zfill(3)
    return s


def _determine_level(code: str) -> int:
    """Return hierarchy level: 0=continent, 1=sub-region, 2=country (alpha-2)."""
    if re.match(r"^[A-Z]{2}$", code):
        return 2
    if code in _CONTINENT_CODES:
        return 0
    return 1


async def ingest_iso3166_1(conn, path=None) -> int:
    """Ingest ISO 3166-1 country hierarchy into the database.

    Returns total number of nodes inserted (continents + sub-regions + countries).
    """
    path = path or DATA_PATH
    ensure_data_file(DATA_URL, path)

    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "iso_3166_1",
        "ISO 3166-1",
        "ISO 3166-1 Countries",
        "2023",
        "Global",
        "ISO / UN Statistics Division",
    )

    # -- Parse CSV --
    continents: dict[str, str] = {}        # code -> name
    subregions: dict[str, tuple] = {}      # code -> (name, continent_code)
    countries: list[tuple] = []            # (alpha2, name, subregion_code, continent_code)

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            alpha2 = row["alpha-2"].strip()
            name = row["name"].strip()
            region_code = row.get("region-code", "").strip()
            subregion_code = row.get("sub-region-code", "").strip()

            if region_code:
                rc = _clean_code(region_code)
                continents.setdefault(rc, row["region"].strip())
            if subregion_code and region_code:
                src = _clean_code(subregion_code)
                rc = _clean_code(region_code)
                subregions.setdefault(src, (row["sub-region"].strip(), rc))

            countries.append((alpha2, name, subregion_code, region_code))

    # -- Insert continent nodes (L0) --
    seq = 0
    for code, title in sorted(continents.items()):
        seq += 1
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT DO NOTHING""",
            "iso_3166_1", code, title, 0, None, code, False, seq,
        )

    # -- Insert sub-region nodes (L1) --
    for code, (title, continent_code) in sorted(subregions.items()):
        seq += 1
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT DO NOTHING""",
            "iso_3166_1", code, title, 1, continent_code, continent_code, False, seq,
        )

    # -- Insert country nodes (L2) --
    parent_set: set[str] = set()  # all codes that are parents (i.e. not leaves among countries)
    # countries are always leaves in this system
    for alpha2, name, subregion_code, region_code in countries:
        seq += 1
        if subregion_code:
            parent = _clean_code(subregion_code)
            sector = _clean_code(region_code) if region_code else "ZZ"
        else:
            parent = None
            sector = "ZZ"

        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT DO NOTHING""",
            "iso_3166_1", alpha2, name, 2, parent, sector, True, seq,
        )

    # -- Update node count --
    count = len(continents) + len(subregions) + len(countries)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "iso_3166_1",
    )
    return count
