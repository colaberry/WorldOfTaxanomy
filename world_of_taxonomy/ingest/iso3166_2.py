"""ISO 3166-2 Subdivisions ingester.

Source: pycountry library (wraps ISO 3166-2 data)
License: LGPL (pycountry library); ISO 3166-2 data is publicly available
Install: pip install pycountry

Hierarchy:
  L0 - Country (alpha-2 code, e.g. "US")
  L1 - Subdivision (e.g. "US-CA" California, "DE-BY" Bavaria)

~5,046 subdivisions across 200 countries.
"""
from typing import Optional


def _extract_country(code: str) -> str:
    """Extract the 2-letter country code from a subdivision code or passthrough."""
    if "-" in code:
        return code.split("-")[0]
    return code


def _determine_level(code: str) -> int:
    """Return 0 for country codes, 1 for subdivision codes."""
    if "-" in code:
        return 1
    return 0


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code: country for subdivisions, None for countries."""
    if "-" in code:
        return _extract_country(code)
    return None


async def ingest_iso3166_2(conn, path=None) -> int:
    """Ingest ISO 3166-2 subdivisions into the database.

    Uses the pycountry library (pip install pycountry).
    Returns total number of nodes inserted (country stubs + subdivisions).
    """
    try:
        import pycountry
    except ImportError:
        raise ImportError(
            "pycountry is required for ISO 3166-2 ingestion. "
            "Install with: pip install pycountry"
        )

    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "iso_3166_2",
        "ISO 3166-2",
        "ISO 3166-2 Country Subdivisions",
        "2023",
        "Global",
        "ISO",
    )

    subdivisions = list(pycountry.subdivisions)

    # Collect unique countries that have subdivisions
    country_codes = sorted({s.country_code for s in subdivisions})

    seq = 0
    count = 0

    # -- Insert country stub nodes (L0) --
    for alpha2 in country_codes:
        seq += 1
        country = pycountry.countries.get(alpha_2=alpha2)
        title = country.name if country else alpha2
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT DO NOTHING""",
            "iso_3166_2", alpha2, title, 0, None, alpha2, False, seq,
        )
        count += 1

    # -- Insert subdivision nodes (L1) --
    for sub in sorted(subdivisions, key=lambda s: s.code):
        seq += 1
        parent = sub.parent_code if sub.parent_code else _extract_country(sub.code)
        # If parent is not a subdivision code (i.e. no hyphen), it is the country
        if "-" not in parent:
            parent = _extract_country(sub.code)
        sector = _extract_country(sub.code)

        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT DO NOTHING""",
            "iso_3166_2", sub.code, sub.name, 1, parent, sector, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "iso_3166_2",
    )
    return count
