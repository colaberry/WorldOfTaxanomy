"""Space Orbital Classification Types domain taxonomy ingester.

Classifies spacecraft operations and missions by orbital regime.
Orthogonal to payload type and mission application (domain_space).
Based on ITU/WRC orbital arc definitions, IADC debris mitigation guidelines,
and standard aerospace industry taxonomy.
Used by satellite operators, frequency coordinators, launch vehicle planners,
and space insurance underwriters.

Code prefix: dsporb_
System ID: domain_space_orbital
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
SPACE_ORBITAL_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dsporb_leo", "Low Earth Orbit (LEO, 160-2000 km)", 1, None),
    ("dsporb_leo_sso", "Sun-synchronous orbit (SSO, polar remote sensing, ~500-900 km)", 2, "dsporb_leo"),
    ("dsporb_leo_vleo", "Very Low Earth Orbit (VLEO, <450 km, drag-compensated)", 2, "dsporb_leo"),
    ("dsporb_leo_mega", "Mega-constellation LEO (Starlink, OneWeb, Kuiper planes)", 2, "dsporb_leo"),
    ("dsporb_leo_iss", "ISS altitude orbit (370-460 km) and rendezvous missions", 2, "dsporb_leo"),
    ("dsporb_meo", "Medium Earth Orbit (MEO, 2000-35786 km)", 1, None),
    ("dsporb_meo_gnss", "GNSS constellation MEO (GPS 20200 km, Galileo 23222 km)", 2, "dsporb_meo"),
    ("dsporb_meo_relay", "Data relay and comms MEO constellations (O3b/SES mPOWER)", 2, "dsporb_meo"),
    ("dsporb_geo", "Geostationary Orbit (GEO, 35786 km)", 1, None),
    ("dsporb_geo_comsat", "GEO communications satellites (broadband, broadcast)", 2, "dsporb_geo"),
    ("dsporb_geo_meteo", "GEO meteorological satellites (GOES, Meteosat, Himawari)", 2, "dsporb_geo"),
    ("dsporb_geo_eo", "GEO Earth observation (weather, environmental monitoring)", 2, "dsporb_geo"),
    ("dsporb_heo", "Highly Elliptical Orbit (HEO) and Special Regimes", 1, None),
    ("dsporb_heo_molniya", "Molniya orbit (high inclination, 12-hr period, polar coverage)", 2, "dsporb_heo"),
    ("dsporb_heo_tundra", "Tundra/quasi-stationary HEO orbit", 2, "dsporb_heo"),
    ("dsporb_heo_lunar", "Cislunar and lunar orbit (LLO, NRHO, Gateway station)", 2, "dsporb_heo"),
    ("dsporb_deep", "Deep Space and Interplanetary Trajectories", 1, None),
    ("dsporb_deep_l2", "L2 halo orbits (JWST, Gaia, ESA missions)", 2, "dsporb_deep"),
    ("dsporb_deep_mars", "Mars transfer and orbital insertion trajectories", 2, "dsporb_deep"),
    ("dsporb_deep_helio", "Heliocentric and solar observation trajectories", 2, "dsporb_deep"),
]

_DOMAIN_ROW = (
    "domain_space_orbital",
    "Space Orbital Classification Types",
    "Orbital regime classification: LEO, MEO, GEO, HEO, SSO, lunar and interplanetary",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3364', '5417', '4812']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_space_orbital(conn) -> int:
    """Ingest Space Orbital Classification Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_space_orbital'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_space_orbital",
        "Space Orbital Classification Types",
        "Orbital regime classification: LEO, MEO, GEO, HEO, SSO, lunar and interplanetary",
        "1.0",
        "Global",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in SPACE_ORBITAL_NODES if parent is not None}

    rows = [
        (
            "domain_space_orbital",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in SPACE_ORBITAL_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(SPACE_ORBITAL_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_space_orbital'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_space_orbital'",
        count,
    )

    naics_codes = [
        row["code"]
        for prefix in _NAICS_PREFIXES
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE $1",
            prefix + "%",
        )
    ]

    if naics_codes:
        await conn.executemany(
            """INSERT INTO node_taxonomy_link
                   (system_id, node_code, taxonomy_id, relevance)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
            [("naics_2022", code, "domain_space_orbital", "primary") for code in naics_codes],
        )

    return count
