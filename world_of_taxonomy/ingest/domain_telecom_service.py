"""Telecom Service domain taxonomy ingester.

Organizes telecommunications service types aligned with NAICS 5171
(Wired telecommunications carriers), 5172 (Wireless), and 5174 (Satellite).

Code prefix: dts_
Categories: Mobile, Fixed-Line, Internet/Broadband, Enterprise Telecom, Satellite.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
TELECOM_SERVICE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Mobile --
    ("dts_mobile",           "Mobile Telecommunications Services",                    1, None),
    ("dts_mobile_voice",     "Mobile Voice Services (VoLTE, VoNR, roaming)",          2, "dts_mobile"),
    ("dts_mobile_data",      "Mobile Data Services (4G LTE, 5G NR, mobile broadband)", 2, "dts_mobile"),
    ("dts_mobile_mvno",      "Mobile Virtual Network Operators (MVNO, MVNE)",         2, "dts_mobile"),
    ("dts_mobile_iot",       "Mobile IoT and M2M Services (NB-IoT, LTE-M, eSIM)",    2, "dts_mobile"),

    # -- Fixed-Line --
    ("dts_fixed",            "Fixed-Line Telecommunications Services",                1, None),
    ("dts_fixed_pstn",       "PSTN and Landline Voice (copper, ISDN, VoIP migration)", 2, "dts_fixed"),
    ("dts_fixed_voip",       "Business VoIP and Unified Communications (SIP, PBX)",   2, "dts_fixed"),
    ("dts_fixed_trunk",      "SIP Trunking and Wholesale Voice (carrier interconnect)", 2, "dts_fixed"),
    ("dts_fixed_fwa",        "Fixed Wireless Access (mmWave, CBRS, point-to-point)",  2, "dts_fixed"),

    # -- Internet and Broadband --
    ("dts_internet",         "Internet and Broadband Services",                       1, None),
    ("dts_internet_fiber",   "Fiber Broadband (FTTH, FTTP, GPON, XGS-PON)",          2, "dts_internet"),
    ("dts_internet_cable",   "Cable Broadband (DOCSIS 3.1, HFC networks)",            2, "dts_internet"),
    ("dts_internet_dsl",     "DSL and Copper Broadband (VDSL2, G.fast, bonding)",     2, "dts_internet"),
    ("dts_internet_isp",     "Internet Service Provider Operations (peering, transit)", 2, "dts_internet"),
    ("dts_internet_cdn",     "Content Delivery Networks (edge caching, streaming)",    2, "dts_internet"),

    # -- Enterprise Telecom --
    ("dts_enterprise",       "Enterprise Telecommunications Services",                1, None),
    ("dts_enterprise_mpls",  "MPLS and SD-WAN Services (managed WAN, hybrid overlay)", 2, "dts_enterprise"),
    ("dts_enterprise_ucaas", "UCaaS and CPaaS Platforms (cloud PBX, video, messaging)", 2, "dts_enterprise"),
    ("dts_enterprise_dedi",  "Dedicated Internet and Ethernet Services (DIA, EVPL)",   2, "dts_enterprise"),
    ("dts_enterprise_colo",  "Colocation and Data Center Services (interconnection)",  2, "dts_enterprise"),

    # -- Satellite --
    ("dts_satellite",        "Satellite Telecommunications Services",                  1, None),
    ("dts_satellite_geo",    "Geostationary Satellite Services (DTH, VSAT, maritime)", 2, "dts_satellite"),
    ("dts_satellite_leo",    "Low-Earth Orbit Broadband (LEO constellations, D2D)",    2, "dts_satellite"),
    ("dts_satellite_meo",    "Medium-Earth Orbit Services (MEO relay, navigation)",    2, "dts_satellite"),
]

_DOMAIN_ROW = (
    "domain_telecom_service",
    "Telecom Service Types",
    "Mobile, fixed-line, internet and broadband, enterprise telecom, "
    "and satellite telecommunications service taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 5171 (Wired), 5172 (Wireless), 5174 (Satellite)
_NAICS_PREFIXES = ["5171", "5172", "5174"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific telecom service types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_telecom_service(conn) -> int:
    """Ingest Telecom Service domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_telecom_service'), and links NAICS 5171/5172/5174
    nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_telecom_service",
        "Telecom Service Types",
        "Mobile, fixed-line, internet and broadband, enterprise telecom, "
        "and satellite telecommunications service taxonomy",
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

    parent_codes = {parent for _, _, _, parent in TELECOM_SERVICE_NODES if parent is not None}

    rows = [
        (
            "domain_telecom_service",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in TELECOM_SERVICE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(TELECOM_SERVICE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_telecom_service'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_telecom_service'",
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
            [("naics_2022", code, "domain_telecom_service", "primary") for code in naics_codes],
        )

    return count
