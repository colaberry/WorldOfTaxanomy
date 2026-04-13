"""Information and Media Type domain taxonomy ingester.

Information/media taxonomy organizes content and software types (NAICS 51):
  Media Type    (dim_media*)  - print, broadcast, streaming, digital/social media
  Software Cat  (dim_soft*)   - enterprise, consumer, SaaS, embedded/OS
  Data Type     (dim_data*)   - structured, unstructured, real-time, geospatial
  Telecom       (dim_tele*)   - wireline, wireless/cellular, broadband/ISP, satellite

Source: NAB (National Association of Broadcasters) + Census NAICS 51 subsectors.
Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
INFO_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Media Type category --
    ("dim_media",            "Media and Publishing Types",                    1, None),
    ("dim_media_print",      "Print Media (newspaper, magazine, book)",      2, "dim_media"),
    ("dim_media_broadcast",  "Broadcast Media (TV, radio, OTA)",             2, "dim_media"),
    ("dim_media_stream",     "Streaming and OTT (Netflix, Spotify, podcast)", 2, "dim_media"),
    ("dim_media_digital",    "Digital and Social Media (web, social, blog)", 2, "dim_media"),
    ("dim_media_film",       "Film and Motion Picture",                       2, "dim_media"),

    # -- Software Category category --
    ("dim_soft",         "Software and Technology Categories",                1, None),
    ("dim_soft_ent",     "Enterprise Software (ERP, CRM, HR, finance)",     2, "dim_soft"),
    ("dim_soft_consumer","Consumer Software (gaming, productivity, mobile)", 2, "dim_soft"),
    ("dim_soft_saas",    "Software-as-a-Service (SaaS, cloud-native)",      2, "dim_soft"),
    ("dim_soft_embed",   "Embedded and Systems Software (OS, firmware)",    2, "dim_soft"),

    # -- Data Type category --
    ("dim_data",          "Data and Information Types",                       1, None),
    ("dim_data_struct",   "Structured Data (SQL, relational, tabular)",      2, "dim_data"),
    ("dim_data_unstruct", "Unstructured Data (text, images, video)",         2, "dim_data"),
    ("dim_data_rt",       "Real-Time and Streaming Data (events, IoT)",      2, "dim_data"),
    ("dim_data_geo",      "Geospatial and Location Data (GIS, maps)",        2, "dim_data"),

    # -- Telecommunications category --
    ("dim_tele",          "Telecommunications",                               1, None),
    ("dim_tele_wire",     "Wireline and Landline (PSTN, fiber, DSL)",       2, "dim_tele"),
    ("dim_tele_wireless", "Wireless and Cellular (4G LTE, 5G, Wi-Fi)",      2, "dim_tele"),
    ("dim_tele_isp",      "Broadband and Internet Service Provider (ISP)",  2, "dim_tele"),
    ("dim_tele_sat",      "Satellite Communications (Starlink, Viasat)",    2, "dim_tele"),
]

_DOMAIN_ROW = (
    "domain_info_media",
    "Information and Media Types",
    "Media, software, data and telecommunications type taxonomy for NAICS 51 Information sector",
    "WorldOfTaxanomy",
    None,
)

_NAICS_PREFIXES = ["51"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific information types."""
    parts = code.split("_")
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_info_media(conn) -> int:
    """Ingest Information and Media Type domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_info_media'), and links NAICS 51xxx nodes
    via node_taxonomy_link.

    Returns total info/media node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_info_media",
        "Information and Media Types",
        "Media, software, data and telecommunications type taxonomy for NAICS 51 Information sector",
        "1.0",
        "United States",
        "WorldOfTaxanomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in INFO_NODES if parent is not None}

    rows = [
        (
            "domain_info_media",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in INFO_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(INFO_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_info_media'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_info_media'",
        count,
    )

    naics_codes = [
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE '51%'"
        )
    ]

    await conn.executemany(
        """INSERT INTO node_taxonomy_link
               (system_id, node_code, taxonomy_id, relevance)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
        [("naics_2022", code, "domain_info_media", "primary") for code in naics_codes],
    )

    return count
