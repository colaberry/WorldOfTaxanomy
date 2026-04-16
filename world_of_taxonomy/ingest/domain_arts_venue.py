"""Arts and Entertainment Venue and Distribution Platform Types domain taxonomy ingester.

Classifies arts and entertainment content distribution by venue and platform type.
Orthogonal to content type, monetization model, and creator structure.
Used by rights holders, booking agents, talent managers, and platform strategists
evaluating audience reach, revenue per venue, and distribution mix.

Code prefix: dartsvn_
System ID: domain_arts_venue
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
ARTS_VENUE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dartsvn_liveperform", "Live Performance Venues", 1, None),
    ("dartsvn_liveperform_arena", "Arena and stadium (10k+ capacity touring acts, sports)", 2, "dartsvn_liveperform"),
    ("dartsvn_liveperform_theater", "Concert hall and theater (500-5000 seats, symphony, ballet, theater)", 2, "dartsvn_liveperform"),
    ("dartsvn_liveperform_club", "Club and small venue (under 500 cap, indie, jazz, comedy)", 2, "dartsvn_liveperform"),
    ("dartsvn_screen", "Film and Video Screening Venues", 1, None),
    ("dartsvn_screen_multiplex", "Multiplex cinema / theater chain (AMC, Regal, Cinemark)", 2, "dartsvn_screen"),
    ("dartsvn_screen_arthouse", "Arthouse and indie cinema", 2, "dartsvn_screen"),
    ("dartsvn_screen_drive", "Drive-in and outdoor cinema", 2, "dartsvn_screen"),
    ("dartsvn_digital", "Digital Distribution Platforms", 1, None),
    ("dartsvn_digital_svod", "SVOD streaming (Netflix, Disney+, HBO Max, Prime Video)", 2, "dartsvn_digital"),
    ("dartsvn_digital_avod", "Ad-supported streaming (YouTube, Pluto TV, Tubi, Peacock Free)", 2, "dartsvn_digital"),
    ("dartsvn_digital_music", "Music streaming (Spotify, Apple Music, Amazon Music, Tidal)", 2, "dartsvn_digital"),
    ("dartsvn_digital_games", "Game platform storefronts (Steam, PlayStation Store, Xbox Game Pass)", 2, "dartsvn_digital"),
    ("dartsvn_exhibit", "Gallery, Museum and Exhibition Spaces", 1, None),
    ("dartsvn_exhibit_museum", "Museum and public gallery (permanent and traveling exhibitions)", 2, "dartsvn_exhibit"),
    ("dartsvn_exhibit_commercial", "Commercial gallery (art sales, artist representation)", 2, "dartsvn_exhibit"),
    ("dartsvn_market", "Online Marketplaces for Arts and Collectibles", 1, None),
    ("dartsvn_market_craft", "Handmade goods marketplaces (Etsy, Society6, Redbubble)", 2, "dartsvn_market"),
    ("dartsvn_market_nft", "NFT and digital art marketplace (OpenSea, Foundation, SuperRare)", 2, "dartsvn_market"),
]

_DOMAIN_ROW = (
    "domain_arts_venue",
    "Arts and Entertainment Venue and Distribution Platform Types",
    "Arts and entertainment venue, distribution channel and platform classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['71', '7111', '7112', '7113', '7114']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_arts_venue(conn) -> int:
    """Ingest Arts and Entertainment Venue and Distribution Platform Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_arts_venue'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_arts_venue",
        "Arts and Entertainment Venue and Distribution Platform Types",
        "Arts and entertainment venue, distribution channel and platform classification",
        "1.0",
        "United States",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in ARTS_VENUE_NODES if parent is not None}

    rows = [
        (
            "domain_arts_venue",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in ARTS_VENUE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(ARTS_VENUE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_arts_venue'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_arts_venue'",
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
            [("naics_2022", code, "domain_arts_venue", "primary") for code in naics_codes],
        )

    return count
