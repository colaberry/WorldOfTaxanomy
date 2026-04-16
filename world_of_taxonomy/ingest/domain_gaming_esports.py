"""Gaming and Esports domain taxonomy ingester.

Organizes gaming and esports sector types aligned with NAICS 7112
(Spectator sports) covering PC gaming, console gaming, mobile gaming,
esports competitive, game development, and streaming/content.

Code prefix: dge_
Categories: PC Gaming, Console Gaming, Mobile Gaming, Esports Competitive,
Game Development, Streaming/Content.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- PC Gaming --
    ("dge_pc",              "PC Gaming",                                          1, None),
    ("dge_pc_mmo",          "Massively Multiplayer Online Games (MMORPGs)",       2, "dge_pc"),
    ("dge_pc_fps",          "First-Person Shooter Games (FPS)",                   2, "dge_pc"),
    ("dge_pc_strategy",     "Real-Time and Turn-Based Strategy Games",            2, "dge_pc"),
    ("dge_pc_sim",          "Simulation and Sandbox Games",                       2, "dge_pc"),

    # -- Console Gaming --
    ("dge_console",         "Console Gaming",                                     1, None),
    ("dge_console_action",  "Action and Adventure Console Games",                 2, "dge_console"),
    ("dge_console_sports",  "Sports and Racing Console Games",                    2, "dge_console"),
    ("dge_console_rpg",     "Role-Playing Console Games (JRPGs, WRPGs)",          2, "dge_console"),
    ("dge_console_excl",    "Platform-Exclusive Console Titles",                  2, "dge_console"),

    # -- Mobile Gaming --
    ("dge_mobile",          "Mobile Gaming",                                      1, None),
    ("dge_mobile_casual",   "Casual and Hyper-Casual Mobile Games",               2, "dge_mobile"),
    ("dge_mobile_midcore",  "Mid-Core and Strategy Mobile Games",                 2, "dge_mobile"),
    ("dge_mobile_social",   "Social and Multiplayer Mobile Games",                2, "dge_mobile"),

    # -- Esports Competitive --
    ("dge_esport",          "Esports Competitive",                                1, None),
    ("dge_esport_league",   "Professional Esports Leagues and Tournaments",       2, "dge_esport"),
    ("dge_esport_team",     "Esports Team and Player Management",                 2, "dge_esport"),
    ("dge_esport_betting",  "Esports Betting and Fantasy Platforms",              2, "dge_esport"),

    # -- Game Development --
    ("dge_dev",             "Game Development",                                   1, None),
    ("dge_dev_engine",      "Game Engine and Middleware Development",              2, "dge_dev"),
    ("dge_dev_studio",      "Game Studio and Publisher Operations",               2, "dge_dev"),
    ("dge_dev_indie",       "Independent Game Development",                       2, "dge_dev"),

    # -- Streaming and Content --
    ("dge_stream",          "Streaming and Content",                              1, None),
    ("dge_stream_live",     "Live Game Streaming Platforms",                      2, "dge_stream"),
    ("dge_stream_video",    "Gaming Video Content and Reviews",                   2, "dge_stream"),
    ("dge_stream_merch",    "Gaming Merchandise and Brand Licensing",             2, "dge_stream"),
]

_DOMAIN_ROW = (
    "domain_gaming_esports",
    "Gaming and Esports Types",
    "Gaming and esports sector types covering PC, console, mobile, "
    "esports competitive, game development, and streaming/content",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes: 7112 (Spectator sports)
_NAICS_PREFIXES = ["7112"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific gaming/esports types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_gaming_esports(conn) -> int:
    """Ingest Gaming and Esports domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_gaming_esports'), and links NAICS 7112 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_gaming_esports",
        "Gaming and Esports Types",
        "Gaming and esports sector types covering PC, console, mobile, "
        "esports competitive, game development, and streaming/content",
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

    parent_codes = {parent for _, _, _, parent in NODES if parent is not None}

    rows = [
        (
            "domain_gaming_esports",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_gaming_esports'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_gaming_esports'",
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
            [("naics_2022", code, "domain_gaming_esports", "primary") for code in naics_codes],
        )

    return count
