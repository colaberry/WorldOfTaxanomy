"""Information and Media Content Format Types domain taxonomy ingester.

Classifies media and information products by content format and delivery modality.
Orthogonal to revenue model, distribution platform, and media type.
Used by content strategists, audience analytics teams, ad sales, and media buyers
to map audiences, ad inventory, and content investment across format types.

Code prefix: dinfcnt_
System ID: domain_info_content
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
INFO_CONTENT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dinfcnt_video", "Video and Film Content Formats", 1, None),
    ("dinfcnt_video_shortform", "Short-form video (under 10 min: TikTok, YouTube Shorts, Reels)", 2, "dinfcnt_video"),
    ("dinfcnt_video_longform", "Long-form video (streaming series, documentaries, films)", 2, "dinfcnt_video"),
    ("dinfcnt_video_live", "Live video streaming (sports, news, events, gaming streams)", 2, "dinfcnt_video"),
    ("dinfcnt_video_ugc", "User-generated video (YouTube, Twitch creator content)", 2, "dinfcnt_video"),
    ("dinfcnt_audio", "Audio and Podcast Content Formats", 1, None),
    ("dinfcnt_audio_podcast", "Podcast (episodic audio storytelling and discussion)", 2, "dinfcnt_audio"),
    ("dinfcnt_audio_music", "Recorded music (albums, singles, playlists on Spotify, Apple)", 2, "dinfcnt_audio"),
    ("dinfcnt_audio_radio", "Broadcast and internet radio (terrestrial, satellite, streaming)", 2, "dinfcnt_audio"),
    ("dinfcnt_audio_audiobook", "Audiobooks and spoken word content (Audible, Libro.fm)", 2, "dinfcnt_audio"),
    ("dinfcnt_text", "Text and Written Content Formats", 1, None),
    ("dinfcnt_text_news", "News articles and breaking news (AP, Reuters, newspaper sites)", 2, "dinfcnt_text"),
    ("dinfcnt_text_newsletter", "Email newsletters and content subscriptions (Substack, Beehiiv)", 2, "dinfcnt_text"),
    ("dinfcnt_text_longread", "Long-form editorial and investigative journalism", 2, "dinfcnt_text"),
    ("dinfcnt_interactive", "Interactive and Game-Based Content", 1, None),
    ("dinfcnt_interactive_game", "Video games as content and entertainment medium", 2, "dinfcnt_interactive"),
    ("dinfcnt_interactive_quiz", "Quizzes, polls and interactive explainer journalism", 2, "dinfcnt_interactive"),
    ("dinfcnt_visual", "Visual and Social Content Formats", 1, None),
    ("dinfcnt_visual_photo", "Photography and photojournalism editorial content", 2, "dinfcnt_visual"),
    ("dinfcnt_visual_infographic", "Infographics, data visualization and explainer graphics", 2, "dinfcnt_visual"),
]

_DOMAIN_ROW = (
    "domain_info_content",
    "Information and Media Content Format Types",
    "Digital and traditional media content format and genre classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['51', '5110', '5120', '5191']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_info_content(conn) -> int:
    """Ingest Information and Media Content Format Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_info_content'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_info_content",
        "Information and Media Content Format Types",
        "Digital and traditional media content format and genre classification",
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

    parent_codes = {parent for _, _, _, parent in INFO_CONTENT_NODES if parent is not None}

    rows = [
        (
            "domain_info_content",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in INFO_CONTENT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(INFO_CONTENT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_info_content'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_info_content'",
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
            [("naics_2022", code, "domain_info_content", "primary") for code in naics_codes],
        )

    return count
