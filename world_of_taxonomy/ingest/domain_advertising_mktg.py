"""Advertising and Marketing Services domain taxonomy ingester.

Advertising and marketing taxonomy organizes marketing channels and strategies:
  Digital Advertising  (dam_digital*)   - search, social, programmatic, video, display
  Traditional Media    (dam_trad*)      - TV, radio, print, outdoor, direct mail
  Content Marketing    (dam_content*)   - SEO, blog, email, influencer, podcast
  Performance Mktg     (dam_perf*)      - affiliate, CPA, retargeting, lead gen
  Brand Strategy       (dam_brand*)     - positioning, identity, PR, sponsorship

Source: IAB (Interactive Advertising Bureau) and ANA (Association of National
Advertisers) frameworks. Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
ADVERTISING_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Digital Advertising category --
    ("dam_digital",            "Digital Advertising",                                  1, None),
    ("dam_digital_search",     "Search Engine Marketing (SEM, PPC, Google Ads)",      2, "dam_digital"),
    ("dam_digital_social",     "Social Media Advertising (Meta, TikTok, LinkedIn)",   2, "dam_digital"),
    ("dam_digital_program",    "Programmatic Display (DSP, RTB, ad exchange)",        2, "dam_digital"),
    ("dam_digital_video",      "Digital Video Advertising (CTV, OTT, pre-roll)",     2, "dam_digital"),
    ("dam_digital_native",     "Native Advertising (sponsored content, in-feed)",     2, "dam_digital"),

    # -- Traditional Media category --
    ("dam_trad",               "Traditional Media Advertising",                        1, None),
    ("dam_trad_tv",            "Television Advertising (broadcast, cable, spot)",      2, "dam_trad"),
    ("dam_trad_radio",         "Radio Advertising (terrestrial, satellite, streaming)",2, "dam_trad"),
    ("dam_trad_print",         "Print Advertising (newspaper, magazine, insert)",      2, "dam_trad"),
    ("dam_trad_ooh",           "Out-of-Home (billboard, transit, DOOH)",              2, "dam_trad"),
    ("dam_trad_direct",        "Direct Mail and Catalog Marketing",                   2, "dam_trad"),

    # -- Content Marketing category --
    ("dam_content",            "Content Marketing",                                    1, None),
    ("dam_content_seo",        "SEO and Organic Search (content optimization)",       2, "dam_content"),
    ("dam_content_blog",       "Blog and Long-Form Content (thought leadership)",     2, "dam_content"),
    ("dam_content_email",      "Email Marketing (newsletter, drip, lifecycle)",        2, "dam_content"),
    ("dam_content_influencer", "Influencer Marketing (creator, KOL, ambassador)",     2, "dam_content"),
    ("dam_content_podcast",    "Podcast and Audio Content Marketing",                  2, "dam_content"),

    # -- Performance Marketing category --
    ("dam_perf",               "Performance Marketing",                                1, None),
    ("dam_perf_affiliate",     "Affiliate Marketing (CPA, commission-based)",         2, "dam_perf"),
    ("dam_perf_retarget",      "Retargeting and Remarketing (pixel, CRM-based)",      2, "dam_perf"),
    ("dam_perf_leadgen",       "Lead Generation (form fill, gated content, CPL)",     2, "dam_perf"),
    ("dam_perf_conversion",    "Conversion Rate Optimization (A/B test, landing page)",2, "dam_perf"),

    # -- Brand Strategy category --
    ("dam_brand",              "Brand Strategy and Communications",                    1, None),
    ("dam_brand_position",     "Brand Positioning and Identity (naming, visual)",      2, "dam_brand"),
    ("dam_brand_pr",           "Public Relations (media relations, crisis comms)",     2, "dam_brand"),
    ("dam_brand_sponsor",      "Sponsorship and Event Marketing (sports, cultural)",   2, "dam_brand"),
    ("dam_brand_research",     "Market Research and Consumer Insights (survey, focus)",2, "dam_brand"),
]

_DOMAIN_ROW = (
    "domain_advertising_mktg",
    "Advertising and Marketing Services Types",
    "Digital, traditional, content, performance and brand marketing taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["5418"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific advertising types."""
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


async def ingest_domain_advertising_mktg(conn) -> int:
    """Ingest Advertising and Marketing Services domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_advertising_mktg'), and links NAICS 5418xxx nodes
    via node_taxonomy_link.

    Returns total advertising/marketing node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_advertising_mktg",
        "Advertising and Marketing Services Types",
        "Digital, traditional, content, performance and brand marketing taxonomy",
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

    parent_codes = {parent for _, _, _, parent in ADVERTISING_NODES if parent is not None}

    rows = [
        (
            "domain_advertising_mktg",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in ADVERTISING_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(ADVERTISING_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_advertising_mktg'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_advertising_mktg'",
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
            [("naics_2022", code, "domain_advertising_mktg", "primary") for code in naics_codes],
        )

    return count
