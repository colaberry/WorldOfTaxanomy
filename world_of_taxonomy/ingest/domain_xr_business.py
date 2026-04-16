"""Extended Reality Business Model Types domain taxonomy ingester.

Classifies XR and metaverse companies by their primary revenue and business model.
Orthogonal to application domain and platform/technology type.
Used by XR investors, platform strategists, and business development teams
to categorize competitive positioning and monetization approaches.

Code prefix: dxrbiz_
System ID: domain_xr_business
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
XR_BUSINESS_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dxrbiz_hardware", "Hardware and Device Sales", 1, None),
    ("dxrbiz_hardware_headset", "Standalone and tethered headset sales (Meta, Apple Vision Pro, Pico)", 2, "dxrbiz_hardware"),
    ("dxrbiz_hardware_accessory", "Controllers, haptics, and peripheral accessories", 2, "dxrbiz_hardware"),
    ("dxrbiz_hardware_b2b", "Enterprise hardware bundles and managed device programs", 2, "dxrbiz_hardware"),
    ("dxrbiz_platform", "Platform and Marketplace Revenue", 1, None),
    ("dxrbiz_platform_appstore", "App store revenue share (Meta Quest Store, PlayStation Store)", 2, "dxrbiz_platform"),
    ("dxrbiz_platform_virtualgood", "Virtual goods, digital item and avatar economy", 2, "dxrbiz_platform"),
    ("dxrbiz_platform_land", "Virtual land and real estate in metaverse platforms", 2, "dxrbiz_platform"),
    ("dxrbiz_saas", "Enterprise SaaS and Subscription Licensing", 1, None),
    ("dxrbiz_saas_b2b", "Per-seat or usage-based B2B licensing (training, simulation)", 2, "dxrbiz_saas"),
    ("dxrbiz_saas_sdk", "Developer SDK and platform fee for XR app builders", 2, "dxrbiz_saas"),
    ("dxrbiz_ads", "Advertising and Sponsorship Revenue", 1, None),
    ("dxrbiz_ads_immersive", "Immersive advertising in virtual environments (in-game, branded spaces)", 2, "dxrbiz_ads"),
    ("dxrbiz_ads_ar_overlay", "AR display advertising (location-based AR overlays)", 2, "dxrbiz_ads"),
    ("dxrbiz_services", "Professional Services and Content Creation", 1, None),
    ("dxrbiz_services_content", "XR content production and experience development", 2, "dxrbiz_services"),
    ("dxrbiz_services_consult", "XR strategy consulting and implementation services", 2, "dxrbiz_services"),
]

_DOMAIN_ROW = (
    "domain_xr_business",
    "Extended Reality Business Model Types",
    "XR, AR, VR and metaverse platform and application business model classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['7112', '5415', '4541']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_xr_business(conn) -> int:
    """Ingest Extended Reality Business Model Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_xr_business'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_xr_business",
        "Extended Reality Business Model Types",
        "XR, AR, VR and metaverse platform and application business model classification",
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

    parent_codes = {parent for _, _, _, parent in XR_BUSINESS_NODES if parent is not None}

    rows = [
        (
            "domain_xr_business",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in XR_BUSINESS_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(XR_BUSINESS_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_xr_business'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_xr_business'",
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
            [("naics_2022", code, "domain_xr_business", "primary") for code in naics_codes],
        )

    return count
