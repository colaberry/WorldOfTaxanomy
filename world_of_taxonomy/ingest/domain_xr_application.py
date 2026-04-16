"""Extended Reality Application Domain Types domain taxonomy ingester.

Classifies XR (extended reality), augmented reality (AR), virtual reality (VR)
and metaverse platform applications by use case and industry vertical.
Orthogonal to platform/technology type (domain_xr_meta) and business model.
Based on IDC XR market forecast taxonomy, Goldman Sachs Metaverse Report 2021,
and VRARA application classification.
Used by XR developers, platform operators, enterprise buyers, and investors.

Code prefix: dxrapp_
System ID: domain_xr_application
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
XR_APPLICATION_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dxrapp_gaming", "Gaming and Interactive Entertainment", 1, None),
    ("dxrapp_gaming_vr", "Immersive VR gaming (Meta Quest, PSVR, PC VR headsets)", 2, "dxrapp_gaming"),
    ("dxrapp_gaming_ar", "AR mobile gaming (Pokemon GO, Niantic, AR location experiences)", 2, "dxrapp_gaming"),
    ("dxrapp_gaming_social", "Social VR and virtual worlds (VRChat, Horizon Worlds, Rec Room)", 2, "dxrapp_gaming"),
    ("dxrapp_enterprise", "Enterprise Training and Collaboration", 1, None),
    ("dxrapp_enterprise_training", "VR safety and procedure training (PTC, Talespin, Strivr)", 2, "dxrapp_enterprise"),
    ("dxrapp_enterprise_collab", "Virtual collaboration and remote work (spatial computing, Meta Workrooms)", 2, "dxrapp_enterprise"),
    ("dxrapp_enterprise_mfg", "Digital twin and AR-guided assembly in manufacturing", 2, "dxrapp_enterprise"),
    ("dxrapp_health", "Healthcare and Medical Applications", 1, None),
    ("dxrapp_health_surgery", "Surgical planning, simulation and AR navigation (SurgicalAR)", 2, "dxrapp_health"),
    ("dxrapp_health_therapy", "VR therapy for PTSD, phobia, chronic pain (AppliedVR, Limbix)", 2, "dxrapp_health"),
    ("dxrapp_health_training", "Medical education and anatomy training (Immersive VR Education)", 2, "dxrapp_health"),
    ("dxrapp_aec", "Architecture, Engineering and Construction (AEC)", 1, None),
    ("dxrapp_aec_design", "Design visualization and client walkthroughs (BIM to VR)", 2, "dxrapp_aec"),
    ("dxrapp_aec_site", "On-site AR overlay for construction workers (HoloLens)", 2, "dxrapp_aec"),
    ("dxrapp_retail", "Retail and Commerce Applications", 1, None),
    ("dxrapp_retail_tryon", "Virtual try-on (AR clothing, eyewear, makeup, furniture)", 2, "dxrapp_retail"),
    ("dxrapp_retail_showroom", "Virtual showroom and 3D product configurators", 2, "dxrapp_retail"),
    ("dxrapp_education", "Education and Edtech", 1, None),
    ("dxrapp_education_k12", "K-12 immersive learning experiences (Labster, zSpace)", 2, "dxrapp_education"),
    ("dxrapp_education_higher", "Higher education virtual labs and field trips", 2, "dxrapp_education"),
]

_DOMAIN_ROW = (
    "domain_xr_application",
    "Extended Reality Application Domain Types",
    "XR, AR, VR and metaverse application domain classification across industry verticals",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['7112', '5415', '6111']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_xr_application(conn) -> int:
    """Ingest Extended Reality Application Domain Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_xr_application'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_xr_application",
        "Extended Reality Application Domain Types",
        "XR, AR, VR and metaverse application domain classification across industry verticals",
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

    parent_codes = {parent for _, _, _, parent in XR_APPLICATION_NODES if parent is not None}

    rows = [
        (
            "domain_xr_application",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in XR_APPLICATION_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(XR_APPLICATION_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_xr_application'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_xr_application'",
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
            [("naics_2022", code, "domain_xr_application", "primary") for code in naics_codes],
        )

    return count
