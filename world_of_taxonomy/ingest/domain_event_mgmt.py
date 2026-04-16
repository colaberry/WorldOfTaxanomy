"""Ingest Event Management and Experience Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_event_mgmt",
    "Event Management Types",
    "Event Management and Experience Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("evt_type", "Event Types", 1, None),
    ("evt_format", "Event Formats", 1, None),
    ("evt_svc", "Event Services", 1, None),
    ("evt_type_corp", "Corporate events (conferences, retreats)", 2, "evt_type"),
    ("evt_type_trade", "Trade shows and exhibitions", 2, "evt_type"),
    ("evt_type_concert", "Concerts and festivals", 2, "evt_type"),
    ("evt_type_sport", "Sporting events", 2, "evt_type"),
    ("evt_type_social", "Social events (weddings, galas)", 2, "evt_type"),
    ("evt_format_inperson", "In-person events", 2, "evt_format"),
    ("evt_format_virtual", "Virtual events", 2, "evt_format"),
    ("evt_format_hybrid", "Hybrid events", 2, "evt_format"),
    ("evt_format_popup", "Pop-up experiences", 2, "evt_format"),
    ("evt_svc_venue", "Venue management", 2, "evt_svc"),
    ("evt_svc_av", "Audio-visual production", 2, "evt_svc"),
    ("evt_svc_cater", "Catering and hospitality", 2, "evt_svc"),
    ("evt_svc_reg", "Registration and ticketing", 2, "evt_svc"),
    ("evt_svc_market", "Event marketing and promotion", 2, "evt_svc"),
    ("evt_svc_analytics", "Event analytics and ROI measurement", 2, "evt_svc"),
]


async def ingest_domain_event_mgmt(conn) -> int:
    """Insert or update Event Management Types system and its nodes. Returns node count."""
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count,
                source_url, data_provenance, license)
           VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9)
           ON CONFLICT (id) DO UPDATE
               SET node_count = 0, source_url = EXCLUDED.source_url,
                   data_provenance = EXCLUDED.data_provenance, license = EXCLUDED.license""",
        *_SYSTEM_ROW, _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute(
        "DELETE FROM classification_node WHERE system_id = $1", "domain_event_mgmt"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_event_mgmt", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_event_mgmt",
    )
    return count
