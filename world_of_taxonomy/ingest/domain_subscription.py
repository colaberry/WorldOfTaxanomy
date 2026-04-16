"""Ingest Subscription Business Model Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_subscription",
    "Subscription Model Types",
    "Subscription Business Model Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("sub_type", "Subscription Types", 1, None),
    ("sub_metric", "Key Metrics", 1, None),
    ("sub_pricing", "Pricing Models", 1, None),
    ("sub_type_media", "Media and content subscriptions", 2, "sub_type"),
    ("sub_type_soft", "Software subscriptions (SaaS)", 2, "sub_type"),
    ("sub_type_box", "Subscription boxes (curated products)", 2, "sub_type"),
    ("sub_type_meal", "Meal kit subscriptions", 2, "sub_type"),
    ("sub_type_member", "Membership clubs", 2, "sub_type"),
    ("sub_type_access", "Access subscriptions (coworking, vehicles)", 2, "sub_type"),
    ("sub_metric_mrr", "Monthly recurring revenue (MRR)", 2, "sub_metric"),
    ("sub_metric_churn", "Churn rate (voluntary, involuntary)", 2, "sub_metric"),
    ("sub_metric_ltv", "Customer lifetime value (CLV/LTV)", 2, "sub_metric"),
    ("sub_metric_cac", "Customer acquisition cost (CAC)", 2, "sub_metric"),
    ("sub_metric_arpu", "Average revenue per user (ARPU)", 2, "sub_metric"),
    ("sub_pricing_flat", "Flat-rate pricing", 2, "sub_pricing"),
    ("sub_pricing_tier", "Tiered pricing", 2, "sub_pricing"),
    ("sub_pricing_usage", "Usage-based pricing", 2, "sub_pricing"),
    ("sub_pricing_free", "Freemium model", 2, "sub_pricing"),
    ("sub_pricing_per", "Per-seat pricing", 2, "sub_pricing"),
]


async def ingest_domain_subscription(conn) -> int:
    """Insert or update Subscription Model Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_subscription"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_subscription", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_subscription",
    )
    return count
