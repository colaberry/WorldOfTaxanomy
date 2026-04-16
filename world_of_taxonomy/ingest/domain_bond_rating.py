"""Ingest Bond and Fixed Income Rating Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_bond_rating",
    "Bond Rating Types",
    "Bond and Fixed Income Rating Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("br_type", "Bond Types", 1, None),
    ("br_feature", "Bond Features", 1, None),
    ("br_market", "Market Segments", 1, None),
    ("br_type_govt", "Government bonds (sovereign, treasury)", 2, "br_type"),
    ("br_type_corp", "Corporate bonds (IG, HY)", 2, "br_type"),
    ("br_type_muni", "Municipal bonds (GO, revenue)", 2, "br_type"),
    ("br_type_agency", "Agency bonds (GSE, MBS pass-through)", 2, "br_type"),
    ("br_type_convert", "Convertible bonds", 2, "br_type"),
    ("br_feature_fixed", "Fixed coupon", 2, "br_feature"),
    ("br_feature_float", "Floating rate note (FRN)", 2, "br_feature"),
    ("br_feature_zero", "Zero coupon", 2, "br_feature"),
    ("br_feature_call", "Callable bond", 2, "br_feature"),
    ("br_feature_put", "Puttable bond", 2, "br_feature"),
    ("br_feature_link", "Inflation-linked (TIPS, linkers)", 2, "br_feature"),
    ("br_market_dom", "Domestic bond market", 2, "br_market"),
    ("br_market_euro", "Eurobond market", 2, "br_market"),
    ("br_market_em", "Emerging market debt", 2, "br_market"),
    ("br_market_green", "Green and social bonds", 2, "br_market"),
]


async def ingest_domain_bond_rating(conn) -> int:
    """Insert or update Bond Rating Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_bond_rating"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_bond_rating", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_bond_rating",
    )
    return count
