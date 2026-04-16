"""Ingest Asset-Backed Securitization Structure Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_securitization",
    "Asset Securitization Types",
    "Asset-Backed Securitization Structure Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("sec_type", "Securitization Types", 1, None),
    ("sec_tranche", "Tranche Structure", 1, None),
    ("sec_asset", "Underlying Assets", 1, None),
    ("sec_type_mbs", "Mortgage-backed securities (MBS)", 2, "sec_type"),
    ("sec_type_abs", "Asset-backed securities (ABS)", 2, "sec_type"),
    ("sec_type_cdo", "Collateralized debt obligations (CDO)", 2, "sec_type"),
    ("sec_type_clo", "Collateralized loan obligations (CLO)", 2, "sec_type"),
    ("sec_type_cmbs", "Commercial MBS (CMBS)", 2, "sec_type"),
    ("sec_tranche_sr", "Senior tranche (AAA/AA)", 2, "sec_tranche"),
    ("sec_tranche_mezz", "Mezzanine tranche (A/BBB)", 2, "sec_tranche"),
    ("sec_tranche_eq", "Equity / first-loss tranche", 2, "sec_tranche"),
    ("sec_tranche_io", "Interest-only strip (IO)", 2, "sec_tranche"),
    ("sec_tranche_po", "Principal-only strip (PO)", 2, "sec_tranche"),
    ("sec_asset_auto", "Auto loans and leases", 2, "sec_asset"),
    ("sec_asset_card", "Credit card receivables", 2, "sec_asset"),
    ("sec_asset_student", "Student loans", 2, "sec_asset"),
    ("sec_asset_equip", "Equipment leases", 2, "sec_asset"),
]


async def ingest_domain_securitization(conn) -> int:
    """Insert or update Asset Securitization Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_securitization"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_securitization", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_securitization",
    )
    return count
