"""Ingest Digital Banking and Neobank Service Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_digital_banking",
    "Digital Banking Service Types",
    "Digital Banking and Neobank Service Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("db_core", "Core Digital Banking", 1, None),
    ("db_pay", "Payments and Transfers", 1, None),
    ("db_lend", "Digital Lending", 1, None),
    ("db_infra", "Banking Infrastructure", 1, None),
    ("db_core_neo", "Neobank (digital-only bank)", 2, "db_core"),
    ("db_core_chal", "Challenger bank (digital + limited branches)", 2, "db_core"),
    ("db_core_baas", "Banking-as-a-Service (BaaS)", 2, "db_core"),
    ("db_core_embed", "Embedded banking", 2, "db_core"),
    ("db_pay_p2p", "P2P payments", 2, "db_pay"),
    ("db_pay_inst", "Instant payments (real-time)", 2, "db_pay"),
    ("db_pay_cross", "Cross-border remittances", 2, "db_pay"),
    ("db_pay_wallet", "Digital wallets", 2, "db_pay"),
    ("db_pay_bnpl", "Buy now pay later (BNPL)", 2, "db_pay"),
    ("db_lend_cons", "Consumer digital lending", 2, "db_lend"),
    ("db_lend_smb", "SMB lending platforms", 2, "db_lend"),
    ("db_lend_mort", "Digital mortgage origination", 2, "db_lend"),
    ("db_lend_p2p", "P2P lending marketplaces", 2, "db_lend"),
    ("db_infra_core", "Core banking platform", 2, "db_infra"),
    ("db_infra_api", "Open banking APIs", 2, "db_infra"),
    ("db_infra_kyc", "Digital KYC and identity verification", 2, "db_infra"),
    ("db_infra_fraud", "Fraud detection and prevention", 2, "db_infra"),
]


async def ingest_domain_digital_banking(conn) -> int:
    """Insert or update Digital Banking Service Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_digital_banking"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_digital_banking", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_digital_banking",
    )
    return count
