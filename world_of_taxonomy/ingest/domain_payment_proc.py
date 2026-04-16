"""Ingest Payment Processing and Settlement Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_payment_proc",
    "Payment Processing Types",
    "Payment Processing and Settlement Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("pp_meth", "Payment Methods", 1, None),
    ("pp_infra", "Processing Infrastructure", 1, None),
    ("pp_settle", "Settlement Types", 1, None),
    ("pp_meth_card", "Card payments (credit, debit, prepaid)", 2, "pp_meth"),
    ("pp_meth_ach", "ACH and bank transfers", 2, "pp_meth"),
    ("pp_meth_wire", "Wire transfers (SWIFT, CHIPS, Fedwire)", 2, "pp_meth"),
    ("pp_meth_mobile", "Mobile payments (NFC, QR, in-app)", 2, "pp_meth"),
    ("pp_meth_crypto", "Cryptocurrency payments", 2, "pp_meth"),
    ("pp_meth_cb", "Central bank digital currency (CBDC)", 2, "pp_meth"),
    ("pp_infra_gateway", "Payment gateway", 2, "pp_infra"),
    ("pp_infra_proc", "Payment processor", 2, "pp_infra"),
    ("pp_infra_acquir", "Acquiring bank", 2, "pp_infra"),
    ("pp_infra_scheme", "Card network/scheme (Visa, MC, Amex)", 2, "pp_infra"),
    ("pp_infra_iso", "ISO/agent (independent sales org)", 2, "pp_infra"),
    ("pp_infra_pf", "Payment facilitator (PayFac)", 2, "pp_infra"),
    ("pp_settle_rt", "Real-time gross settlement (RTGS)", 2, "pp_settle"),
    ("pp_settle_net", "Net settlement (batch)", 2, "pp_settle"),
    ("pp_settle_dvp", "Delivery vs payment (DvP)", 2, "pp_settle"),
    ("pp_settle_t2", "T+2 securities settlement", 2, "pp_settle"),
    ("pp_settle_t1", "T+1 settlement", 2, "pp_settle"),
    ("pp_settle_t0", "T+0 same-day settlement", 2, "pp_settle"),
]


async def ingest_domain_payment_proc(conn) -> int:
    """Insert or update Payment Processing Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_payment_proc"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_payment_proc", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_payment_proc",
    )
    return count
