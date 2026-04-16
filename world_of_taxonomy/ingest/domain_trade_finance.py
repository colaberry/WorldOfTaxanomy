"""Ingest Trade Finance Instrument and Facility Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_trade_finance",
    "Trade Finance Instrument Types",
    "Trade Finance Instrument and Facility Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("tf_trad", "Traditional Trade Finance", 1, None),
    ("tf_scf", "Supply Chain Finance", 1, None),
    ("tf_risk", "Trade Risk Mitigation", 1, None),
    ("tf_trad_lc", "Letter of credit (L/C)", 2, "tf_trad"),
    ("tf_trad_da", "Documentary collection (D/A, D/P)", 2, "tf_trad"),
    ("tf_trad_bg", "Bank guarantee", 2, "tf_trad"),
    ("tf_trad_sblc", "Standby letter of credit (SBLC)", 2, "tf_trad"),
    ("tf_trad_accept", "Bankers acceptance", 2, "tf_trad"),
    ("tf_trad_bill", "Bill of exchange", 2, "tf_trad"),
    ("tf_scf_rf", "Receivables financing (factoring)", 2, "tf_scf"),
    ("tf_scf_pf", "Payables finance (reverse factoring)", 2, "tf_scf"),
    ("tf_scf_loan", "Loan against receivables", 2, "tf_scf"),
    ("tf_scf_dist", "Distributor finance", 2, "tf_scf"),
    ("tf_scf_preex", "Pre-export finance", 2, "tf_scf"),
    ("tf_risk_eci", "Export credit insurance", 2, "tf_risk"),
    ("tf_risk_pol", "Political risk insurance", 2, "tf_risk"),
    ("tf_risk_eca", "Export credit agency guarantee", 2, "tf_risk"),
    ("tf_risk_forf", "Forfaiting", 2, "tf_risk"),
]


async def ingest_domain_trade_finance(conn) -> int:
    """Insert or update Trade Finance Instrument Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_trade_finance"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_trade_finance", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_trade_finance",
    )
    return count
