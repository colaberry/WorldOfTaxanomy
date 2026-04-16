"""Ingest Municipal Bond Classification Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_muni_bond",
    "Municipal Bond Types",
    "Municipal Bond Classification Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("mb_type", "Bond Types", 1, None),
    ("mb_sec", "Security Types", 1, None),
    ("mb_tax", "Tax Status", 1, None),
    ("mb_type_go", "General obligation (GO) bonds", 2, "mb_type"),
    ("mb_type_rev", "Revenue bonds", 2, "mb_type"),
    ("mb_type_assess", "Special assessment bonds", 2, "mb_type"),
    ("mb_type_tif", "Tax increment financing (TIF)", 2, "mb_type"),
    ("mb_type_conduit", "Conduit bonds (industrial revenue)", 2, "mb_type"),
    ("mb_sec_insured", "Insured municipal bonds", 2, "mb_sec"),
    ("mb_sec_preref", "Pre-refunded bonds", 2, "mb_sec"),
    ("mb_sec_escrowed", "Escrowed-to-maturity bonds", 2, "mb_sec"),
    ("mb_tax_exempt", "Tax-exempt municipal bonds", 2, "mb_tax"),
    ("mb_tax_taxable", "Taxable municipal bonds", 2, "mb_tax"),
    ("mb_tax_amt", "Alternative minimum tax (AMT) bonds", 2, "mb_tax"),
    ("mb_tax_bab", "Build America Bonds (BABs)", 2, "mb_tax"),
]


async def ingest_domain_muni_bond(conn) -> int:
    """Insert or update Municipal Bond Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_muni_bond"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_muni_bond", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_muni_bond",
    )
    return count
