"""Ingest Residential Mortgage Product Types."""

from __future__ import annotations

_SYSTEM_ROW = (
    "domain_mortgage_type",
    "Mortgage Types",
    "Residential Mortgage Product Types",
    "WorldOfTaxonomy",
    "Global",
    "WorldOfTaxonomy",
)
_SOURCE_URL = ""
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("mt_conv", "Conventional Mortgages", 1, None),
    ("mt_govt", "Government-Backed Mortgages", 1, None),
    ("mt_spec", "Specialty Mortgages", 1, None),
    ("mt_conv_fixed", "Fixed-rate mortgage (15yr, 20yr, 30yr)", 2, "mt_conv"),
    ("mt_conv_arm", "Adjustable-rate mortgage (ARM)", 2, "mt_conv"),
    ("mt_conv_conf", "Conforming loan (GSE eligible)", 2, "mt_conv"),
    ("mt_conv_jumbo", "Jumbo / non-conforming loan", 2, "mt_conv"),
    ("mt_govt_fha", "FHA loan (Federal Housing Administration)", 2, "mt_govt"),
    ("mt_govt_va", "VA loan (Veterans Affairs)", 2, "mt_govt"),
    ("mt_govt_usda", "USDA Rural Development loan", 2, "mt_govt"),
    ("mt_spec_io", "Interest-only mortgage", 2, "mt_spec"),
    ("mt_spec_rev", "Reverse mortgage (HECM)", 2, "mt_spec"),
    ("mt_spec_constr", "Construction-to-permanent loan", 2, "mt_spec"),
    ("mt_spec_bridge", "Bridge loan", 2, "mt_spec"),
    ("mt_spec_heloc", "Home equity line of credit (HELOC)", 2, "mt_spec"),
    ("mt_spec_refi", "Refinance (rate-term, cash-out)", 2, "mt_spec"),
]


async def ingest_domain_mortgage_type(conn) -> int:
    """Insert or update Mortgage Types system and its nodes. Returns node count."""
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
        "DELETE FROM classification_node WHERE system_id = $1", "domain_mortgage_type"
    )
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1, $2, $3, $4, $5)""",
            "domain_mortgage_type", code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, "domain_mortgage_type",
    )
    return count
