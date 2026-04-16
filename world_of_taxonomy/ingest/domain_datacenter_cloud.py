"""Data Center and Cloud Infrastructure domain taxonomy ingester.

Data center and cloud taxonomy organizes infrastructure and service models:
  Hyperscale     (ddc_hyper*)   - public cloud DC, mega-scale, custom silicon
  Colocation     (ddc_colo*)    - retail colo, wholesale, carrier-neutral, meet-me
  Edge           (ddc_edge*)    - micro data center, edge node, CDN PoP
  Cloud IaaS     (ddc_iaas*)    - compute, storage, networking, bare metal
  Cloud SaaS     (ddc_saas*)    - horizontal, vertical, micro-SaaS, API platform

Source: Uptime Institute tier standards and NIST cloud definitions.
Public domain. Hand-coded. Open.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
DATACENTER_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Hyperscale Data Centers category --
    ("ddc_hyper",              "Hyperscale Data Centers",                               1, None),
    ("ddc_hyper_public",       "Public Cloud Hyperscale (AWS, Azure, GCP regions)",    2, "ddc_hyper"),
    ("ddc_hyper_enterprise",   "Enterprise Hyperscale (private mega-campus)",          2, "ddc_hyper"),
    ("ddc_hyper_sovereign",    "Sovereign Cloud Data Center (in-country, compliant)",  2, "ddc_hyper"),
    ("ddc_hyper_subsea",       "Subsea and Modular Data Center (offshore, portable)",  2, "ddc_hyper"),

    # -- Colocation category --
    ("ddc_colo",               "Colocation Facilities",                                 1, None),
    ("ddc_colo_retail",        "Retail Colocation (single rack, cage, cabinet)",        2, "ddc_colo"),
    ("ddc_colo_wholesale",     "Wholesale Colocation (dedicated hall, MW-scale)",       2, "ddc_colo"),
    ("ddc_colo_carrier",       "Carrier-Neutral Meet-Me Room and IX (interconnect)",   2, "ddc_colo"),
    ("ddc_colo_managed",       "Managed Hosting and Dedicated Server",                 2, "ddc_colo"),

    # -- Edge Data Centers category --
    ("ddc_edge",               "Edge Data Centers",                                     1, None),
    ("ddc_edge_micro",         "Micro Data Center (prefab, single-rack, ruggedized)",  2, "ddc_edge"),
    ("ddc_edge_telco",         "Telco Edge (cell tower, central office, MEC)",         2, "ddc_edge"),
    ("ddc_edge_cdn",           "CDN Point of Presence (content cache, streaming)",     2, "ddc_edge"),
    ("ddc_edge_iot",           "IoT Gateway Edge (industrial, smart city, vehicle)",   2, "ddc_edge"),

    # -- Cloud IaaS category --
    ("ddc_iaas",               "Cloud Infrastructure as a Service (IaaS)",              1, None),
    ("ddc_iaas_compute",       "Compute Service (VM, container, serverless, GPU)",     2, "ddc_iaas"),
    ("ddc_iaas_storage",       "Object and Block Storage (S3, EBS, managed disk)",     2, "ddc_iaas"),
    ("ddc_iaas_network",       "Virtual Network and Load Balancer (VPC, CDN, DNS)",    2, "ddc_iaas"),
    ("ddc_iaas_bare",          "Bare Metal and HPC (dedicated host, FPGA, Infiniband)",2, "ddc_iaas"),
    ("ddc_iaas_db",            "Managed Database Service (RDS, Cosmos, Cloud SQL)",    2, "ddc_iaas"),

    # -- Cloud SaaS category --
    ("ddc_saas",               "Cloud Software as a Service (SaaS)",                    1, None),
    ("ddc_saas_horizontal",    "Horizontal SaaS (CRM, ERP, HR, collaboration)",        2, "ddc_saas"),
    ("ddc_saas_vertical",      "Vertical SaaS (industry-specific - health, legal)",    2, "ddc_saas"),
    ("ddc_saas_micro",         "Micro-SaaS and Single-Feature Tool",                   2, "ddc_saas"),
    ("ddc_saas_api",           "API Platform and Developer Service (Twilio, Stripe)",  2, "ddc_saas"),
]

_DOMAIN_ROW = (
    "domain_datacenter_cloud",
    "Data Center and Cloud Infrastructure Types",
    "Hyperscale, colocation, edge, IaaS and SaaS cloud infrastructure taxonomy",
    "WorldOfTaxonomy",
    None,
)

_NAICS_PREFIXES = ["5182", "5415"]


def _determine_level(code: str) -> int:
    """Return level: 1 for top categories, 2 for specific datacenter types."""
    parts = code.split("_")
    if len(parts) == 2:
        return 1
    return 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_datacenter_cloud(conn) -> int:
    """Ingest Data Center and Cloud Infrastructure domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_datacenter_cloud'), and links NAICS 5182/5415 nodes
    via node_taxonomy_link.

    Returns total datacenter/cloud node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_datacenter_cloud",
        "Data Center and Cloud Infrastructure Types",
        "Hyperscale, colocation, edge, IaaS and SaaS cloud infrastructure taxonomy",
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

    parent_codes = {parent for _, _, _, parent in DATACENTER_NODES if parent is not None}

    rows = [
        (
            "domain_datacenter_cloud",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in DATACENTER_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(DATACENTER_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_datacenter_cloud'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_datacenter_cloud'",
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
            [("naics_2022", code, "domain_datacenter_cloud", "primary") for code in naics_codes],
        )

    return count
