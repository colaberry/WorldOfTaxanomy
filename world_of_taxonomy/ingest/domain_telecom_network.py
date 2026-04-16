"""Telecom Network domain taxonomy ingester.

Organizes telecommunications network infrastructure types aligned with
NAICS 5171 (Wired telecommunications carriers).

Code prefix: dtn_
Categories: Access Network, Core Network, Transport, Edge Computing, Spectrum.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
TELECOM_NETWORK_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Access Network --
    ("dtn_access",           "Access Network Infrastructure",                          1, None),
    ("dtn_access_ran",       "Radio Access Network (macro cell, small cell, DAS)",     2, "dtn_access"),
    ("dtn_access_pon",       "Passive Optical Network (GPON, XGS-PON, splitters)",     2, "dtn_access"),
    ("dtn_access_hfc",       "Hybrid Fiber Coax (CMTS, DOCSIS, node splitting)",       2, "dtn_access"),
    ("dtn_access_fttx",      "FTTx Variants (FTTH, FTTB, FTTC, last-mile fiber)",     2, "dtn_access"),

    # -- Core Network --
    ("dtn_core",             "Core Network Infrastructure",                            1, None),
    ("dtn_core_5gc",         "5G Core Network (AMF, SMF, UPF, service-based arch)",    2, "dtn_core"),
    ("dtn_core_epc",         "Evolved Packet Core (MME, SGW, PGW, 4G LTE core)",      2, "dtn_core"),
    ("dtn_core_ims",         "IP Multimedia Subsystem (VoLTE, RCS, IMS core)",         2, "dtn_core"),
    ("dtn_core_sdn",         "SDN and NFV Infrastructure (controllers, VNF, MANO)",    2, "dtn_core"),
    ("dtn_core_signal",      "Signaling and Control Plane (Diameter, SBI, SIP)",       2, "dtn_core"),

    # -- Transport --
    ("dtn_transport",        "Transport Network Infrastructure",                       1, None),
    ("dtn_transport_dwdm",   "DWDM Optical Transport (long-haul, metro ring, ROADM)", 2, "dtn_transport"),
    ("dtn_transport_otn",    "OTN and SONET/SDH (optical switching, grooming)",        2, "dtn_transport"),
    ("dtn_transport_mpls",   "MPLS Transport (label switching, traffic engineering)",   2, "dtn_transport"),
    ("dtn_transport_sub",    "Submarine Cable Systems (transoceanic, landing stations)", 2, "dtn_transport"),

    # -- Edge Computing --
    ("dtn_edge",             "Edge Computing Infrastructure",                          1, None),
    ("dtn_edge_mec",         "Multi-Access Edge Computing (MEC servers, API gateway)", 2, "dtn_edge"),
    ("dtn_edge_central",     "Central Office Re-architected (CORD, edge data center)", 2, "dtn_edge"),
    ("dtn_edge_cdn",         "Edge CDN and Caching (content distribution, streaming)", 2, "dtn_edge"),
    ("dtn_edge_iot",         "IoT Edge Gateway (protocol translation, local compute)", 2, "dtn_edge"),

    # -- Spectrum --
    ("dtn_spectrum",         "Spectrum and Radio Frequency Management",                1, None),
    ("dtn_spectrum_licensed", "Licensed Spectrum (sub-6 GHz, mmWave, C-band)",         2, "dtn_spectrum"),
    ("dtn_spectrum_shared",  "Shared Spectrum (CBRS, LSA, dynamic access)",            2, "dtn_spectrum"),
    ("dtn_spectrum_unlicensed", "Unlicensed Spectrum (Wi-Fi 6E, 7, ISM bands)",        2, "dtn_spectrum"),
]

_DOMAIN_ROW = (
    "domain_telecom_network",
    "Telecom Network Types",
    "Access network, core network, transport, edge computing, "
    "and spectrum management infrastructure taxonomy",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefix: 5171 (Wired telecommunications carriers)
_NAICS_PREFIXES = ["5171"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific telecom network types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_telecom_network(conn) -> int:
    """Ingest Telecom Network domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_telecom_network'), and links NAICS 5171 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_telecom_network",
        "Telecom Network Types",
        "Access network, core network, transport, edge computing, "
        "and spectrum management infrastructure taxonomy",
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

    parent_codes = {parent for _, _, _, parent in TELECOM_NETWORK_NODES if parent is not None}

    rows = [
        (
            "domain_telecom_network",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in TELECOM_NETWORK_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(TELECOM_NETWORK_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_telecom_network'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_telecom_network'",
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
            [("naics_2022", code, "domain_telecom_network", "primary") for code in naics_codes],
        )

    return count
