"""Quantum Computing Application Domain Types domain taxonomy ingester.

Classifies quantum technology use cases by application domain.
Orthogonal to quantum hardware/platform type (domain_quantum).
Based on NIST Post-Quantum Cryptography standards, McKinsey/BCG quantum survey
application taxonomy, and NATO quantum technology roadmap.
Used by quantum investment analysts, corporate innovation teams, and policy makers.

Code prefix: dqcapp_
System ID: domain_quantum_application
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
QUANTUM_APPLICATION_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dqcapp_crypto", "Quantum Cryptography and Communications", 1, None),
    ("dqcapp_crypto_qkd", "Quantum Key Distribution (QKD, BB84, E91 protocols)", 2, "dqcapp_crypto"),
    ("dqcapp_crypto_pqc", "Post-Quantum Cryptography (NIST PQC standards: Kyber, Dilithium)", 2, "dqcapp_crypto"),
    ("dqcapp_crypto_qrng", "Quantum Random Number Generation (QRNG, hardware entropy)", 2, "dqcapp_crypto"),
    ("dqcapp_simulate", "Quantum Simulation", 1, None),
    ("dqcapp_simulate_chem", "Molecular and quantum chemistry simulation (drug discovery, catalysis)", 2, "dqcapp_simulate"),
    ("dqcapp_simulate_mat", "Materials science simulation (superconductors, battery chemistry)", 2, "dqcapp_simulate"),
    ("dqcapp_simulate_phys", "High-energy physics and condensed matter simulation", 2, "dqcapp_simulate"),
    ("dqcapp_optimize", "Quantum Optimization", 1, None),
    ("dqcapp_optimize_log", "Logistics and supply chain optimization (QAOA, VQE)", 2, "dqcapp_optimize"),
    ("dqcapp_optimize_fin", "Financial portfolio optimization and risk calculation", 2, "dqcapp_optimize"),
    ("dqcapp_optimize_ml", "Quantum machine learning (QML, quantum kernel methods)", 2, "dqcapp_optimize"),
    ("dqcapp_sensing", "Quantum Sensing and Metrology", 1, None),
    ("dqcapp_sensing_mag", "Quantum magnetometers and gravimeters (medical, navigation)", 2, "dqcapp_sensing"),
    ("dqcapp_sensing_clock", "Optical atomic clocks and quantum timing (GPS, telecom)", 2, "dqcapp_sensing"),
    ("dqcapp_sensing_imag", "Quantum imaging (ghost imaging, enhanced resolution)", 2, "dqcapp_sensing"),
]

_DOMAIN_ROW = (
    "domain_quantum_application",
    "Quantum Computing Application Domain Types",
    "Quantum technology application domain classification: cryptography, simulation, optimization, sensing, ML",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['5415', '5417', '3679']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_quantum_application(conn) -> int:
    """Ingest Quantum Computing Application Domain Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_quantum_application'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_quantum_application",
        "Quantum Computing Application Domain Types",
        "Quantum technology application domain classification: cryptography, simulation, optimization, sensing, ML",
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

    parent_codes = {parent for _, _, _, parent in QUANTUM_APPLICATION_NODES if parent is not None}

    rows = [
        (
            "domain_quantum_application",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in QUANTUM_APPLICATION_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(QUANTUM_APPLICATION_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_quantum_application'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_quantum_application'",
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
            [("naics_2022", code, "domain_quantum_application", "primary") for code in naics_codes],
        )

    return count
