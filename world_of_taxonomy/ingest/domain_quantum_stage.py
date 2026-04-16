"""Quantum Technology Commercialization Stage Types domain taxonomy ingester.

Classifies quantum hardware and software platforms by commercialization maturity.
Orthogonal to quantum type and application domain.
Based on IBM Quantum Roadmap, IQM/IonQ/Quantinuum disclosed roadmaps,
and McKinsey quantum readiness framework.
Used by corporate quantum teams, investors, and government programme offices
assessing investment horizon and technology risk.

Code prefix: dqcstg_
System ID: domain_quantum_stage
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
QUANTUM_STAGE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dqcstg_research", "Research and Academic Stage", 1, None),
    ("dqcstg_research_lab", "University/national lab prototype (sub-100 physical qubits)", 2, "dqcstg_research"),
    ("dqcstg_research_noisy", "NISQ era (noisy intermediate-scale quantum, 100-1000 qubits)", 2, "dqcstg_research"),
    ("dqcstg_earlybiz", "Early Commercial and Cloud Access Stage", 1, None),
    ("dqcstg_earlybiz_cloud", "Cloud-accessible quantum computers (IBM Q Network, IonQ, Rigetti)", 2, "dqcstg_earlybiz"),
    ("dqcstg_earlybiz_hybrid", "Quantum-classical hybrid algorithms for near-term advantage", 2, "dqcstg_earlybiz"),
    ("dqcstg_faulttol", "Fault-Tolerant Quantum Computing Stage", 1, None),
    ("dqcstg_faulttol_early", "Early fault-tolerant (logical qubits, surface code, limited error-correction)", 2, "dqcstg_faulttol"),
    ("dqcstg_faulttol_full", "Full fault-tolerant (millions of physical qubits, broad QEA)", 2, "dqcstg_faulttol"),
    ("dqcstg_software", "Quantum Software and Toolchain Stage", 1, None),
    ("dqcstg_software_sdk", "Quantum SDK and programming framework (Qiskit, Cirq, PennyLane)", 2, "dqcstg_software"),
    ("dqcstg_software_os", "Quantum OS and control software stack", 2, "dqcstg_software"),
    ("dqcstg_software_app", "Quantum application software for specific use cases", 2, "dqcstg_software"),
]

_DOMAIN_ROW = (
    "domain_quantum_stage",
    "Quantum Technology Commercialization Stage Types",
    "Quantum technology maturity and commercialization stage classification for hardware and software platforms",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['5415', '5417', '5231']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_quantum_stage(conn) -> int:
    """Ingest Quantum Technology Commercialization Stage Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_quantum_stage'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_quantum_stage",
        "Quantum Technology Commercialization Stage Types",
        "Quantum technology maturity and commercialization stage classification for hardware and software platforms",
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

    parent_codes = {parent for _, _, _, parent in QUANTUM_STAGE_NODES if parent is not None}

    rows = [
        (
            "domain_quantum_stage",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in QUANTUM_STAGE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(QUANTUM_STAGE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_quantum_stage'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_quantum_stage'",
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
            [("naics_2022", code, "domain_quantum_stage", "primary") for code in naics_codes],
        )

    return count
