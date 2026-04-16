"""ACM CCS 2012 (ACM Computing Classification System) ingester.

The ACM Computing Classification System organizes computer science
into 12 top-level categories and ~56 subject areas.

Source: Association for Computing Machinery (acm.org)
License: Freely available for non-commercial use
"""
from __future__ import annotations

ACM_TOPLEVEL: list[tuple[str, str]] = [
    ("CCS-GEN", "General and Reference"),
    ("CCS-HARD", "Hardware"),
    ("CCS-COMP", "Computer Systems Organization"),
    ("CCS-NET", "Networks"),
    ("CCS-SW", "Software and Its Engineering"),
    ("CCS-THEORY", "Theory of Computation"),
    ("CCS-MATH", "Mathematics of Computing"),
    ("CCS-INFO", "Information Systems"),
    ("CCS-SEC", "Security and Privacy"),
    ("CCS-HCI", "Human-Centered Computing"),
    ("CCS-APP", "Applied Computing"),
    ("CCS-SOCIAL", "Social and Professional Topics"),
]

ACM_SUBJECTS: list[tuple[str, str, str]] = [
    ("CCS-GEN-001", "Cross-computing tools and techniques", "CCS-GEN"),
    ("CCS-HARD-001", "Printed circuit boards and hardware components", "CCS-HARD"),
    ("CCS-HARD-002", "Integrated circuits and chip design", "CCS-HARD"),
    ("CCS-HARD-003", "Power and energy management (hardware)", "CCS-HARD"),
    ("CCS-HARD-004", "Very large scale integration design", "CCS-HARD"),
    ("CCS-HARD-005", "Emerging hardware technologies", "CCS-HARD"),
    ("CCS-COMP-001", "Architectures - processor and memory architectures", "CCS-COMP"),
    ("CCS-COMP-002", "Embedded and cyber-physical systems", "CCS-COMP"),
    ("CCS-COMP-003", "Real-time systems", "CCS-COMP"),
    ("CCS-COMP-004", "Dependable and fault-tolerant systems and networks", "CCS-COMP"),
    ("CCS-NET-001", "Network architectures and protocols", "CCS-NET"),
    ("CCS-NET-002", "Network performance evaluation", "CCS-NET"),
    ("CCS-NET-003", "Wireless access networks", "CCS-NET"),
    ("CCS-NET-004", "Network services (SDN, CDN, overlay networks)", "CCS-NET"),
    ("CCS-SW-001", "Software creation and management", "CCS-SW"),
    ("CCS-SW-002", "Software notations and tools", "CCS-SW"),
    ("CCS-SW-003", "Software organization and properties", "CCS-SW"),
    ("CCS-SW-004", "Software system structures", "CCS-SW"),
    ("CCS-THEORY-001", "Formal languages and automata theory", "CCS-THEORY"),
    ("CCS-THEORY-002", "Computational complexity and cryptography", "CCS-THEORY"),
    ("CCS-THEORY-003", "Logic and verification", "CCS-THEORY"),
    ("CCS-THEORY-004", "Design and analysis of algorithms", "CCS-THEORY"),
    ("CCS-THEORY-005", "Randomness, geometry and discrete mathematics", "CCS-THEORY"),
    ("CCS-MATH-001", "Continuous mathematics", "CCS-MATH"),
    ("CCS-MATH-002", "Discrete mathematics", "CCS-MATH"),
    ("CCS-MATH-003", "Mathematical software", "CCS-MATH"),
    ("CCS-MATH-004", "Probability and statistics", "CCS-MATH"),
    ("CCS-INFO-001", "Data management systems", "CCS-INFO"),
    ("CCS-INFO-002", "Information storage systems", "CCS-INFO"),
    ("CCS-INFO-003", "Information retrieval", "CCS-INFO"),
    ("CCS-INFO-004", "World Wide Web", "CCS-INFO"),
    ("CCS-INFO-005", "Data mining", "CCS-INFO"),
    ("CCS-SEC-001", "Cryptography", "CCS-SEC"),
    ("CCS-SEC-002", "Formal methods and theory of security", "CCS-SEC"),
    ("CCS-SEC-003", "Security services", "CCS-SEC"),
    ("CCS-SEC-004", "Intrusion and anomaly detection and malware mitigation", "CCS-SEC"),
    ("CCS-SEC-005", "Security in hardware", "CCS-SEC"),
    ("CCS-SEC-006", "Systems security", "CCS-SEC"),
    ("CCS-SEC-007", "Network security", "CCS-SEC"),
    ("CCS-SEC-008", "Privacy protections and mechanisms", "CCS-SEC"),
    ("CCS-HCI-001", "Human computer interaction (HCI)", "CCS-HCI"),
    ("CCS-HCI-002", "Interaction design", "CCS-HCI"),
    ("CCS-HCI-003", "Collaborative and social computing", "CCS-HCI"),
    ("CCS-HCI-004", "Ubiquitous and mobile computing", "CCS-HCI"),
    ("CCS-HCI-005", "Visualization", "CCS-HCI"),
    ("CCS-HCI-006", "Accessibility", "CCS-HCI"),
    ("CCS-APP-001", "Life and medical sciences applications", "CCS-APP"),
    ("CCS-APP-002", "Physical sciences and engineering applications", "CCS-APP"),
    ("CCS-APP-003", "Social and behavioral sciences applications", "CCS-APP"),
    ("CCS-APP-004", "Law, social and behavioral sciences", "CCS-APP"),
    ("CCS-APP-005", "Arts and humanities applications", "CCS-APP"),
    ("CCS-APP-006", "Computers in other domains", "CCS-APP"),
    ("CCS-SOCIAL-001", "Professional topics (computing profession, computing education)", "CCS-SOCIAL"),
    ("CCS-SOCIAL-002", "Computing and society (impact, ethics, governance)", "CCS-SOCIAL"),
    ("CCS-SOCIAL-003", "User characteristics and accessibility", "CCS-SOCIAL"),
]


async def ingest_acm_ccs(conn) -> int:
    """Ingest ACM CCS 2012 into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "acm_ccs",
        "ACM CCS 2012",
        "ACM Computing Classification System 2012",
        "2012",
        "Global",
        "Association for Computing Machinery (ACM)",
    )

    count = 0
    for seq, (code, title) in enumerate(ACM_TOPLEVEL, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "acm_ccs", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(ACM_SUBJECTS, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "acm_ccs", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'acm_ccs'",
        count,
    )

    return count
