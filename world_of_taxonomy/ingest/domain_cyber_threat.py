"""Cyber Threat domain taxonomy ingester.

Organizes cyber threat types aligned with NAICS 5415
(Computer systems design and related services).

Code prefix: dct_
Categories: Malware, Social Engineering, Network Attacks, Application Attacks, Insider Threats.

Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
CYBER_THREAT_NODES: list[tuple[str, str, int, Optional[str]]] = [
    # -- Malware --
    ("dct_malware",           "Malware Threats",                                       1, None),
    ("dct_malware_ransom",    "Ransomware (crypto-locking, double extortion, RaaS)",   2, "dct_malware"),
    ("dct_malware_trojan",    "Trojan and Remote Access Trojan (RAT, banking trojan)", 2, "dct_malware"),
    ("dct_malware_worm",      "Worms and Self-Propagating Malware (network, email)",   2, "dct_malware"),
    ("dct_malware_rootkit",   "Rootkits and Bootkits (kernel, firmware, UEFI)",        2, "dct_malware"),
    ("dct_malware_fileless",  "Fileless Malware (memory-resident, LOLBins, scripts)",  2, "dct_malware"),

    # -- Social Engineering --
    ("dct_social",            "Social Engineering Threats",                             1, None),
    ("dct_social_phishing",   "Phishing (email, spear-phishing, whaling, clone)",      2, "dct_social"),
    ("dct_social_vishing",    "Voice Phishing and Smishing (phone, SMS, deepfake)",    2, "dct_social"),
    ("dct_social_pretext",    "Pretexting and Impersonation (executive, vendor fraud)", 2, "dct_social"),
    ("dct_social_bec",        "Business Email Compromise (account takeover, redirect)", 2, "dct_social"),

    # -- Network Attacks --
    ("dct_network",           "Network-Based Attacks",                                 1, None),
    ("dct_network_ddos",      "DDoS Attacks (volumetric, protocol, application layer)", 2, "dct_network"),
    ("dct_network_mitm",      "Man-in-the-Middle Attacks (ARP spoofing, SSL stripping)", 2, "dct_network"),
    ("dct_network_dns",       "DNS Attacks (poisoning, tunneling, hijacking, typosquat)", 2, "dct_network"),
    ("dct_network_lateral",   "Lateral Movement (pass-the-hash, Kerberoasting, RDP)",  2, "dct_network"),

    # -- Application Attacks --
    ("dct_app",               "Application-Layer Attacks",                              1, None),
    ("dct_app_injection",     "Injection Attacks (SQL, command, LDAP, XPath)",          2, "dct_app"),
    ("dct_app_xss",           "Cross-Site Scripting (reflected, stored, DOM-based)",    2, "dct_app"),
    ("dct_app_api",           "API Abuse and Exploitation (BOLA, BFLA, rate bypass)",   2, "dct_app"),
    ("dct_app_supply",        "Supply Chain Attacks (dependency confusion, typosquat)", 2, "dct_app"),

    # -- Insider Threats --
    ("dct_insider",           "Insider Threats",                                        1, None),
    ("dct_insider_malicious", "Malicious Insider (data theft, sabotage, espionage)",    2, "dct_insider"),
    ("dct_insider_negligent", "Negligent Insider (misconfiguration, accidental leak)",  2, "dct_insider"),
    ("dct_insider_compromised", "Compromised Insider (credential theft, account takeover)", 2, "dct_insider"),
    ("dct_insider_privilege", "Privilege Escalation (unauthorized access, role abuse)", 2, "dct_insider"),
]

_DOMAIN_ROW = (
    "domain_cyber_threat",
    "Cyber Threat Types",
    "Malware, social engineering, network attacks, application attacks, "
    "and insider threat taxonomy for cybersecurity",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefix: 5415 (Computer systems design and related services)
_NAICS_PREFIXES = ["5415"]


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific cyber threat types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_cyber_threat(conn) -> int:
    """Ingest Cyber Threat domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_cyber_threat'), and links NAICS 5415 nodes
    via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_cyber_threat",
        "Cyber Threat Types",
        "Malware, social engineering, network attacks, application attacks, "
        "and insider threat taxonomy for cybersecurity",
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

    parent_codes = {parent for _, _, _, parent in CYBER_THREAT_NODES if parent is not None}

    rows = [
        (
            "domain_cyber_threat",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in CYBER_THREAT_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(CYBER_THREAT_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_cyber_threat'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_cyber_threat'",
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
            [("naics_2022", code, "domain_cyber_threat", "primary") for code in naics_codes],
        )

    return count
