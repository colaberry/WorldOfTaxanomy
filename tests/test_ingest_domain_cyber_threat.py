"""Tests for Cyber Threat domain taxonomy ingester.

Cyber Threat taxonomy organizes cybersecurity threat types:
  Malware            (dct_malware*)  - ransomware, trojan, worm, rootkit, fileless
  Social Engineering (dct_social*)   - phishing, vishing, pretexting, BEC
  Network Attacks    (dct_network*)  - DDoS, MITM, DNS, lateral movement
  Application Attacks(dct_app*)      - injection, XSS, API abuse, supply chain
  Insider Threats    (dct_insider*)  - malicious, negligent, compromised, privilege

Source: NAICS 5415 cybersecurity industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_cyber_threat import (
    CYBER_THREAT_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_cyber_threat,
)


class TestDetermineLevel:
    def test_malware_category_is_level_1(self):
        assert _determine_level("dct_malware") == 1

    def test_malware_type_is_level_2(self):
        assert _determine_level("dct_malware_ransom") == 2

    def test_social_category_is_level_1(self):
        assert _determine_level("dct_social") == 1

    def test_network_type_is_level_2(self):
        assert _determine_level("dct_network_ddos") == 2

    def test_app_category_is_level_1(self):
        assert _determine_level("dct_app") == 1

    def test_insider_type_is_level_2(self):
        assert _determine_level("dct_insider_malicious") == 2


class TestDetermineParent:
    def test_malware_category_has_no_parent(self):
        assert _determine_parent("dct_malware") is None

    def test_malware_ransom_parent_is_malware(self):
        assert _determine_parent("dct_malware_ransom") == "dct_malware"

    def test_network_ddos_parent_is_network(self):
        assert _determine_parent("dct_network_ddos") == "dct_network"

    def test_insider_malicious_parent_is_insider(self):
        assert _determine_parent("dct_insider_malicious") == "dct_insider"


class TestCyberThreatNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(CYBER_THREAT_NODES) > 0

    def test_has_malware_category(self):
        codes = [n[0] for n in CYBER_THREAT_NODES]
        assert "dct_malware" in codes

    def test_has_social_category(self):
        codes = [n[0] for n in CYBER_THREAT_NODES]
        assert "dct_social" in codes

    def test_has_network_category(self):
        codes = [n[0] for n in CYBER_THREAT_NODES]
        assert "dct_network" in codes

    def test_has_app_category(self):
        codes = [n[0] for n in CYBER_THREAT_NODES]
        assert "dct_app" in codes

    def test_has_insider_category(self):
        codes = [n[0] for n in CYBER_THREAT_NODES]
        assert "dct_insider" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in CYBER_THREAT_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in CYBER_THREAT_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in CYBER_THREAT_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in CYBER_THREAT_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(CYBER_THREAT_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in CYBER_THREAT_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_cyber_threat_module_importable():
    assert callable(ingest_domain_cyber_threat)
    assert isinstance(CYBER_THREAT_NODES, list)


def test_ingest_domain_cyber_threat(db_pool):
    """Integration test: cyber threat taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_cyber_threat(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_cyber_threat'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_cyber_threat_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_cyber_threat(conn)
            count2 = await ingest_domain_cyber_threat(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
