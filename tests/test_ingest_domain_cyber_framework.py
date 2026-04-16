"""Tests for Cyber Framework domain taxonomy ingester.

Cyber Framework taxonomy organizes cybersecurity framework types:
  Risk Frameworks     (dcf_risk*)     - NIST CSF, ISO 27001, COBIT, FAIR, CIS
  Compliance          (dcf_comply*)   - PCI DSS, SOX, HIPAA, FedRAMP, CMMC
  Security Controls   (dcf_controls*) - NIST 800-53, MITRE ATT&CK, OWASP, Zero Trust
  Privacy Frameworks  (dcf_privacy*)  - GDPR, CCPA, NIST Privacy, ISO 27701

Source: NAICS 5415 cybersecurity industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_cyber_framework import (
    CYBER_FRAMEWORK_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_cyber_framework,
)


class TestDetermineLevel:
    def test_risk_category_is_level_1(self):
        assert _determine_level("dcf_risk") == 1

    def test_risk_type_is_level_2(self):
        assert _determine_level("dcf_risk_nist") == 2

    def test_comply_category_is_level_1(self):
        assert _determine_level("dcf_comply") == 1

    def test_controls_type_is_level_2(self):
        assert _determine_level("dcf_controls_mitre") == 2

    def test_privacy_category_is_level_1(self):
        assert _determine_level("dcf_privacy") == 1

    def test_privacy_type_is_level_2(self):
        assert _determine_level("dcf_privacy_gdpr") == 2


class TestDetermineParent:
    def test_risk_category_has_no_parent(self):
        assert _determine_parent("dcf_risk") is None

    def test_risk_nist_parent_is_risk(self):
        assert _determine_parent("dcf_risk_nist") == "dcf_risk"

    def test_comply_pci_parent_is_comply(self):
        assert _determine_parent("dcf_comply_pci") == "dcf_comply"

    def test_privacy_gdpr_parent_is_privacy(self):
        assert _determine_parent("dcf_privacy_gdpr") == "dcf_privacy"


class TestCyberFrameworkNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(CYBER_FRAMEWORK_NODES) > 0

    def test_has_risk_category(self):
        codes = [n[0] for n in CYBER_FRAMEWORK_NODES]
        assert "dcf_risk" in codes

    def test_has_compliance_category(self):
        codes = [n[0] for n in CYBER_FRAMEWORK_NODES]
        assert "dcf_comply" in codes

    def test_has_controls_category(self):
        codes = [n[0] for n in CYBER_FRAMEWORK_NODES]
        assert "dcf_controls" in codes

    def test_has_privacy_category(self):
        codes = [n[0] for n in CYBER_FRAMEWORK_NODES]
        assert "dcf_privacy" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in CYBER_FRAMEWORK_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in CYBER_FRAMEWORK_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in CYBER_FRAMEWORK_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in CYBER_FRAMEWORK_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(CYBER_FRAMEWORK_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in CYBER_FRAMEWORK_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_cyber_framework_module_importable():
    assert callable(ingest_domain_cyber_framework)
    assert isinstance(CYBER_FRAMEWORK_NODES, list)


def test_ingest_domain_cyber_framework(db_pool):
    """Integration test: cyber framework taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_cyber_framework(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_cyber_framework'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_cyber_framework_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_cyber_framework(conn)
            count2 = await ingest_domain_cyber_framework(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
