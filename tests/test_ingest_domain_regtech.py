"""Tests for RegTech domain taxonomy ingester.

RED tests - written before any implementation exists.

RegTech taxonomy organizes regulatory technology sector types:
  KYC/AML Automation     (drt_kyc*)        - identity, transaction, beneficial, PEP
  Regulatory Reporting   (drt_reporting*)   - filing, data, dashboard, XBRL
  Risk Monitoring        (drt_risk*)        - credit, market, operational, cyber
  Compliance Training    (drt_training*)    - e-learning, certification, simulation
  Sanctions Screening    (drt_sanctions*)   - list, payment, vessel, crypto

Source: NAICS 5221 + 5415 banking/tech industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_regtech import (
    REGTECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_regtech,
)


class TestDetermineLevel:
    def test_kyc_category_is_level_1(self):
        assert _determine_level("drt_kyc") == 1

    def test_kyc_type_is_level_2(self):
        assert _determine_level("drt_kyc_identity") == 2

    def test_reporting_category_is_level_1(self):
        assert _determine_level("drt_reporting") == 1

    def test_reporting_type_is_level_2(self):
        assert _determine_level("drt_reporting_auto") == 2

    def test_risk_category_is_level_1(self):
        assert _determine_level("drt_risk") == 1

    def test_risk_type_is_level_2(self):
        assert _determine_level("drt_risk_credit") == 2


class TestDetermineParent:
    def test_kyc_category_has_no_parent(self):
        assert _determine_parent("drt_kyc") is None

    def test_kyc_identity_parent_is_kyc(self):
        assert _determine_parent("drt_kyc_identity") == "drt_kyc"

    def test_reporting_auto_parent_is_reporting(self):
        assert _determine_parent("drt_reporting_auto") == "drt_reporting"

    def test_sanctions_list_parent_is_sanctions(self):
        assert _determine_parent("drt_sanctions_list") == "drt_sanctions"


class TestRegTechNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(REGTECH_NODES) > 0

    def test_has_kyc_category(self):
        codes = [n[0] for n in REGTECH_NODES]
        assert "drt_kyc" in codes

    def test_has_reporting_category(self):
        codes = [n[0] for n in REGTECH_NODES]
        assert "drt_reporting" in codes

    def test_has_risk_category(self):
        codes = [n[0] for n in REGTECH_NODES]
        assert "drt_risk" in codes

    def test_has_training_category(self):
        codes = [n[0] for n in REGTECH_NODES]
        assert "drt_training" in codes

    def test_has_sanctions_category(self):
        codes = [n[0] for n in REGTECH_NODES]
        assert "drt_sanctions" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in REGTECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in REGTECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in REGTECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in REGTECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(REGTECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in REGTECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_regtech_module_importable():
    assert callable(ingest_domain_regtech)
    assert isinstance(REGTECH_NODES, list)


def test_ingest_domain_regtech(db_pool):
    """Integration test: RegTech taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_regtech(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_regtech'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_regtech'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_regtech_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_regtech(conn)
            count2 = await ingest_domain_regtech(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
