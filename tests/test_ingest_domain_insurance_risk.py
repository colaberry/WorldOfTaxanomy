"""Tests for Insurance Risk domain taxonomy ingester.

Insurance Risk taxonomy organizes insurance risk categories:
  Natural Catastrophe (dir_natcat*)  - hurricane, earthquake, flood, wildfire
  Liability Risk      (dir_liab*)    - product, professional, environmental, employer
  Financial Risk      (dir_fin*)     - credit, market, liquidity, reserve
  Operational Risk    (dir_ops*)     - fraud, model, regulatory, vendor
  Cyber Risk          (dir_cyber*)   - data breach, ransomware, BEC, cloud

Source: NAICS 5241 insurance industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_insurance_risk import (
    INSURANCE_RISK_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_insurance_risk,
)


class TestDetermineLevel:
    def test_natcat_category_is_level_1(self):
        assert _determine_level("dir_natcat") == 1

    def test_natcat_type_is_level_2(self):
        assert _determine_level("dir_natcat_hurricane") == 2

    def test_liab_category_is_level_1(self):
        assert _determine_level("dir_liab") == 1

    def test_fin_type_is_level_2(self):
        assert _determine_level("dir_fin_credit") == 2

    def test_ops_category_is_level_1(self):
        assert _determine_level("dir_ops") == 1

    def test_cyber_type_is_level_2(self):
        assert _determine_level("dir_cyber_breach") == 2


class TestDetermineParent:
    def test_natcat_category_has_no_parent(self):
        assert _determine_parent("dir_natcat") is None

    def test_natcat_hurricane_parent_is_natcat(self):
        assert _determine_parent("dir_natcat_hurricane") == "dir_natcat"

    def test_liab_product_parent_is_liab(self):
        assert _determine_parent("dir_liab_product") == "dir_liab"

    def test_cyber_breach_parent_is_cyber(self):
        assert _determine_parent("dir_cyber_breach") == "dir_cyber"


class TestInsuranceRiskNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(INSURANCE_RISK_NODES) > 0

    def test_has_natcat_category(self):
        codes = [n[0] for n in INSURANCE_RISK_NODES]
        assert "dir_natcat" in codes

    def test_has_liability_category(self):
        codes = [n[0] for n in INSURANCE_RISK_NODES]
        assert "dir_liab" in codes

    def test_has_financial_category(self):
        codes = [n[0] for n in INSURANCE_RISK_NODES]
        assert "dir_fin" in codes

    def test_has_operational_category(self):
        codes = [n[0] for n in INSURANCE_RISK_NODES]
        assert "dir_ops" in codes

    def test_has_cyber_category(self):
        codes = [n[0] for n in INSURANCE_RISK_NODES]
        assert "dir_cyber" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in INSURANCE_RISK_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in INSURANCE_RISK_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in INSURANCE_RISK_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in INSURANCE_RISK_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(INSURANCE_RISK_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in INSURANCE_RISK_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_insurance_risk_module_importable():
    assert callable(ingest_domain_insurance_risk)
    assert isinstance(INSURANCE_RISK_NODES, list)


def test_ingest_domain_insurance_risk(db_pool):
    """Integration test: insurance risk taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_insurance_risk(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_insurance_risk'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_insurance_risk_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_insurance_risk(conn)
            count2 = await ingest_domain_insurance_risk(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
