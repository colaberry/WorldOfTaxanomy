"""Tests for Insurance Product domain taxonomy ingester.

Insurance Product taxonomy organizes insurance product types:
  Life Insurance       (dip_life*)   - term, whole, universal, variable, annuity
  Property & Casualty  (dip_pc*)     - homeowners, auto, commercial, liability
  Health Insurance     (dip_health*) - group, individual, dental, LTC, disability
  Specialty Insurance  (dip_spec*)   - E&O, D&O, cyber, surety, marine

Source: NAICS 5241 insurance industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_insurance_product import (
    INSURANCE_PRODUCT_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_insurance_product,
)


class TestDetermineLevel:
    def test_life_category_is_level_1(self):
        assert _determine_level("dip_life") == 1

    def test_life_type_is_level_2(self):
        assert _determine_level("dip_life_term") == 2

    def test_pc_category_is_level_1(self):
        assert _determine_level("dip_pc") == 1

    def test_pc_type_is_level_2(self):
        assert _determine_level("dip_pc_auto") == 2

    def test_health_category_is_level_1(self):
        assert _determine_level("dip_health") == 1

    def test_spec_type_is_level_2(self):
        assert _determine_level("dip_spec_cyber") == 2


class TestDetermineParent:
    def test_life_category_has_no_parent(self):
        assert _determine_parent("dip_life") is None

    def test_life_term_parent_is_life(self):
        assert _determine_parent("dip_life_term") == "dip_life"

    def test_pc_auto_parent_is_pc(self):
        assert _determine_parent("dip_pc_auto") == "dip_pc"

    def test_health_group_parent_is_health(self):
        assert _determine_parent("dip_health_group") == "dip_health"


class TestInsuranceProductNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(INSURANCE_PRODUCT_NODES) > 0

    def test_has_life_category(self):
        codes = [n[0] for n in INSURANCE_PRODUCT_NODES]
        assert "dip_life" in codes

    def test_has_pc_category(self):
        codes = [n[0] for n in INSURANCE_PRODUCT_NODES]
        assert "dip_pc" in codes

    def test_has_health_category(self):
        codes = [n[0] for n in INSURANCE_PRODUCT_NODES]
        assert "dip_health" in codes

    def test_has_specialty_category(self):
        codes = [n[0] for n in INSURANCE_PRODUCT_NODES]
        assert "dip_spec" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in INSURANCE_PRODUCT_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in INSURANCE_PRODUCT_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in INSURANCE_PRODUCT_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in INSURANCE_PRODUCT_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(INSURANCE_PRODUCT_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in INSURANCE_PRODUCT_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_insurance_product_module_importable():
    assert callable(ingest_domain_insurance_product)
    assert isinstance(INSURANCE_PRODUCT_NODES, list)


def test_ingest_domain_insurance_product(db_pool):
    """Integration test: insurance product taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_insurance_product(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_insurance_product'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_insurance_product_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_insurance_product(conn)
            count2 = await ingest_domain_insurance_product(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
