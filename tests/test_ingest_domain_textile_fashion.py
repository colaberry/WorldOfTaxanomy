"""Tests for Textile and Fashion domain taxonomy ingester.

Textile and Fashion taxonomy organizes textile and fashion sector types:
  Fiber and Yarn       (dtf_fiber*)   - natural, synthetic, blended, recycled
  Fabric Manufacturing (dtf_fabric*)  - woven, knit, nonwoven, dyeing
  Apparel Design       (dtf_apparel*) - ready-to-wear, sportswear, sustainable
  Fast Fashion         (dtf_fast*)    - mass-market, ultra-fast, e-commerce
  Luxury/Haute Couture (dtf_luxury*)  - couture, brands, artisan, resale

Source: NAICS 3131/3132/3133 textile industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_textile_fashion import (
    NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_textile_fashion,
)


class TestDetermineLevel:
    def test_fiber_category_is_level_1(self):
        assert _determine_level("dtf_fiber") == 1

    def test_fiber_type_is_level_2(self):
        assert _determine_level("dtf_fiber_natural") == 2

    def test_fabric_category_is_level_1(self):
        assert _determine_level("dtf_fabric") == 1

    def test_fabric_type_is_level_2(self):
        assert _determine_level("dtf_fabric_woven") == 2

    def test_luxury_category_is_level_1(self):
        assert _determine_level("dtf_luxury") == 1

    def test_luxury_type_is_level_2(self):
        assert _determine_level("dtf_luxury_couture") == 2


class TestDetermineParent:
    def test_fiber_category_has_no_parent(self):
        assert _determine_parent("dtf_fiber") is None

    def test_fiber_natural_parent_is_fiber(self):
        assert _determine_parent("dtf_fiber_natural") == "dtf_fiber"

    def test_fabric_woven_parent_is_fabric(self):
        assert _determine_parent("dtf_fabric_woven") == "dtf_fabric"

    def test_luxury_couture_parent_is_luxury(self):
        assert _determine_parent("dtf_luxury_couture") == "dtf_luxury"


class TestNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(NODES) > 0

    def test_minimum_node_count(self):
        assert len(NODES) >= 20

    def test_has_fiber_category(self):
        codes = [n[0] for n in NODES]
        assert "dtf_fiber" in codes

    def test_has_fabric_category(self):
        codes = [n[0] for n in NODES]
        assert "dtf_fabric" in codes

    def test_has_luxury_category(self):
        codes = [n[0] for n in NODES]
        assert "dtf_luxury" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in NODES:
            if level == 2:
                assert parent is not None

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_textile_fashion_module_importable():
    assert callable(ingest_domain_textile_fashion)
    assert isinstance(NODES, list)


def test_ingest_domain_textile_fashion(db_pool):
    """Integration test: textile/fashion taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_textile_fashion(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_textile_fashion'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_textile_fashion_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_textile_fashion(conn)
            count2 = await ingest_domain_textile_fashion(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
