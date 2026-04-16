"""Tests for Forestry Management domain taxonomy ingester.

Forestry Management taxonomy organizes forestry management sector types:
  Timber Production    (dfm_timber*)   - softwood, hardwood, plantation, selective
  Reforestation        (dfm_reforest*) - nursery, planting, restoration, carbon
  Forest Conservation  (dfm_conserve*) - protected, fire, pest, biodiversity
  Agroforestry         (dfm_agro*)     - silvopasture, alley, riparian, tropical
  Forest Products      (dfm_product*)  - lumber, pulp, non-timber, biomass

Source: NAICS 1131/1132/1133 forestry industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_forestry_mgmt import (
    NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_forestry_mgmt,
)


class TestDetermineLevel:
    def test_timber_category_is_level_1(self):
        assert _determine_level("dfm_timber") == 1

    def test_timber_type_is_level_2(self):
        assert _determine_level("dfm_timber_softwood") == 2

    def test_reforest_category_is_level_1(self):
        assert _determine_level("dfm_reforest") == 1

    def test_reforest_type_is_level_2(self):
        assert _determine_level("dfm_reforest_nursery") == 2

    def test_product_category_is_level_1(self):
        assert _determine_level("dfm_product") == 1

    def test_product_type_is_level_2(self):
        assert _determine_level("dfm_product_lumber") == 2


class TestDetermineParent:
    def test_timber_category_has_no_parent(self):
        assert _determine_parent("dfm_timber") is None

    def test_timber_softwood_parent_is_timber(self):
        assert _determine_parent("dfm_timber_softwood") == "dfm_timber"

    def test_reforest_nursery_parent_is_reforest(self):
        assert _determine_parent("dfm_reforest_nursery") == "dfm_reforest"

    def test_product_lumber_parent_is_product(self):
        assert _determine_parent("dfm_product_lumber") == "dfm_product"


class TestNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(NODES) > 0

    def test_minimum_node_count(self):
        assert len(NODES) >= 20

    def test_has_timber_category(self):
        codes = [n[0] for n in NODES]
        assert "dfm_timber" in codes

    def test_has_reforest_category(self):
        codes = [n[0] for n in NODES]
        assert "dfm_reforest" in codes

    def test_has_product_category(self):
        codes = [n[0] for n in NODES]
        assert "dfm_product" in codes

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


def test_domain_forestry_mgmt_module_importable():
    assert callable(ingest_domain_forestry_mgmt)
    assert isinstance(NODES, list)


def test_ingest_domain_forestry_mgmt(db_pool):
    """Integration test: forestry management taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_forestry_mgmt(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_forestry_mgmt'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_forestry_mgmt_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_forestry_mgmt(conn)
            count2 = await ingest_domain_forestry_mgmt(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
