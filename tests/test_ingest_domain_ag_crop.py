"""Tests for Agriculture Crop Type domain taxonomy ingester.

RED tests - written before any implementation exists.

Crop taxonomy organizes agricultural crop types into categories:
  Cereal Grains  (dac_grain*)  - wheat, corn, rice, barley, oats, sorghum
  Oilseeds       (dac_oil*)    - soybeans, canola, sunflower, peanuts, cottonseed
  Vegetables     (dac_veg*)    - root, leafy, fruiting, legume vegetables
  Fruits/Tree    (dac_fruit*)  - citrus, pome, stone, tropical, berries
  Fiber Crops    (dac_fiber*)  - cotton, hemp, flax
  Sugar Crops    (dac_sugar*)  - sugarcane, sugar beets
  Forages        (dac_forage*) - hay, alfalfa, silage, pasture grasses
  Specialty      (dac_spec*)   - tobacco, hops, herbs, spices

Source: FAO commodity classification + USDA crop categories. Public domain.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_ag_crop import (
    CROP_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_ag_crop,
)


class TestDetermineLevel:
    def test_grain_category_is_level_1(self):
        assert _determine_level("dac_grain") == 1

    def test_grain_type_is_level_2(self):
        assert _determine_level("dac_grain_wheat") == 2

    def test_oil_category_is_level_1(self):
        assert _determine_level("dac_oil") == 1

    def test_oil_type_is_level_2(self):
        assert _determine_level("dac_oil_soy") == 2

    def test_fruit_category_is_level_1(self):
        assert _determine_level("dac_fruit") == 1

    def test_fruit_type_is_level_2(self):
        assert _determine_level("dac_fruit_citrus") == 2


class TestDetermineParent:
    def test_grain_category_has_no_parent(self):
        assert _determine_parent("dac_grain") is None

    def test_wheat_parent_is_grain(self):
        assert _determine_parent("dac_grain_wheat") == "dac_grain"

    def test_soy_parent_is_oil(self):
        assert _determine_parent("dac_oil_soy") == "dac_oil"

    def test_citrus_parent_is_fruit(self):
        assert _determine_parent("dac_fruit_citrus") == "dac_fruit"


class TestCropNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(CROP_NODES) > 0

    def test_has_grain_category(self):
        codes = [n[0] for n in CROP_NODES]
        assert "dac_grain" in codes

    def test_has_oilseed_category(self):
        codes = [n[0] for n in CROP_NODES]
        assert "dac_oil" in codes

    def test_has_vegetable_category(self):
        codes = [n[0] for n in CROP_NODES]
        assert "dac_veg" in codes

    def test_has_fruit_category(self):
        codes = [n[0] for n in CROP_NODES]
        assert "dac_fruit" in codes

    def test_has_wheat(self):
        codes = [n[0] for n in CROP_NODES]
        assert "dac_grain_wheat" in codes

    def test_has_corn(self):
        codes = [n[0] for n in CROP_NODES]
        assert "dac_grain_corn" in codes

    def test_has_soybeans(self):
        codes = [n[0] for n in CROP_NODES]
        assert "dac_oil_soy" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in CROP_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in CROP_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in CROP_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in CROP_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(CROP_NODES) >= 30


def test_domain_ag_crop_module_importable():
    assert callable(ingest_domain_ag_crop)
    assert isinstance(CROP_NODES, list)


def test_ingest_domain_ag_crop(db_pool):
    """Integration test: crop taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_ag_crop(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_ag_crop'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_ag_crop'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_ag_crop_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_ag_crop(conn)
            count2 = await ingest_domain_ag_crop(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
