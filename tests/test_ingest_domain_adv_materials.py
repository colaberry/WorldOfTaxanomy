"""Tests for Advanced Materials domain taxonomy ingester.

RED tests - written before any implementation exists.

Advanced materials taxonomy organizes material science sector types:
  Composites        (dam_comp*)    - CFRP, GFRP, MMC, CMC
  Biomaterials      (dam_bio*)     - implants, scaffolds, drug delivery
  Smart Materials   (dam_smart*)   - shape-memory, piezoelectric, magnetostrictive
  Nanomaterials     (dam_nano*)    - CNT, graphene, quantum dots, nanoparticles
  High-Perf Alloys  (dam_alloy*)   - superalloys, titanium, high-entropy
  Ceramics/Glass    (dam_cer*)     - technical ceramics, optical glass, refractory
  Semiconducting    (dam_semi*)    - wide-bandgap, compound semiconductors
  Coatings/Surfaces (dam_coat*)    - thin films, hard coatings, anti-corrosion

Source: NAICS 325 + 327 advanced materials industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_adv_materials import (
    MATERIALS_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_adv_materials,
)


class TestDetermineLevel:
    def test_comp_category_is_level_1(self):
        assert _determine_level("dam_comp") == 1

    def test_comp_type_is_level_2(self):
        assert _determine_level("dam_comp_cfrp") == 2

    def test_nano_category_is_level_1(self):
        assert _determine_level("dam_nano") == 1

    def test_nano_type_is_level_2(self):
        assert _determine_level("dam_nano_cnt") == 2

    def test_smart_category_is_level_1(self):
        assert _determine_level("dam_smart") == 1

    def test_smart_type_is_level_2(self):
        assert _determine_level("dam_smart_sma") == 2


class TestDetermineParent:
    def test_comp_category_has_no_parent(self):
        assert _determine_parent("dam_comp") is None

    def test_comp_cfrp_parent_is_comp(self):
        assert _determine_parent("dam_comp_cfrp") == "dam_comp"

    def test_nano_cnt_parent_is_nano(self):
        assert _determine_parent("dam_nano_cnt") == "dam_nano"

    def test_smart_sma_parent_is_smart(self):
        assert _determine_parent("dam_smart_sma") == "dam_smart"


class TestMaterialsNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(MATERIALS_NODES) > 0

    def test_has_composites_category(self):
        codes = [n[0] for n in MATERIALS_NODES]
        assert "dam_comp" in codes

    def test_has_nanomaterials_category(self):
        codes = [n[0] for n in MATERIALS_NODES]
        assert "dam_nano" in codes

    def test_has_biomaterials_category(self):
        codes = [n[0] for n in MATERIALS_NODES]
        assert "dam_bio" in codes

    def test_has_smart_materials_category(self):
        codes = [n[0] for n in MATERIALS_NODES]
        assert "dam_smart" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in MATERIALS_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in MATERIALS_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in MATERIALS_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in MATERIALS_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(MATERIALS_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in MATERIALS_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_adv_materials_module_importable():
    assert callable(ingest_domain_adv_materials)
    assert isinstance(MATERIALS_NODES, list)


def test_ingest_domain_adv_materials(db_pool):
    """Integration test: advanced materials taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_adv_materials(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_adv_materials'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_adv_materials'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_adv_materials_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_adv_materials(conn)
            count2 = await ingest_domain_adv_materials(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
