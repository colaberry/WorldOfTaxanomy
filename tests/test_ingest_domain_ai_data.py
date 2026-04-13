"""Tests for AI and Data domain taxonomy ingester.

RED tests - written before any implementation exists.

AI and Data taxonomy organizes artificial intelligence and data sector types:
  AI Model Types    (dai_model*)   - foundation, generative, discriminative, RL
  Data Infrastructure (dai_infra*) - lakes, warehouses, pipelines, vector DBs
  AI Verticals      (dai_vert*)    - healthcare AI, finance AI, logistics AI
  MLOps/Governance  (dai_ops*)     - model registry, lineage, observability

Source: NAICS 5415 + 5182 AI/data industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_ai_data import (
    AI_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_ai_data,
)


class TestDetermineLevel:
    def test_model_category_is_level_1(self):
        assert _determine_level("dai_model") == 1

    def test_model_type_is_level_2(self):
        assert _determine_level("dai_model_found") == 2

    def test_infra_category_is_level_1(self):
        assert _determine_level("dai_infra") == 1

    def test_infra_type_is_level_2(self):
        assert _determine_level("dai_infra_lake") == 2

    def test_vert_category_is_level_1(self):
        assert _determine_level("dai_vert") == 1

    def test_vert_type_is_level_2(self):
        assert _determine_level("dai_vert_health") == 2


class TestDetermineParent:
    def test_model_category_has_no_parent(self):
        assert _determine_parent("dai_model") is None

    def test_model_found_parent_is_model(self):
        assert _determine_parent("dai_model_found") == "dai_model"

    def test_infra_lake_parent_is_infra(self):
        assert _determine_parent("dai_infra_lake") == "dai_infra"

    def test_vert_health_parent_is_vert(self):
        assert _determine_parent("dai_vert_health") == "dai_vert"


class TestAINodes:
    def test_nodes_list_is_non_empty(self):
        assert len(AI_NODES) > 0

    def test_has_model_category(self):
        codes = [n[0] for n in AI_NODES]
        assert "dai_model" in codes

    def test_has_infra_category(self):
        codes = [n[0] for n in AI_NODES]
        assert "dai_infra" in codes

    def test_has_verticals_category(self):
        codes = [n[0] for n in AI_NODES]
        assert "dai_vert" in codes

    def test_has_generative_ai_node(self):
        codes = [n[0] for n in AI_NODES]
        assert any("gen" in c for c in codes), "Should have a generative AI node"

    def test_all_titles_non_empty(self):
        for code, title, level, parent in AI_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in AI_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in AI_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in AI_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(AI_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in AI_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_ai_data_module_importable():
    assert callable(ingest_domain_ai_data)
    assert isinstance(AI_NODES, list)


def test_ingest_domain_ai_data(db_pool):
    """Integration test: AI/data taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_ai_data(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_ai_data'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_ai_data'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_ai_data_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_ai_data(conn)
            count2 = await ingest_domain_ai_data(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
