"""Tests for Data Center and Cloud Infrastructure domain taxonomy ingester.

RED tests - written before any implementation exists.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_datacenter_cloud import (
    DATACENTER_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_datacenter_cloud,
)


class TestDetermineLevel:
    def test_hyper_category_is_level_1(self):
        assert _determine_level("ddc_hyper") == 1

    def test_public_is_level_2(self):
        assert _determine_level("ddc_hyper_public") == 2

    def test_colo_category_is_level_1(self):
        assert _determine_level("ddc_colo") == 1


class TestDetermineParent:
    def test_hyper_has_no_parent(self):
        assert _determine_parent("ddc_hyper") is None

    def test_public_parent_is_hyper(self):
        assert _determine_parent("ddc_hyper_public") == "ddc_hyper"

    def test_retail_parent_is_colo(self):
        assert _determine_parent("ddc_colo_retail") == "ddc_colo"


class TestNodes:
    def test_nodes_non_empty(self):
        assert len(DATACENTER_NODES) > 0

    def test_has_hyper_category(self):
        codes = [n[0] for n in DATACENTER_NODES]
        assert "ddc_hyper" in codes

    def test_has_colo_category(self):
        codes = [n[0] for n in DATACENTER_NODES]
        assert "ddc_colo" in codes

    def test_has_edge_category(self):
        codes = [n[0] for n in DATACENTER_NODES]
        assert "ddc_edge" in codes

    def test_has_public_node(self):
        codes = [n[0] for n in DATACENTER_NODES]
        assert "ddc_hyper_public" in codes

    def test_has_retail_node(self):
        codes = [n[0] for n in DATACENTER_NODES]
        assert "ddc_colo_retail" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in DATACENTER_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in DATACENTER_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in DATACENTER_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in DATACENTER_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(DATACENTER_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in DATACENTER_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_module_importable():
    assert callable(ingest_domain_datacenter_cloud)
    assert isinstance(DATACENTER_NODES, list)


def test_ingest(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_datacenter_cloud(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_datacenter_cloud'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_datacenter_cloud(conn)
            count2 = await ingest_domain_datacenter_cloud(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
