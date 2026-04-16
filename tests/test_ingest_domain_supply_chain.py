"""Tests for domain_supply_chain ingester (Phase 28 - Cross-cutting)."""
from __future__ import annotations

import asyncio
import pytest
from world_of_taxonomy.ingest.domain_supply_chain import (
    SUPPLY_CHAIN_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_supply_chain,
)


class TestSupplyChainDetermineLevel:
    def test_top_level_category(self):
        assert _determine_level("dsc_incoterm") == 1

    def test_sub_level_node(self):
        assert _determine_level("dsc_incoterm_exw") == 2

    def test_another_top_level(self):
        assert _determine_level("dsc_lane") == 1

    def test_another_sub_level(self):
        assert _determine_level("dsc_lane_domestic") == 2

    def test_tier_top(self):
        assert _determine_level("dsc_tier") == 1

    def test_tier_sub(self):
        assert _determine_level("dsc_tier_1pl") == 2


class TestSupplyChainDetermineParent:
    def test_top_level_has_no_parent(self):
        assert _determine_parent("dsc_incoterm") is None

    def test_sub_level_returns_parent(self):
        assert _determine_parent("dsc_incoterm_exw") == "dsc_incoterm"

    def test_another_top_level_none(self):
        assert _determine_parent("dsc_lane") is None

    def test_another_sub_returns_parent(self):
        assert _determine_parent("dsc_lane_domestic") == "dsc_lane"

    def test_tier_sub_returns_parent(self):
        assert _determine_parent("dsc_tier_1pl") == "dsc_tier"

    def test_customs_sub_returns_parent(self):
        assert _determine_parent("dsc_customs_ftz") == "dsc_customs"


class TestSupplyChainNodes:
    def test_nodes_is_list(self):
        assert isinstance(SUPPLY_CHAIN_NODES, list)

    def test_at_least_15_nodes(self):
        assert len(SUPPLY_CHAIN_NODES) >= 15

    def test_all_tuples_four_elements(self):
        for node in SUPPLY_CHAIN_NODES:
            assert len(node) == 4

    def test_top_level_nodes_have_no_parent(self):
        for code, _title, level, parent in SUPPLY_CHAIN_NODES:
            if level == 1:
                assert parent is None, f"{code} level 1 should have no parent"

    def test_sub_nodes_have_parent(self):
        for code, _title, level, parent in SUPPLY_CHAIN_NODES:
            if level == 2:
                assert parent is not None, f"{code} level 2 should have a parent"

    def test_no_em_dashes(self):
        for code, title, _level, _parent in SUPPLY_CHAIN_NODES:
            assert "\u2014" not in title, f"em-dash found in title: {title}"
            assert "\u2014" not in code, f"em-dash found in code: {code}"


def test_ingest_domain_supply_chain(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_supply_chain(conn)
            assert count == len(SUPPLY_CHAIN_NODES)
            assert count >= 15
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_supply_chain'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_supply_chain_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_supply_chain(conn)
            count2 = await ingest_domain_supply_chain(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
