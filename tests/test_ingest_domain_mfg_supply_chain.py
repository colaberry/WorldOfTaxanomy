"""Tests for Manufacturing Supply Chain Integration Model Types domain taxonomy ingester."""
from __future__ import annotations

import asyncio
import pytest

from world_of_taxonomy.ingest.domain_mfg_supply_chain import (
    MFG_SUPPLY_CHAIN_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_mfg_supply_chain,
)


class TestMfgSupplyChainNodes:
    def test_non_empty(self):
        assert len(MFG_SUPPLY_CHAIN_NODES) > 0

    def test_minimum_count(self):
        assert len(MFG_SUPPLY_CHAIN_NODES) >= 17

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in MFG_SUPPLY_CHAIN_NODES]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for code, title, level, parent in MFG_SUPPLY_CHAIN_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_em_dashes(self):
        for code, title, level, parent in MFG_SUPPLY_CHAIN_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"

    def test_level_1_nodes_have_no_parent(self):
        for code, title, level, parent in MFG_SUPPLY_CHAIN_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_nodes_have_parent(self):
        for code, title, level, parent in MFG_SUPPLY_CHAIN_NODES:
            if level == 2:
                assert parent is not None

    def test_level_2_parents_are_valid(self):
        codes = {n[0] for n in MFG_SUPPLY_CHAIN_NODES}
        for code, title, level, parent in MFG_SUPPLY_CHAIN_NODES:
            if level == 2:
                assert parent in codes, f"Parent {parent} not found for {code}"

    def test_has_expected_root_dmfgsc_vertical(self):
        codes = [n[0] for n in MFG_SUPPLY_CHAIN_NODES]
        assert "dmfgsc_vertical" in codes


class TestDetermineLevelMfgSupplyChain:
    def test_root_code_is_level_1(self):
        assert _determine_level("dmfgsc_vertical") == 1

    def test_leaf_code_is_level_2(self):
        assert _determine_level("dmfgsc_vertical_full") == 2


class TestDetermineParentMfgSupplyChain:
    def test_root_has_no_parent(self):
        assert _determine_parent("dmfgsc_vertical") is None

    def test_leaf_parent_is_root(self):
        assert _determine_parent("dmfgsc_vertical_full") == "dmfgsc_vertical"


def test_domain_mfg_supply_chain_module_importable():
    assert callable(ingest_domain_mfg_supply_chain)
    assert isinstance(MFG_SUPPLY_CHAIN_NODES, list)


def test_ingest_domain_mfg_supply_chain(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_mfg_supply_chain(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_mfg_supply_chain'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_mfg_supply_chain_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_mfg_supply_chain(conn)
            count2 = await ingest_domain_mfg_supply_chain(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
