"""Tests for Semiconductor Application End-Market Types domain taxonomy ingester."""
from __future__ import annotations

import asyncio
import pytest

from world_of_taxonomy.ingest.domain_semiconductor_application import (
    SEMICONDUCTOR_APP_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_semiconductor_application,
)


class TestSemiconductorApplicationNodes:
    def test_non_empty(self):
        assert len(SEMICONDUCTOR_APP_NODES) > 0

    def test_minimum_count(self):
        assert len(SEMICONDUCTOR_APP_NODES) >= 18

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in SEMICONDUCTOR_APP_NODES]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for code, title, level, parent in SEMICONDUCTOR_APP_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_em_dashes(self):
        for code, title, level, parent in SEMICONDUCTOR_APP_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"

    def test_level_1_nodes_have_no_parent(self):
        for code, title, level, parent in SEMICONDUCTOR_APP_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_nodes_have_parent(self):
        for code, title, level, parent in SEMICONDUCTOR_APP_NODES:
            if level == 2:
                assert parent is not None

    def test_level_2_parents_are_valid(self):
        codes = {n[0] for n in SEMICONDUCTOR_APP_NODES}
        for code, title, level, parent in SEMICONDUCTOR_APP_NODES:
            if level == 2:
                assert parent in codes, f"Parent {parent} not found for {code}"

    def test_has_expected_root_dscapp_consumer(self):
        codes = [n[0] for n in SEMICONDUCTOR_APP_NODES]
        assert "dscapp_consumer" in codes


class TestDetermineLevelSemiconductorApplication:
    def test_root_code_is_level_1(self):
        assert _determine_level("dscapp_consumer") == 1

    def test_leaf_code_is_level_2(self):
        assert _determine_level("dscapp_consumer_mobile") == 2


class TestDetermineParentSemiconductorApplication:
    def test_root_has_no_parent(self):
        assert _determine_parent("dscapp_consumer") is None

    def test_leaf_parent_is_root(self):
        assert _determine_parent("dscapp_consumer_mobile") == "dscapp_consumer"


def test_domain_semiconductor_application_module_importable():
    assert callable(ingest_domain_semiconductor_application)
    assert isinstance(SEMICONDUCTOR_APP_NODES, list)


def test_ingest_domain_semiconductor_application(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_semiconductor_application(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_semiconductor_application'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_semiconductor_application_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_semiconductor_application(conn)
            count2 = await ingest_domain_semiconductor_application(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
