"""Tests for cross-system equivalence queries."""

import asyncio
import pytest

from world_of_taxanomy.query.equivalence import (
    get_equivalences, translate_code, get_crosswalk_stats,
)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_get_equivalences(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            equivs = await get_equivalences(conn, "naics_2022", "6211")
            assert len(equivs) >= 1
            assert any(e.target_system == "isic_rev4" and e.target_code == "8620" for e in equivs)
    _run(_test())


def test_get_equivalences_reverse(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            equivs = await get_equivalences(conn, "isic_rev4", "8620")
            assert len(equivs) >= 1
            assert any(e.target_system == "naics_2022" and e.target_code == "6211" for e in equivs)
    _run(_test())


def test_translate_code(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            results = await translate_code(conn, "naics_2022", "6211", "isic_rev4")
            assert len(results) >= 1
            assert results[0].target_code == "8620"
    _run(_test())


def test_get_crosswalk_stats(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            stats = await get_crosswalk_stats(conn)
            assert len(stats) > 0
            pairs = {(s["source_system"], s["target_system"]) for s in stats}
            assert ("naics_2022", "isic_rev4") in pairs
    _run(_test())


def test_no_equivalences(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            equivs = await get_equivalences(conn, "naics_2022", "62")
            assert equivs == []
    _run(_test())
