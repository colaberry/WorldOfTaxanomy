"""Tests for full-text search queries."""

import asyncio
import pytest

from world_of_taxanomy.query.search import search_nodes


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_search_by_title(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            results = await search_nodes(conn, "physician")
            assert len(results) >= 1
            codes = {r.code for r in results}
            assert "6211" in codes or "62111" in codes
    _run(_test())


def test_search_by_code(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            results = await search_nodes(conn, "6211")
            assert len(results) >= 1
            assert any(r.code == "6211" for r in results)
    _run(_test())


def test_search_cross_system(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            results = await search_nodes(conn, "health")
            systems = {r.system_id for r in results}
            assert len(systems) >= 1
    _run(_test())


def test_search_filter_by_system(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            results = await search_nodes(conn, "health", system_id="naics_2022")
            for r in results:
                assert r.system_id == "naics_2022"
    _run(_test())


def test_search_limit(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            results = await search_nodes(conn, "farming", limit=2)
            assert len(results) <= 2
    _run(_test())


def test_search_no_results(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            results = await search_nodes(conn, "xyznonexistent")
            assert results == []
    _run(_test())
