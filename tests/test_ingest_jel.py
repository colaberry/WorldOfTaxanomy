"""Tests for JEL Codes ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.jel import (
    JEL_MAIN,
    JEL_SUBCODES,
    ingest_jel,
)


class TestJelMain:
    def test_non_empty(self):
        assert len(JEL_MAIN) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in JEL_MAIN]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in JEL_MAIN:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in JEL_MAIN:
            assert "\u2014" not in item[1]

    def test_minimum_jel_main(self):
        assert len(JEL_MAIN) >= 20

    def test_has_a_entry(self):
        codes = [n[0] for n in JEL_MAIN]
        assert "A" in codes


class TestJelSubcodes:
    def test_non_empty(self):
        assert len(JEL_SUBCODES) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in JEL_SUBCODES]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in JEL_SUBCODES:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in JEL_SUBCODES:
            assert "\u2014" not in item[1]

    def test_parents_exist_in_top(self):
        top_codes = {n[0] for n in JEL_MAIN}
        for code, title, parent in JEL_SUBCODES:
            assert parent in top_codes


def test_jel_module_importable():
    assert callable(ingest_jel)


def test_ingest_jel(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_jel(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'jel'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_jel_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_jel(conn)
            count2 = await ingest_jel(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
