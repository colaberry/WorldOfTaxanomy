"""Tests for CPV 2008 ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.cpv_2008 import (
    CPV_DIVISIONS,
    CPV_GROUPS,
    ingest_cpv_2008,
)


class TestCpvDivisions:
    def test_non_empty(self):
        assert len(CPV_DIVISIONS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in CPV_DIVISIONS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in CPV_DIVISIONS:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in CPV_DIVISIONS:
            assert "\u2014" not in item[1]

    def test_minimum_cpv_divisions(self):
        assert len(CPV_DIVISIONS) >= 45

    def test_has_03_entry(self):
        codes = [n[0] for n in CPV_DIVISIONS]
        assert "03" in codes


class TestCpvGroups:
    def test_non_empty(self):
        assert len(CPV_GROUPS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in CPV_GROUPS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in CPV_GROUPS:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in CPV_GROUPS:
            assert "\u2014" not in item[1]

    def test_parents_exist_in_top(self):
        top_codes = {n[0] for n in CPV_DIVISIONS}
        for code, title, parent in CPV_GROUPS:
            assert parent in top_codes


def test_cpv_2008_module_importable():
    assert callable(ingest_cpv_2008)


def test_ingest_cpv_2008(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_cpv_2008(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'cpv_2008'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_cpv_2008_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_cpv_2008(conn)
            count2 = await ingest_cpv_2008(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
