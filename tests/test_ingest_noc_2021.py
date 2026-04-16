"""Tests for NOC 2021 ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.noc_2021 import (
    MAJOR_GROUPS,
    ingest_noc_2021,
)


class TestMajorGroups:
    def test_non_empty(self):
        assert len(MAJOR_GROUPS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in MAJOR_GROUPS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in MAJOR_GROUPS:
            assert item[1].strip(), f"Empty title for {item[0]!r}"

    def test_no_em_dashes(self):
        for item in MAJOR_GROUPS:
            assert "\u2014" not in item[1], f"Em-dash in {item[0]!r}"

    def test_minimum_count(self):
        assert len(MAJOR_GROUPS) >= 10

    def test_has_0_group(self):
        codes = [n[0] for n in MAJOR_GROUPS]
        assert "0" in codes


def test_noc_2021_module_importable():
    assert callable(ingest_noc_2021)
    assert isinstance(MAJOR_GROUPS, list)


def test_ingest_noc_2021(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.isco_08 import ingest_isco_08
        async with db_pool.acquire() as conn:
            await ingest_isco_08(conn)
            count = await ingest_noc_2021(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'noc_2021'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_noc_2021_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.isco_08 import ingest_isco_08
        async with db_pool.acquire() as conn:
            await ingest_isco_08(conn)
            count1 = await ingest_noc_2021(conn)
            count2 = await ingest_noc_2021(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
