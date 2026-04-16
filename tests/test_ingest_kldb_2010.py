"""Tests for KldB 2010 ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.kldb_2010 import (
    AREAS,
    ingest_kldb_2010,
)


class TestAreas:
    def test_non_empty(self):
        assert len(AREAS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in AREAS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in AREAS:
            assert item[1].strip(), f"Empty title for {item[0]!r}"

    def test_no_em_dashes(self):
        for item in AREAS:
            assert "\u2014" not in item[1], f"Em-dash in {item[0]!r}"

    def test_minimum_count(self):
        assert len(AREAS) >= 10

    def test_has_1_group(self):
        codes = [n[0] for n in AREAS]
        assert "1" in codes


def test_kldb_2010_module_importable():
    assert callable(ingest_kldb_2010)
    assert isinstance(AREAS, list)


def test_ingest_kldb_2010(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.isco_08 import ingest_isco_08
        async with db_pool.acquire() as conn:
            await ingest_isco_08(conn)
            count = await ingest_kldb_2010(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'kldb_2010'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_kldb_2010_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.isco_08 import ingest_isco_08
        async with db_pool.acquire() as conn:
            await ingest_isco_08(conn)
            count1 = await ingest_kldb_2010(conn)
            count2 = await ingest_kldb_2010(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
