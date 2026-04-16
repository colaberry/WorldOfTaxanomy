"""Tests for US FIPS ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.us_fips import (
    US_STATES,
    MAJOR_COUNTIES,
    ingest_us_fips,
)


class TestUsStates:
    def test_non_empty(self):
        assert len(US_STATES) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in US_STATES]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for code, title in US_STATES:
            assert title.strip()

    def test_no_em_dashes(self):
        for code, title in US_STATES:
            assert "\u2014" not in title

    def test_has_california(self):
        codes = [n[0] for n in US_STATES]
        assert "06" in codes

    def test_has_texas(self):
        codes = [n[0] for n in US_STATES]
        assert "48" in codes

    def test_fifty_states_plus_territories(self):
        assert len(US_STATES) >= 50


class TestMajorCounties:
    def test_non_empty(self):
        assert len(MAJOR_COUNTIES) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in MAJOR_COUNTIES]
        assert len(codes) == len(set(codes))

    def test_parents_are_states(self):
        state_codes = {n[0] for n in US_STATES}
        for code, title, parent in MAJOR_COUNTIES:
            assert parent in state_codes

    def test_has_los_angeles(self):
        codes = [n[0] for n in MAJOR_COUNTIES]
        assert "06037" in codes


def test_us_fips_module_importable():
    assert callable(ingest_us_fips)
    assert isinstance(US_STATES, list)


def test_ingest_us_fips(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_us_fips(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'us_fips'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_us_fips_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_us_fips(conn)
            count2 = await ingest_us_fips(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
