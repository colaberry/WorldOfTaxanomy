"""Tests for SASB SICS ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.sasb_sics import (
    SASB_SECTORS,
    SASB_INDUSTRIES,
    ingest_sasb_sics,
)


class TestSasbSectors:
    def test_non_empty(self):
        assert len(SASB_SECTORS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in SASB_SECTORS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in SASB_SECTORS:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in SASB_SECTORS:
            assert "\u2014" not in item[1]

    def test_has_first_entry(self):
        codes = [n[0] for n in SASB_SECTORS]
        assert "CG" in codes


class TestSasbIndustries:
    def test_non_empty(self):
        assert len(SASB_INDUSTRIES) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in SASB_INDUSTRIES]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in SASB_INDUSTRIES:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in SASB_INDUSTRIES:
            assert "\u2014" not in item[1]

    def test_parents_exist_in_top(self):
        top_codes = {n[0] for n in SASB_SECTORS}
        for code, title, parent in SASB_INDUSTRIES:
            assert parent in top_codes


def test_sasb_sics_total_minimum():
    assert len(SASB_SECTORS) + len(SASB_INDUSTRIES) >= 86


def test_sasb_sics_module_importable():
    assert callable(ingest_sasb_sics)
    assert isinstance(SASB_SECTORS, list)
    assert isinstance(SASB_INDUSTRIES, list)


def test_ingest_sasb_sics(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_sasb_sics(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'sasb_sics'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_sasb_sics_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_sasb_sics(conn)
            count2 = await ingest_sasb_sics(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
