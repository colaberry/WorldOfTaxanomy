"""Tests for NUCC HCPT ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.nucc_hcpt import (
    NPI_GROUPINGS,
    NPI_CLASSIFICATIONS,
    ingest_nucc_hcpt,
)


class TestNpiGroupings:
    def test_non_empty(self):
        assert len(NPI_GROUPINGS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NPI_GROUPINGS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in NPI_GROUPINGS:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in NPI_GROUPINGS:
            assert "\u2014" not in item[1]

    def test_has_a_entry(self):
        codes = [n[0] for n in NPI_GROUPINGS]
        assert "A" in codes


class TestNpiClassifications:
    def test_non_empty(self):
        assert len(NPI_CLASSIFICATIONS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NPI_CLASSIFICATIONS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in NPI_CLASSIFICATIONS:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in NPI_CLASSIFICATIONS:
            assert "\u2014" not in item[1]

    def test_parents_exist_in_top(self):
        top_codes = {n[0] for n in NPI_GROUPINGS}
        for code, title, parent in NPI_CLASSIFICATIONS:
            assert parent in top_codes, f"Parent {parent!r} not in top-level codes"


def test_nucc_hcpt_total_minimum():
    assert len(NPI_GROUPINGS) + len(NPI_CLASSIFICATIONS) >= 94


def test_nucc_hcpt_module_importable():
    assert callable(ingest_nucc_hcpt)
    assert isinstance(NPI_GROUPINGS, list)
    assert isinstance(NPI_CLASSIFICATIONS, list)


def test_ingest_nucc_hcpt(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_nucc_hcpt(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'nucc_hcpt'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_nucc_hcpt_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_nucc_hcpt(conn)
            count2 = await ingest_nucc_hcpt(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
