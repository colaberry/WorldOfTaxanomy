"""Tests for MS-DRG ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.ms_drg import (
    MDC_LIST,
    DRG_EXAMPLES,
    ingest_ms_drg,
)


class TestMdcList:
    def test_non_empty(self):
        assert len(MDC_LIST) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in MDC_LIST]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in MDC_LIST:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in MDC_LIST:
            assert "\u2014" not in item[1]

    def test_has_mdc00_entry(self):
        codes = [n[0] for n in MDC_LIST]
        assert "MDC00" in codes


class TestDrgExamples:
    def test_non_empty(self):
        assert len(DRG_EXAMPLES) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in DRG_EXAMPLES]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in DRG_EXAMPLES:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in DRG_EXAMPLES:
            assert "\u2014" not in item[1]

    def test_parents_exist_in_top(self):
        top_codes = {n[0] for n in MDC_LIST}
        for code, title, parent in DRG_EXAMPLES:
            assert parent in top_codes, f"Parent {parent!r} not in top-level codes"


def test_ms_drg_total_minimum():
    assert len(MDC_LIST) + len(DRG_EXAMPLES) >= 50


def test_ms_drg_module_importable():
    assert callable(ingest_ms_drg)
    assert isinstance(MDC_LIST, list)
    assert isinstance(DRG_EXAMPLES, list)


def test_ingest_ms_drg(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_ms_drg(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'ms_drg'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_ms_drg_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_ms_drg(conn)
            count2 = await ingest_ms_drg(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
