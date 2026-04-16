"""Tests for ACM CCS 2012 ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.acm_ccs import (
    ACM_TOPLEVEL,
    ACM_SUBJECTS,
    ingest_acm_ccs,
)


class TestAcmToplevel:
    def test_non_empty(self):
        assert len(ACM_TOPLEVEL) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in ACM_TOPLEVEL]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in ACM_TOPLEVEL:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in ACM_TOPLEVEL:
            assert "\u2014" not in item[1]

    def test_minimum_acm_toplevel(self):
        assert len(ACM_TOPLEVEL) >= 12

    def test_has_ccs_gen_entry(self):
        codes = [n[0] for n in ACM_TOPLEVEL]
        assert "CCS-GEN" in codes


class TestAcmSubjects:
    def test_non_empty(self):
        assert len(ACM_SUBJECTS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in ACM_SUBJECTS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in ACM_SUBJECTS:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in ACM_SUBJECTS:
            assert "\u2014" not in item[1]

    def test_parents_exist_in_top(self):
        top_codes = {n[0] for n in ACM_TOPLEVEL}
        for code, title, parent in ACM_SUBJECTS:
            assert parent in top_codes


def test_acm_ccs_module_importable():
    assert callable(ingest_acm_ccs)


def test_ingest_acm_ccs(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_acm_ccs(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'acm_ccs'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_acm_ccs_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_acm_ccs(conn)
            count2 = await ingest_acm_ccs(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
