"""Tests for HCPCS Level II ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.hcpcs_l2 import (
    HCPCS_SECTIONS,
    HCPCS_SUBSECTIONS,
    ingest_hcpcs_l2,
)


class TestHcpcsSections:
    def test_non_empty(self):
        assert len(HCPCS_SECTIONS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in HCPCS_SECTIONS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in HCPCS_SECTIONS:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in HCPCS_SECTIONS:
            assert "\u2014" not in item[1]

    def test_has_a_entry(self):
        codes = [n[0] for n in HCPCS_SECTIONS]
        assert "A" in codes


class TestHcpcsSubsections:
    def test_non_empty(self):
        assert len(HCPCS_SUBSECTIONS) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in HCPCS_SUBSECTIONS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in HCPCS_SUBSECTIONS:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in HCPCS_SUBSECTIONS:
            assert "\u2014" not in item[1]

    def test_parents_exist_in_top(self):
        top_codes = {n[0] for n in HCPCS_SECTIONS}
        for code, title, parent in HCPCS_SUBSECTIONS:
            assert parent in top_codes, f"Parent {parent!r} not in top-level codes"


def test_hcpcs_l2_total_minimum():
    assert len(HCPCS_SECTIONS) + len(HCPCS_SUBSECTIONS) >= 59


def test_hcpcs_l2_module_importable():
    assert callable(ingest_hcpcs_l2)
    assert isinstance(HCPCS_SECTIONS, list)
    assert isinstance(HCPCS_SUBSECTIONS, list)


def test_ingest_hcpcs_l2(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_hcpcs_l2(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'hcpcs_l2'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_hcpcs_l2_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_hcpcs_l2(conn)
            count2 = await ingest_hcpcs_l2(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
