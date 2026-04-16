"""Tests for EU Taxonomy ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.eu_taxonomy import (
    EU_TAX_OBJECTIVES,
    EU_TAX_ACTIVITIES,
    ingest_eu_taxonomy,
)


class TestEuTaxObjectives:
    def test_non_empty(self):
        assert len(EU_TAX_OBJECTIVES) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in EU_TAX_OBJECTIVES]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in EU_TAX_OBJECTIVES:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in EU_TAX_OBJECTIVES:
            assert "\u2014" not in item[1]

    def test_has_first_entry(self):
        codes = [n[0] for n in EU_TAX_OBJECTIVES]
        assert "OBJ1" in codes


class TestEuTaxActivities:
    def test_non_empty(self):
        assert len(EU_TAX_ACTIVITIES) > 0

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in EU_TAX_ACTIVITIES]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for item in EU_TAX_ACTIVITIES:
            assert item[1].strip()

    def test_no_em_dashes(self):
        for item in EU_TAX_ACTIVITIES:
            assert "\u2014" not in item[1]

    def test_parents_exist_in_top(self):
        top_codes = {n[0] for n in EU_TAX_OBJECTIVES}
        for code, title, parent in EU_TAX_ACTIVITIES:
            assert parent in top_codes


def test_eu_taxonomy_total_minimum():
    assert len(EU_TAX_OBJECTIVES) + len(EU_TAX_ACTIVITIES) >= 60


def test_eu_taxonomy_module_importable():
    assert callable(ingest_eu_taxonomy)
    assert isinstance(EU_TAX_OBJECTIVES, list)
    assert isinstance(EU_TAX_ACTIVITIES, list)


def test_ingest_eu_taxonomy(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_eu_taxonomy(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'eu_taxonomy'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_eu_taxonomy_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_eu_taxonomy(conn)
            count2 = await ingest_eu_taxonomy(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
