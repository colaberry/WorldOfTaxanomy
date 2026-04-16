"""Tests for HTS (US) ingester."""
import asyncio
import pytest

from world_of_taxonomy.ingest.hts_us import (
    HTS_SECTIONS,
    HTS_CHAPTERS,
    ingest_hts_us,
)


class TestHtsSections:
    def test_non_empty(self):
        assert len(HTS_SECTIONS) > 0

    def test_has_22_sections(self):
        assert len(HTS_SECTIONS) == 22

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in HTS_SECTIONS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for code, title in HTS_SECTIONS:
            assert title.strip()

    def test_no_em_dashes(self):
        for code, title in HTS_SECTIONS:
            assert "\u2014" not in title


class TestHtsChapters:
    def test_non_empty(self):
        assert len(HTS_CHAPTERS) > 0

    def test_minimum_chapters(self):
        assert len(HTS_CHAPTERS) >= 96

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in HTS_CHAPTERS]
        assert len(codes) == len(set(codes))

    def test_no_empty_titles(self):
        for code, title, parent in HTS_CHAPTERS:
            assert title.strip()

    def test_no_em_dashes(self):
        for code, title, parent in HTS_CHAPTERS:
            assert "\u2014" not in title

    def test_parents_are_sections(self):
        section_codes = {n[0] for n in HTS_SECTIONS}
        for code, title, parent in HTS_CHAPTERS:
            assert parent in section_codes

    def test_has_chapter_84_machinery(self):
        codes = [n[0] for n in HTS_CHAPTERS]
        assert "84" in codes

    def test_has_chapter_30_pharma(self):
        codes = [n[0] for n in HTS_CHAPTERS]
        assert "30" in codes


def test_hts_us_module_importable():
    assert callable(ingest_hts_us)
    assert isinstance(HTS_SECTIONS, list)
    assert isinstance(HTS_CHAPTERS, list)


def test_ingest_hts_us(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_hts_us(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'hts_us'"
            )
            assert row is not None
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_hts_us_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_hts_us(conn)
            count2 = await ingest_hts_us(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
