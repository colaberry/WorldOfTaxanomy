"""Tests for Korean Standard Industry Classification (10th revision, 2017)."""
import asyncio
import pytest
from world_of_taxonomy.ingest.ksic_2017 import (
    SECTIONS,
    DIVISIONS,
    SECTION_TO_ISIC,
    ingest_ksic_2017,
)

def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)

class TestSections:
    def test_has_21_sections(self):
        assert len(SECTIONS) == 21

    def test_sections_run_a_through_u(self):
        assert set(SECTIONS.keys()) == set("ABCDEFGHIJKLMNOPQRSTU")

    def test_all_titles_non_empty(self):
        for code, title in SECTIONS.items():
            assert len(title) > 0

    def test_section_a_title(self):
        assert SECTIONS["A"].startswith("Agriculture, Forestr")

class TestDivisions:
    def test_divisions_non_empty(self):
        assert len(DIVISIONS) > 0

    def test_all_division_sections_valid(self):
        for sec, div, title in DIVISIONS:
            assert sec in SECTIONS, f"Unknown section {sec} for division {div}"

    def test_all_division_titles_non_empty(self):
        for sec, div, title in DIVISIONS:
            assert len(title) > 0

    def test_no_duplicate_division_codes(self):
        codes = [d[1] for d in DIVISIONS]
        assert len(codes) == len(set(codes))

class TestSectionToIsic:
    def test_all_sections_mapped(self):
        assert set(SECTION_TO_ISIC.keys()) == set(SECTIONS.keys())

    def test_isic_targets_are_valid_sections(self):
        valid = set("ABCDEFGHIJKLMNOPQRSTU")
        for src, tgt in SECTION_TO_ISIC.items():
            assert tgt in valid, f"{tgt} is not a valid ISIC section"

def test_ksic_2017_importable():
    assert callable(ingest_ksic_2017)

def test_ingest_ksic_2017(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM equivalence WHERE source_system=$1 OR target_system=$1", "ksic_2017")
            await conn.execute("DELETE FROM classification_node WHERE system_id=$1", "ksic_2017")
            await conn.execute("DELETE FROM classification_system WHERE id=$1", "ksic_2017")
            count = await ingest_ksic_2017(conn)
            assert count == 108
            row = await conn.fetchrow("SELECT node_count, region FROM classification_system WHERE id=$1", "ksic_2017")
            assert row is not None
            assert row["node_count"] == 108
            assert row["region"] == "South Korea"
    _run(_test())

def test_ingest_ksic_2017_idempotent(db_pool):
    async def _test():
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM equivalence WHERE source_system=$1 OR target_system=$1", "ksic_2017")
            await conn.execute("DELETE FROM classification_node WHERE system_id=$1", "ksic_2017")
            await conn.execute("DELETE FROM classification_system WHERE id=$1", "ksic_2017")
            c1 = await ingest_ksic_2017(conn)
            c2 = await ingest_ksic_2017(conn)
            assert c1 == c2 == 108
    _run(_test())
