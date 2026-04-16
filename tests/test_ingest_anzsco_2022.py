"""Tests for ANZSCO 2022 ingester (Phase 3-E).

ANZSCO: Australian and New Zealand Standard Classification of Occupations, 2022 edition.
Source: ABS SDMX API - CC BY 4.0
Hierarchy: Major Group (1-digit) -> Sub-Major (2) -> Minor (3) -> Unit (4) -> Occupation (6)
"""
from __future__ import annotations

import asyncio
import pytest
from pathlib import Path
from world_of_taxonomy.ingest.anzsco_2022 import (
    _determine_level,
    _determine_parent,
    _determine_sector,
    ingest_anzsco_2022,
)


class TestAnzsco2022DetermineLevel:
    def test_one_digit_is_level_1(self):
        assert _determine_level("1") == 1

    def test_two_digit_is_level_2(self):
        assert _determine_level("11") == 2

    def test_three_digit_is_level_3(self):
        assert _determine_level("111") == 3

    def test_four_digit_is_level_4(self):
        assert _determine_level("1111") == 4

    def test_six_digit_is_level_5(self):
        assert _determine_level("111111") == 5

    def test_tot_is_level_0(self):
        assert _determine_level("TOT") == 0


class TestAnzsco2022DetermineParent:
    def test_one_digit_has_no_parent(self):
        assert _determine_parent("1") is None

    def test_two_digit_parent_is_one_digit(self):
        assert _determine_parent("11") == "1"

    def test_three_digit_parent_is_two_digit(self):
        assert _determine_parent("111") == "11"

    def test_four_digit_parent_is_three_digit(self):
        assert _determine_parent("1111") == "111"

    def test_six_digit_parent_is_four_digit(self):
        assert _determine_parent("111111") == "1111"

    def test_tot_has_no_parent(self):
        assert _determine_parent("TOT") is None


class TestAnzsco2022DetermineSector:
    def test_one_digit_code_is_own_sector(self):
        assert _determine_sector("1") == "1"

    def test_two_digit_code_sector_is_first_digit(self):
        assert _determine_sector("11") == "1"

    def test_three_digit_sector_is_first_digit(self):
        assert _determine_sector("111") == "1"

    def test_four_digit_sector_is_first_digit(self):
        assert _determine_sector("1111") == "1"

    def test_six_digit_sector_is_first_digit(self):
        assert _determine_sector("251111") == "2"

    def test_sector_8_labourers(self):
        assert _determine_sector("8") == "8"


def test_ingest_anzsco_2022(db_pool):
    """Integration test: download from ABS SDMX or use cached file."""
    async def _run():
        from world_of_taxonomy.ingest.anzsco_2022 import ingest_anzsco_2022
        async with db_pool.acquire() as conn:
            count = await ingest_anzsco_2022(conn)
            assert count > 1000, f"Expected >1000 codes, got {count}"
            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'anzsco_2022'"
            )
            assert row is not None
            assert row["node_count"] == count
            # Check major groups are present
            major = await conn.fetchrow(
                "SELECT code, title FROM classification_node "
                "WHERE system_id = 'anzsco_2022' AND code = '1'"
            )
            assert major is not None
            assert "Manager" in major["title"]
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_anzsco_2022_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.anzsco_2022 import ingest_anzsco_2022
        async with db_pool.acquire() as conn:
            count1 = await ingest_anzsco_2022(conn)
            count2 = await ingest_anzsco_2022(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
