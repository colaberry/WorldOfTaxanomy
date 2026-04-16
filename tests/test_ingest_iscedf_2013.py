"""Tests for ISCED-F 2013 (Fields of Education and Training) ingester.

RED tests - written before any implementation exists.

Hierarchy (level determined by code length):
  L1 - Broad field   (2-digit, e.g. "01" = Education)
  L2 - Narrow field  (3-digit, e.g. "011" = Education)
  L3 - Detailed field (4-digit, leaf, e.g. "0111" = Education science)

Source: united-education/isced GitHub (JSON, public domain)
  https://raw.githubusercontent.com/united-education/isced/master/isced.json
"""
import asyncio
import pytest
from pathlib import Path

from world_of_taxonomy.ingest.iscedf_2013 import (
    _determine_level,
    _determine_parent,
    _determine_sector,
    ingest_iscedf_2013,
)


class TestIscedf2013DetermineLevel:
    def test_2digit_is_broad_level_1(self):
        assert _determine_level("00") == 1

    def test_another_2digit_is_level_1(self):
        assert _determine_level("07") == 1

    def test_3digit_is_narrow_level_2(self):
        assert _determine_level("001") == 2

    def test_another_3digit_is_level_2(self):
        assert _determine_level("071") == 2

    def test_4digit_is_detailed_level_3(self):
        assert _determine_level("0011") == 3

    def test_another_4digit_is_level_3(self):
        assert _determine_level("0711") == 3


class TestIscedf2013DetermineParent:
    def test_broad_has_no_parent(self):
        assert _determine_parent("00") is None

    def test_another_broad_has_no_parent(self):
        assert _determine_parent("10") is None

    def test_narrow_parent_is_broad(self):
        assert _determine_parent("001") == "00"

    def test_another_narrow_parent_is_broad(self):
        assert _determine_parent("071") == "07"

    def test_detailed_parent_is_narrow(self):
        assert _determine_parent("0011") == "001"

    def test_another_detailed_parent_is_narrow(self):
        assert _determine_parent("0711") == "071"


class TestIscedf2013DetermineSector:
    def test_broad_sector_is_itself(self):
        assert _determine_sector("00") == "00"

    def test_narrow_sector_is_broad(self):
        assert _determine_sector("001") == "00"

    def test_detailed_sector_is_broad(self):
        assert _determine_sector("0011") == "00"

    def test_different_broad(self):
        assert _determine_sector("0711") == "07"


def test_iscedf_2013_module_importable():
    """All public symbols are importable."""
    assert callable(ingest_iscedf_2013)
    assert callable(_determine_level)
    assert callable(_determine_parent)
    assert callable(_determine_sector)


def test_ingest_iscedf_2013_from_file(db_pool):
    """Integration test - ingest from downloaded JSON."""
    data_path = Path("data/iscedf_2013.json")
    if not data_path.exists():
        pytest.skip(
            "Download data/iscedf_2013.json first: "
            "https://raw.githubusercontent.com/united-education/isced/master/isced.json"
        )

    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_iscedf_2013(conn, path=str(data_path))
            # 11 broad + 30 narrow + 81 detailed = 122
            assert count >= 110, f"Expected >= 110 nodes, got {count}"

            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'iscedf_2013'"
            )
            assert row is not None
            assert row["node_count"] == count

            # Broad "01" at level 1, no parent
            broad = await conn.fetchrow(
                "SELECT code, level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'iscedf_2013' AND code = '01'"
            )
            assert broad is not None
            assert broad["level"] == 1
            assert broad["parent_code"] is None
            assert broad["is_leaf"] is False

            # Narrow "011" at level 2, parent "01"
            narrow = await conn.fetchrow(
                "SELECT code, level, parent_code FROM classification_node "
                "WHERE system_id = 'iscedf_2013' AND code = '011'"
            )
            assert narrow is not None
            assert narrow["level"] == 2
            assert narrow["parent_code"] == "01"

            # Detailed "0111" at level 3, parent "011", is_leaf
            detail = await conn.fetchrow(
                "SELECT code, level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'iscedf_2013' AND code = '0111'"
            )
            assert detail is not None
            assert detail["level"] == 3
            assert detail["parent_code"] == "011"
            assert detail["is_leaf"] is True

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_iscedf_2013_idempotent(db_pool):
    """Running ingest twice returns consistent count."""
    data_path = Path("data/iscedf_2013.json")
    if not data_path.exists():
        pytest.skip("Download data/iscedf_2013.json first")

    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_iscedf_2013(conn, path=str(data_path))
            count2 = await ingest_iscedf_2013(conn, path=str(data_path))
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
