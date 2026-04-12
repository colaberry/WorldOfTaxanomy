"""Tests for CPC v2.1 Central Product Classification ingester.

RED tests - written before any implementation exists.

Hierarchy (all determined by code length):
  L1 - Section (1-digit, e.g. "0" = Agriculture, forestry and fishery products)
  L2 - Division (2-digit, e.g. "01" = Products of agriculture)
  L3 - Group (3-digit, e.g. "011" = Cereals)
  L4 - Class (4-digit, e.g. "0111" = Wheat)
  L5 - Subclass (5-digit, leaf, e.g. "01111" = Wheat, seed)

Source: unstats.un.org/unsd/classifications (open)
"""
import pytest
from world_of_taxanomy.ingest.cpc_v21 import (
    _determine_level,
    _determine_parent,
    _determine_sector,
    ingest_cpc_v21,
)


class TestCpcV21DetermineLevel:
    def test_1digit_is_section_level_1(self):
        assert _determine_level("0") == 1

    def test_1digit_9_is_level_1(self):
        assert _determine_level("9") == 1

    def test_2digit_is_division_level_2(self):
        assert _determine_level("01") == 2

    def test_3digit_is_group_level_3(self):
        assert _determine_level("011") == 3

    def test_4digit_is_class_level_4(self):
        assert _determine_level("0111") == 4

    def test_5digit_is_subclass_level_5(self):
        assert _determine_level("01111") == 5


class TestCpcV21DetermineParent:
    def test_section_has_no_parent(self):
        assert _determine_parent("0") is None

    def test_division_parent_is_section(self):
        assert _determine_parent("01") == "0"

    def test_group_parent_is_division(self):
        assert _determine_parent("011") == "01"

    def test_class_parent_is_group(self):
        assert _determine_parent("0111") == "011"

    def test_subclass_parent_is_class(self):
        assert _determine_parent("01111") == "0111"

    def test_section_9_has_no_parent(self):
        assert _determine_parent("9") is None


class TestCpcV21DetermineSector:
    def test_section_sector_is_itself(self):
        assert _determine_sector("0") == "0"

    def test_division_sector_is_first_digit(self):
        assert _determine_sector("01") == "0"

    def test_group_sector_is_first_digit(self):
        assert _determine_sector("011") == "0"

    def test_class_sector_is_first_digit(self):
        assert _determine_sector("0111") == "0"

    def test_subclass_sector_is_first_digit(self):
        assert _determine_sector("01111") == "0"

    def test_section_9_sector_is_9(self):
        assert _determine_sector("9") == "9"


def test_ingest_cpc_v21_from_file(db_pool):
    """Integration test - ingest from downloaded file."""
    import asyncio
    from pathlib import Path

    data_path = Path("data/cpc_v21.txt")
    if not data_path.exists():
        pytest.skip(f"Download {data_path} first: see world_of_taxanomy/ingest/cpc_v21.py for URL")

    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_cpc_v21(conn, path=str(data_path))
            assert count >= 4000, f"Expected >= 4000 nodes, got {count}"

            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'cpc_v21'"
            )
            assert row is not None
            assert row["node_count"] == count

            # Section "0" at level 1 with no parent
            sec = await conn.fetchrow(
                "SELECT code, level, parent_code FROM classification_node "
                "WHERE system_id = 'cpc_v21' AND code = '0'"
            )
            assert sec is not None
            assert sec["level"] == 1
            assert sec["parent_code"] is None

            # Division "01" at level 2, parent "0"
            div = await conn.fetchrow(
                "SELECT code, level, parent_code FROM classification_node "
                "WHERE system_id = 'cpc_v21' AND code = '01'"
            )
            assert div is not None
            assert div["level"] == 2
            assert div["parent_code"] == "0"

            # Subclass "01111" at level 5, parent "0111", is_leaf
            sub = await conn.fetchrow(
                "SELECT code, level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'cpc_v21' AND code = '01111'"
            )
            assert sub is not None
            assert sub["level"] == 5
            assert sub["parent_code"] == "0111"
            assert sub["is_leaf"] is True

    asyncio.get_event_loop().run_until_complete(_run())
