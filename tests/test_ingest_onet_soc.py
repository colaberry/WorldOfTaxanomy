"""Tests for O*NET-SOC ingester.

RED tests - written before any implementation exists.

O*NET-SOC is the Occupational Information Network, published by the
US Department of Labor. It extends SOC 2018 with detailed occupational
information. O*NET codes use the format 'XX-XXXX.XX' (SOC code + decimal suffix).

We store only the ~867 base occupations (codes ending in '.00') that map
1:1 with SOC 2018 detailed occupations. O*NET-specific sub-occupations
(e.g., '11-1011.03') are excluded.

Structure:
  ~867 occupation nodes
  code = O*NET-SOC code (e.g., '11-1011.00')
  title = occupation title
  level = 1 (flat - all are detailed occupations)
  parent_code = None (crosswalk in Phase 7-E handles SOC relationship)
  sector_code = SOC major group prefix (e.g., '11' from '11-1011.00')
  is_leaf = True (all leaves)

Source: O*NET Database 29.0
  https://www.onetcenter.org/dl_files/database/db_29_0_text/Occupation%20Data.txt
License: CC BY 4.0
"""
import asyncio
import os
import pytest

from world_of_taxonomy.ingest.onet_soc import (
    _determine_sector,
    _is_base_occupation,
    ingest_onet_soc,
)

_DATA_PATH = "data/onet_occupation_data.txt"


class TestDetermineSector:
    def test_management_prefix_gives_11(self):
        assert _determine_sector("11-1011.00") == "11"

    def test_business_prefix_gives_13(self):
        assert _determine_sector("13-1051.00") == "13"

    def test_computer_prefix_gives_15(self):
        assert _determine_sector("15-1251.00") == "15"

    def test_healthcare_prefix_gives_29(self):
        assert _determine_sector("29-1141.00") == "29"

    def test_transportation_prefix_gives_53(self):
        assert _determine_sector("53-3031.00") == "53"


class TestIsBaseOccupation:
    def test_dot_zero_zero_is_base(self):
        assert _is_base_occupation("11-1011.00") is True

    def test_dot_zero_three_is_not_base(self):
        assert _is_base_occupation("11-1011.03") is False

    def test_dot_zero_one_is_not_base(self):
        assert _is_base_occupation("11-1011.01") is False

    def test_no_decimal_is_not_base(self):
        # SOC codes without decimal (e.g., from SOC CSV) are not O*NET codes
        assert _is_base_occupation("11-1011") is False

    def test_dot_zero_two_is_not_base(self):
        assert _is_base_occupation("13-1051.02") is False


def test_onet_soc_module_importable():
    assert callable(ingest_onet_soc)
    assert callable(_determine_sector)
    assert callable(_is_base_occupation)


@pytest.mark.skipif(
    not os.path.exists(_DATA_PATH),
    reason=f"O*NET Occupation Data not found at {_DATA_PATH}. "
           "Run: python -m world_of_taxonomy ingest onet_soc",
)
def test_ingest_onet_soc_from_real_file(db_pool):
    """Integration test: ingest O*NET-SOC from downloaded data file."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_onet_soc(conn, path=_DATA_PATH)
            assert count >= 800, f"Expected >= 800 O*NET-SOC occupations, got {count}"
            assert count <= 1000, f"Expected <= 1000 O*NET-SOC occupations, got {count}"

            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system "
                "WHERE id = 'onet_soc'"
            )
            assert row is not None
            assert row["node_count"] == count

            # All nodes should be level=1, parent=None, is_leaf=True
            sample = await conn.fetchrow(
                "SELECT level, parent_code, is_leaf, sector_code "
                "FROM classification_node "
                "WHERE system_id = 'onet_soc' "
                "LIMIT 1"
            )
            assert sample is not None
            assert sample["level"] == 1
            assert sample["parent_code"] is None
            assert sample["is_leaf"] is True
            # sector_code should be a 2-digit major group prefix
            assert len(sample["sector_code"]) == 2

    asyncio.get_event_loop().run_until_complete(_run())


@pytest.mark.skipif(
    not os.path.exists(_DATA_PATH),
    reason=f"O*NET Occupation Data not found at {_DATA_PATH}.",
)
def test_ingest_onet_soc_idempotent(db_pool):
    """Running ingest twice returns the same count."""
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_onet_soc(conn, path=_DATA_PATH)
            count2 = await ingest_onet_soc(conn, path=_DATA_PATH)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
