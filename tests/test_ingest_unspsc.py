"""Tests for UNSPSC v24 ingester.

RED tests - written before any implementation exists.

Hierarchy (all codes are 8-digit strings, level determined by trailing zeros):
  L1 - Segment   (ends in 000000, e.g. "10000000")
  L2 - Family    (ends in 0000,   e.g. "10100000")
  L3 - Class     (ends in 00,     e.g. "10101500")
  L4 - Commodity (no trailing 00, e.g. "10101501") - leaf

Source: Oklahoma Open Data Portal (public domain, no auth required)
  https://data.ok.gov/dataset/unspsc-codes
"""
import asyncio
import pytest
from pathlib import Path

from world_of_taxonomy.ingest.unspsc import (
    _determine_level,
    _determine_parent,
    _determine_sector,
    ingest_unspsc,
)


class TestUnspscDetermineLevel:
    def test_segment_is_level_1(self):
        assert _determine_level("10000000") == 1

    def test_another_segment_is_level_1(self):
        assert _determine_level("20000000") == 1

    def test_family_is_level_2(self):
        assert _determine_level("10100000") == 2

    def test_another_family_is_level_2(self):
        assert _determine_level("10200000") == 2

    def test_class_is_level_3(self):
        assert _determine_level("10101500") == 3

    def test_another_class_is_level_3(self):
        assert _determine_level("10102100") == 3

    def test_commodity_is_level_4(self):
        assert _determine_level("10101501") == 4

    def test_another_commodity_is_level_4(self):
        assert _determine_level("10101502") == 4


class TestUnspscDetermineParent:
    def test_segment_has_no_parent(self):
        assert _determine_parent("10000000") is None

    def test_another_segment_has_no_parent(self):
        assert _determine_parent("20000000") is None

    def test_family_parent_is_segment(self):
        assert _determine_parent("10100000") == "10000000"

    def test_another_family_parent_is_segment(self):
        assert _determine_parent("10200000") == "10000000"

    def test_class_parent_is_family(self):
        assert _determine_parent("10101500") == "10100000"

    def test_another_class_parent_is_family(self):
        assert _determine_parent("10102100") == "10100000"

    def test_commodity_parent_is_class(self):
        assert _determine_parent("10101501") == "10101500"

    def test_another_commodity_parent_is_class(self):
        assert _determine_parent("10101502") == "10101500"


class TestUnspscDetermineSector:
    def test_segment_sector_is_itself(self):
        assert _determine_sector("10000000") == "10000000"

    def test_family_sector_is_segment(self):
        assert _determine_sector("10100000") == "10000000"

    def test_class_sector_is_segment(self):
        assert _determine_sector("10101500") == "10000000"

    def test_commodity_sector_is_segment(self):
        assert _determine_sector("10101501") == "10000000"

    def test_different_segment_family(self):
        assert _determine_sector("20101500") == "20000000"


def test_unspsc_module_importable():
    """All public symbols are importable."""
    assert callable(ingest_unspsc)
    assert callable(_determine_level)
    assert callable(_determine_parent)
    assert callable(_determine_sector)


def test_ingest_unspsc_from_file(db_pool):
    """Integration test - ingest from downloaded CSV."""
    data_path = Path("data/unspsc_v24.csv")
    if not data_path.exists():
        pytest.skip(
            "Download data/unspsc_v24.csv first: "
            "https://data.ok.gov/dataset/unspsc-codes"
        )

    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_unspsc(conn, path=str(data_path))
            # 57 segments + 465 families + 5313 classes + 71502 commodities
            assert count >= 77000, f"Expected >= 77000 nodes, got {count}"

            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'unspsc_v24'"
            )
            assert row is not None
            assert row["node_count"] == count

            # Segment "10000000" at level 1, no parent
            seg = await conn.fetchrow(
                "SELECT code, level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'unspsc_v24' AND code = '10000000'"
            )
            assert seg is not None
            assert seg["level"] == 1
            assert seg["parent_code"] is None
            assert seg["is_leaf"] is False

            # Family "10100000" at level 2, parent "10000000"
            fam = await conn.fetchrow(
                "SELECT code, level, parent_code FROM classification_node "
                "WHERE system_id = 'unspsc_v24' AND code = '10100000'"
            )
            assert fam is not None
            assert fam["level"] == 2
            assert fam["parent_code"] == "10000000"

            # Class "10101500" at level 3, parent "10100000"
            cls_ = await conn.fetchrow(
                "SELECT code, level, parent_code FROM classification_node "
                "WHERE system_id = 'unspsc_v24' AND code = '10101500'"
            )
            assert cls_ is not None
            assert cls_["level"] == 3
            assert cls_["parent_code"] == "10100000"

            # Commodity "10101501" (Cats) at level 4, parent "10101500", is_leaf
            com = await conn.fetchrow(
                "SELECT code, level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'unspsc_v24' AND code = '10101501'"
            )
            assert com is not None
            assert com["level"] == 4
            assert com["parent_code"] == "10101500"
            assert com["is_leaf"] is True

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_unspsc_idempotent(db_pool):
    """Running ingest twice does not raise and returns consistent count."""
    data_path = Path("data/unspsc_v24.csv")
    if not data_path.exists():
        pytest.skip("Download data/unspsc_v24.csv first")

    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_unspsc(conn, path=str(data_path))
            count2 = await ingest_unspsc(conn, path=str(data_path))
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
