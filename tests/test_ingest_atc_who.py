"""Tests for ATC WHO Drug Classification ingester.

RED tests - written before any implementation exists.

ATC (Anatomical Therapeutic Chemical) classification, WHO 2021.
Source: github.com/fabkury/atcd (CC BY 4.0)
  https://raw.githubusercontent.com/fabkury/atcd/master/WHO%20ATC-DDD%202021-12-03.csv

Hierarchy (level determined by code length):
  L1 - Anatomical main group    (1-char,  e.g. "A")
  L2 - Therapeutic subgroup     (3-char,  e.g. "A01")
  L3 - Pharmacological subgroup (4-char,  e.g. "A01A")
  L4 - Chemical subgroup        (5-char,  e.g. "A01AA")
  L5 - Chemical substance       (7-char,  leaf, e.g. "A01AA01")

14 + 94 + 269 + 910 + 5684 = 6971 nodes.
"""
import asyncio
import pytest
from pathlib import Path

from world_of_taxanomy.ingest.atc_who import (
    _determine_level,
    _determine_parent,
    _determine_sector,
    ingest_atc_who,
)


class TestAtcWhoDetermineLevel:
    def test_1char_is_level_1(self):
        assert _determine_level("A") == 1

    def test_another_1char_is_level_1(self):
        assert _determine_level("N") == 1

    def test_3char_is_level_2(self):
        assert _determine_level("A01") == 2

    def test_another_3char_is_level_2(self):
        assert _determine_level("N02") == 2

    def test_4char_is_level_3(self):
        assert _determine_level("A01A") == 3

    def test_5char_is_level_4(self):
        assert _determine_level("A01AA") == 4

    def test_7char_is_level_5(self):
        assert _determine_level("A01AA01") == 5

    def test_another_7char_is_level_5(self):
        assert _determine_level("N02AA01") == 5


class TestAtcWhoDetermineParent:
    def test_level1_has_no_parent(self):
        assert _determine_parent("A") is None

    def test_level2_parent_is_level1(self):
        assert _determine_parent("A01") == "A"

    def test_another_level2_parent(self):
        assert _determine_parent("N02") == "N"

    def test_level3_parent_is_level2(self):
        assert _determine_parent("A01A") == "A01"

    def test_level4_parent_is_level3(self):
        assert _determine_parent("A01AA") == "A01A"

    def test_level5_parent_is_level4(self):
        assert _determine_parent("A01AA01") == "A01AA"

    def test_another_level5_parent(self):
        assert _determine_parent("N02AA01") == "N02AA"


class TestAtcWhoDetermineSector:
    def test_level1_sector_is_itself(self):
        assert _determine_sector("A") == "A"

    def test_level2_sector_is_level1(self):
        assert _determine_sector("A01") == "A"

    def test_level3_sector_is_level1(self):
        assert _determine_sector("A01A") == "A"

    def test_level4_sector_is_level1(self):
        assert _determine_sector("A01AA") == "A"

    def test_level5_sector_is_level1(self):
        assert _determine_sector("A01AA01") == "A"

    def test_different_sector(self):
        assert _determine_sector("N02AA01") == "N"


def test_atc_who_module_importable():
    """All public symbols are importable."""
    assert callable(ingest_atc_who)
    assert callable(_determine_level)
    assert callable(_determine_parent)
    assert callable(_determine_sector)


def test_ingest_atc_who_from_file(db_pool):
    """Integration test - ingest from downloaded CSV."""
    data_path = Path("data/atc_who.csv")
    if not data_path.exists():
        pytest.skip(
            "Download data/atc_who.csv first: "
            "https://raw.githubusercontent.com/fabkury/atcd/master/WHO%20ATC-DDD%202021-12-03.csv"
        )

    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_atc_who(conn, path=str(data_path))
            # 14 + 94 + 269 + 910 + 5684 = 6971
            assert count >= 6900, f"Expected >= 6900 nodes, got {count}"

            row = await conn.fetchrow(
                "SELECT id, node_count FROM classification_system WHERE id = 'atc_who'"
            )
            assert row is not None
            assert row["node_count"] == count

            # Level 1: "A" - Alimentary Tract and Metabolism, no parent
            a = await conn.fetchrow(
                "SELECT code, level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'atc_who' AND code = 'A'"
            )
            assert a is not None
            assert a["level"] == 1
            assert a["parent_code"] is None
            assert a["is_leaf"] is False

            # Level 2: "A01" parent="A"
            a01 = await conn.fetchrow(
                "SELECT level, parent_code FROM classification_node "
                "WHERE system_id = 'atc_who' AND code = 'A01'"
            )
            assert a01 is not None
            assert a01["level"] == 2
            assert a01["parent_code"] == "A"

            # Level 5: "A01AA01" is leaf
            leaf = await conn.fetchrow(
                "SELECT level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'atc_who' AND code = 'A01AA01'"
            )
            assert leaf is not None
            assert leaf["level"] == 5
            assert leaf["parent_code"] == "A01AA"
            assert leaf["is_leaf"] is True

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_atc_who_idempotent(db_pool):
    """Running ingest twice returns consistent count."""
    data_path = Path("data/atc_who.csv")
    if not data_path.exists():
        pytest.skip("Download data/atc_who.csv first")

    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_atc_who(conn, path=str(data_path))
            count2 = await ingest_atc_who(conn, path=str(data_path))
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
