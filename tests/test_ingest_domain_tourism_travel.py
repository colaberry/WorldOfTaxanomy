"""Tests for Tourism and Travel domain taxonomy ingester.

Tourism and Travel taxonomy organizes tourism and travel sector types:
  Leisure Travel    (dtt_leisure*)   - beach, cruise, family, luxury
  Business Travel   (dtt_business*)  - corporate, conference, incentive
  Adventure Tourism (dtt_adventure*) - eco, extreme, trekking, safari
  Cultural Tourism  (dtt_culture*)   - heritage, food, festival, religious
  Medical Tourism   (dtt_medical*)   - surgery, dental, wellness, rehab

Source: NAICS 5615/7211 travel industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_tourism_travel import (
    NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_tourism_travel,
)


class TestDetermineLevel:
    def test_leisure_category_is_level_1(self):
        assert _determine_level("dtt_leisure") == 1

    def test_leisure_type_is_level_2(self):
        assert _determine_level("dtt_leisure_beach") == 2

    def test_business_category_is_level_1(self):
        assert _determine_level("dtt_business") == 1

    def test_business_type_is_level_2(self):
        assert _determine_level("dtt_business_corp") == 2

    def test_medical_category_is_level_1(self):
        assert _determine_level("dtt_medical") == 1

    def test_medical_type_is_level_2(self):
        assert _determine_level("dtt_medical_surgery") == 2


class TestDetermineParent:
    def test_leisure_category_has_no_parent(self):
        assert _determine_parent("dtt_leisure") is None

    def test_leisure_beach_parent_is_leisure(self):
        assert _determine_parent("dtt_leisure_beach") == "dtt_leisure"

    def test_business_corp_parent_is_business(self):
        assert _determine_parent("dtt_business_corp") == "dtt_business"

    def test_medical_surgery_parent_is_medical(self):
        assert _determine_parent("dtt_medical_surgery") == "dtt_medical"


class TestNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(NODES) > 0

    def test_minimum_node_count(self):
        assert len(NODES) >= 20

    def test_has_leisure_category(self):
        codes = [n[0] for n in NODES]
        assert "dtt_leisure" in codes

    def test_has_business_category(self):
        codes = [n[0] for n in NODES]
        assert "dtt_business" in codes

    def test_has_medical_category(self):
        codes = [n[0] for n in NODES]
        assert "dtt_medical" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in NODES:
            if level == 2:
                assert parent is not None

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_tourism_travel_module_importable():
    assert callable(ingest_domain_tourism_travel)
    assert isinstance(NODES, list)


def test_ingest_domain_tourism_travel(db_pool):
    """Integration test: tourism/travel taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_tourism_travel(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_tourism_travel'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_tourism_travel_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_tourism_travel(conn)
            count2 = await ingest_domain_tourism_travel(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
