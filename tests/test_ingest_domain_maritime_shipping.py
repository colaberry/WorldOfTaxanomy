"""Tests for Maritime Shipping domain taxonomy ingester.

Maritime Shipping taxonomy organizes maritime shipping sector types:
  Container Shipping (dms_container*) - liner, feeder, reefer, intermodal
  Bulk Cargo         (dms_bulk*)      - dry bulk, breakbulk, RoRo, heavy-lift
  Tanker Operations  (dms_tanker*)    - crude, product, LNG, bunkering
  Port Services      (dms_port*)      - terminal, stevedoring, pilotage
  Shipbuilding       (dms_shipbuild*) - commercial, naval, repair, offshore

Source: NAICS 4831/4832 maritime shipping structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_maritime_shipping import (
    NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_maritime_shipping,
)


class TestDetermineLevel:
    def test_container_category_is_level_1(self):
        assert _determine_level("dms_container") == 1

    def test_container_type_is_level_2(self):
        assert _determine_level("dms_container_liner") == 2

    def test_bulk_category_is_level_1(self):
        assert _determine_level("dms_bulk") == 1

    def test_bulk_type_is_level_2(self):
        assert _determine_level("dms_bulk_dry") == 2

    def test_tanker_category_is_level_1(self):
        assert _determine_level("dms_tanker") == 1

    def test_tanker_type_is_level_2(self):
        assert _determine_level("dms_tanker_crude") == 2


class TestDetermineParent:
    def test_container_category_has_no_parent(self):
        assert _determine_parent("dms_container") is None

    def test_container_liner_parent_is_container(self):
        assert _determine_parent("dms_container_liner") == "dms_container"

    def test_bulk_dry_parent_is_bulk(self):
        assert _determine_parent("dms_bulk_dry") == "dms_bulk"

    def test_port_terminal_parent_is_port(self):
        assert _determine_parent("dms_port_terminal") == "dms_port"


class TestNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(NODES) > 0

    def test_minimum_node_count(self):
        assert len(NODES) >= 20

    def test_has_container_category(self):
        codes = [n[0] for n in NODES]
        assert "dms_container" in codes

    def test_has_bulk_category(self):
        codes = [n[0] for n in NODES]
        assert "dms_bulk" in codes

    def test_has_shipbuild_category(self):
        codes = [n[0] for n in NODES]
        assert "dms_shipbuild" in codes

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


def test_domain_maritime_shipping_module_importable():
    assert callable(ingest_domain_maritime_shipping)
    assert isinstance(NODES, list)


def test_ingest_domain_maritime_shipping(db_pool):
    """Integration test: maritime shipping taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_maritime_shipping(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_maritime_shipping'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_maritime_shipping_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_maritime_shipping(conn)
            count2 = await ingest_domain_maritime_shipping(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
