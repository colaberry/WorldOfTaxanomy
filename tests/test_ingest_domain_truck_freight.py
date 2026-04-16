"""Tests for Truck Freight Type domain taxonomy ingester.

RED tests - written before any implementation exists.

Introduces the domain deep-dive pattern:
  - `domain_taxonomy` table: anchors the taxonomy to a parent industry node
  - `classification_node` with system_id='domain_truck_freight': freight types
  - `node_taxonomy_link`: connects NAICS 484xxx nodes to this domain taxonomy

Freight Type Taxonomy organizes truck freight into 3 top-level categories:
  Mode      (level 1) -> LTL, FTL, Intermodal, Drayage, Expedited, ...
  Equipment (level 1) -> Dry Van, Flatbed, Reefer, Tanker, Lowboy, ...
  Service   (level 1) -> Local/Regional, Long-haul, OTR, Dedicated, ...

All specific freight types are level 2 (leaves).
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_truck_freight import (
    FREIGHT_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_truck_freight,
)


class TestDetermineLevel:
    def test_top_category_is_level_1(self):
        assert _determine_level("dtf_mode") == 1

    def test_specific_type_is_level_2(self):
        assert _determine_level("dtf_mode_ltl") == 2

    def test_equipment_category_is_level_1(self):
        assert _determine_level("dtf_equip") == 1

    def test_equipment_type_is_level_2(self):
        assert _determine_level("dtf_equip_dryvan") == 2

    def test_service_category_is_level_1(self):
        assert _determine_level("dtf_svc") == 1

    def test_service_type_is_level_2(self):
        assert _determine_level("dtf_svc_otr") == 2


class TestDetermineParent:
    def test_top_category_has_no_parent(self):
        assert _determine_parent("dtf_mode") is None

    def test_mode_ltl_parent_is_dtf_mode(self):
        assert _determine_parent("dtf_mode_ltl") == "dtf_mode"

    def test_equip_type_parent_is_equip(self):
        assert _determine_parent("dtf_equip_flatbed") == "dtf_equip"

    def test_service_type_parent_is_svc(self):
        assert _determine_parent("dtf_svc_longhaul") == "dtf_svc"


class TestFreightNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(FREIGHT_NODES) > 0

    def test_has_mode_category(self):
        codes = [n[0] for n in FREIGHT_NODES]
        assert "dtf_mode" in codes

    def test_has_equipment_category(self):
        codes = [n[0] for n in FREIGHT_NODES]
        assert "dtf_equip" in codes

    def test_has_service_category(self):
        codes = [n[0] for n in FREIGHT_NODES]
        assert "dtf_svc" in codes

    def test_ltl_node_present(self):
        codes = [n[0] for n in FREIGHT_NODES]
        assert "dtf_mode_ltl" in codes

    def test_ftl_node_present(self):
        codes = [n[0] for n in FREIGHT_NODES]
        assert "dtf_mode_ftl" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in FREIGHT_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in FREIGHT_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in FREIGHT_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in FREIGHT_NODES:
            if level == 2:
                assert parent is not None


def test_domain_truck_freight_module_importable():
    assert callable(ingest_domain_truck_freight)
    assert isinstance(FREIGHT_NODES, list)


def test_ingest_domain_truck_freight(db_pool):
    """Integration test: domain taxonomy row + freight nodes + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_truck_freight(conn)
            assert count > 0

            # domain_taxonomy row created
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_truck_freight'"
            )
            assert row is not None
            assert row["code_count"] == count

            # classification_node rows under system_id='domain_truck_freight'
            node_count = await conn.fetchval(
                "SELECT COUNT(*) FROM classification_node "
                "WHERE system_id = 'domain_truck_freight'"
            )
            assert node_count == count

            # Level 1 category nodes exist
            cat = await conn.fetchrow(
                "SELECT level, parent_code, is_leaf FROM classification_node "
                "WHERE system_id = 'domain_truck_freight' AND level = 1 LIMIT 1"
            )
            assert cat["level"] == 1
            assert cat["parent_code"] is None
            assert cat["is_leaf"] is False

            # node_taxonomy_link entries connect NAICS 484 to this domain
            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_truck_freight'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_truck_freight_idempotent(db_pool):
    """Running ingest twice returns same count."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_truck_freight(conn)
            count2 = await ingest_domain_truck_freight(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
