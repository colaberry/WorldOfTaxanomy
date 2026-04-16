"""Tests for Defence and Security Type domain taxonomy ingester.

RED tests - written before any implementation exists.

Defence taxonomy organizes defence and security sector types:
  Land Systems      (ddf_land*)    - armored vehicles, artillery, infantry systems
  Naval Systems     (ddf_naval*)   - surface, submarine, amphibious
  Air Systems       (ddf_air*)     - combat, transport, surveillance aircraft
  Cyber & EW        (ddf_cyber*)   - cyber operations, electronic warfare
  Intelligence      (ddf_intel*)   - ISR, signals, imagery
  Missiles/Munitions (ddf_wpn*)    - missiles, guided munitions, ammunition
  Force Protection  (ddf_prot*)    - armor, CBRN, counter-IED
  Logistics/Support (ddf_log*)     - maintenance, supply chain, training

Source: NAICS 928 + 3364 defence industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_defence_type import (
    DEFENCE_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_defence_type,
)


class TestDetermineLevel:
    def test_land_category_is_level_1(self):
        assert _determine_level("ddf_land") == 1

    def test_land_type_is_level_2(self):
        assert _determine_level("ddf_land_armor") == 2

    def test_air_category_is_level_1(self):
        assert _determine_level("ddf_air") == 1

    def test_air_type_is_level_2(self):
        assert _determine_level("ddf_air_combat") == 2

    def test_cyber_category_is_level_1(self):
        assert _determine_level("ddf_cyber") == 1

    def test_cyber_type_is_level_2(self):
        assert _determine_level("ddf_cyber_ops") == 2


class TestDetermineParent:
    def test_land_category_has_no_parent(self):
        assert _determine_parent("ddf_land") is None

    def test_land_armor_parent_is_land(self):
        assert _determine_parent("ddf_land_armor") == "ddf_land"

    def test_naval_sub_parent_is_naval(self):
        assert _determine_parent("ddf_naval_sub") == "ddf_naval"

    def test_wpn_missile_parent_is_wpn(self):
        assert _determine_parent("ddf_wpn_missile") == "ddf_wpn"


class TestDefenceNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(DEFENCE_NODES) > 0

    def test_has_land_systems_category(self):
        codes = [n[0] for n in DEFENCE_NODES]
        assert "ddf_land" in codes

    def test_has_naval_systems_category(self):
        codes = [n[0] for n in DEFENCE_NODES]
        assert "ddf_naval" in codes

    def test_has_air_systems_category(self):
        codes = [n[0] for n in DEFENCE_NODES]
        assert "ddf_air" in codes

    def test_has_cyber_category(self):
        codes = [n[0] for n in DEFENCE_NODES]
        assert "ddf_cyber" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in DEFENCE_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in DEFENCE_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in DEFENCE_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in DEFENCE_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(DEFENCE_NODES) >= 18

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in DEFENCE_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_defence_type_module_importable():
    assert callable(ingest_domain_defence_type)
    assert isinstance(DEFENCE_NODES, list)


def test_ingest_domain_defence_type(db_pool):
    """Integration test: defence taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_defence_type(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_defence_type'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_defence_type'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_defence_type_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_defence_type(conn)
            count2 = await ingest_domain_defence_type(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
