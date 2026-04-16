"""Tests for Sports and Recreation domain taxonomy ingester.

RED tests - written before any implementation exists.

Sports and recreation taxonomy organizes types into categories:
  Professional Sports (dsr_pro*)     - team, individual, esports, racing, college
  Fitness and Gym     (dsr_fitness*) - club, boutique, personal, digital
  Outdoor Recreation  (dsr_outdoor*) - camp, water, snow, climb, fishing
  Sports Equipment    (dsr_equip*)   - apparel, gear, tech, facility
  Sports Media        (dsr_media*)   - broadcast, betting, data, sponsor

Source: NAICS 7112 + 7131 + 7139 sports/recreation. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_sports_recreation import (
    SPORTS_RECREATION_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_sports_recreation,
)


class TestDetermineLevel:
    def test_pro_category_is_level_1(self):
        assert _determine_level("dsr_pro") == 1

    def test_pro_type_is_level_2(self):
        assert _determine_level("dsr_pro_team") == 2

    def test_fitness_category_is_level_1(self):
        assert _determine_level("dsr_fitness") == 1

    def test_fitness_type_is_level_2(self):
        assert _determine_level("dsr_fitness_club") == 2

    def test_outdoor_category_is_level_1(self):
        assert _determine_level("dsr_outdoor") == 1

    def test_outdoor_type_is_level_2(self):
        assert _determine_level("dsr_outdoor_camp") == 2


class TestDetermineParent:
    def test_pro_category_has_no_parent(self):
        assert _determine_parent("dsr_pro") is None

    def test_pro_team_parent_is_pro(self):
        assert _determine_parent("dsr_pro_team") == "dsr_pro"

    def test_fitness_club_parent_is_fitness(self):
        assert _determine_parent("dsr_fitness_club") == "dsr_fitness"

    def test_media_broadcast_parent_is_media(self):
        assert _determine_parent("dsr_media_broadcast") == "dsr_media"


class TestSportsRecreationNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(SPORTS_RECREATION_NODES) > 0

    def test_has_professional_sports_category(self):
        codes = [n[0] for n in SPORTS_RECREATION_NODES]
        assert "dsr_pro" in codes

    def test_has_fitness_category(self):
        codes = [n[0] for n in SPORTS_RECREATION_NODES]
        assert "dsr_fitness" in codes

    def test_has_outdoor_recreation_category(self):
        codes = [n[0] for n in SPORTS_RECREATION_NODES]
        assert "dsr_outdoor" in codes

    def test_has_sports_equipment_category(self):
        codes = [n[0] for n in SPORTS_RECREATION_NODES]
        assert "dsr_equip" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in SPORTS_RECREATION_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in SPORTS_RECREATION_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in SPORTS_RECREATION_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in SPORTS_RECREATION_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(SPORTS_RECREATION_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in SPORTS_RECREATION_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_sports_recreation_module_importable():
    assert callable(ingest_domain_sports_recreation)
    assert isinstance(SPORTS_RECREATION_NODES, list)


def test_ingest_domain_sports_recreation(db_pool):
    """Integration test: sports/recreation taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_sports_recreation(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_sports_recreation'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_sports_recreation'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_sports_recreation_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_sports_recreation(conn)
            count2 = await ingest_domain_sports_recreation(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
