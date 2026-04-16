"""Tests for Pet and Animal Services domain taxonomy ingester.

RED tests - written before any implementation exists.

Pet and animal taxonomy organizes types into categories:
  Veterinary Services (dpa_vet*)     - companion, equine, exotic, emergency
  Pet Retail          (dpa_retail*)   - food, supply, pharmacy, e-commerce
  Pet Grooming/Board  (dpa_care*)    - groom, board, daycare, walking
  Animal Feed         (dpa_feed*)    - premium, raw, supplements
  Pet Tech            (dpa_tech*)    - wearables, telehealth, smart, insurance

Source: NAICS 5419 + 4539 pet services/retail. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_pet_animal import (
    PET_ANIMAL_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_pet_animal,
)


class TestDetermineLevel:
    def test_vet_category_is_level_1(self):
        assert _determine_level("dpa_vet") == 1

    def test_vet_type_is_level_2(self):
        assert _determine_level("dpa_vet_companion") == 2

    def test_retail_category_is_level_1(self):
        assert _determine_level("dpa_retail") == 1

    def test_retail_type_is_level_2(self):
        assert _determine_level("dpa_retail_food") == 2

    def test_tech_category_is_level_1(self):
        assert _determine_level("dpa_tech") == 1

    def test_tech_type_is_level_2(self):
        assert _determine_level("dpa_tech_wearable") == 2


class TestDetermineParent:
    def test_vet_category_has_no_parent(self):
        assert _determine_parent("dpa_vet") is None

    def test_vet_companion_parent_is_vet(self):
        assert _determine_parent("dpa_vet_companion") == "dpa_vet"

    def test_care_groom_parent_is_care(self):
        assert _determine_parent("dpa_care_groom") == "dpa_care"

    def test_tech_wearable_parent_is_tech(self):
        assert _determine_parent("dpa_tech_wearable") == "dpa_tech"


class TestPetAnimalNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(PET_ANIMAL_NODES) > 0

    def test_has_veterinary_category(self):
        codes = [n[0] for n in PET_ANIMAL_NODES]
        assert "dpa_vet" in codes

    def test_has_pet_retail_category(self):
        codes = [n[0] for n in PET_ANIMAL_NODES]
        assert "dpa_retail" in codes

    def test_has_grooming_boarding_category(self):
        codes = [n[0] for n in PET_ANIMAL_NODES]
        assert "dpa_care" in codes

    def test_has_pet_tech_category(self):
        codes = [n[0] for n in PET_ANIMAL_NODES]
        assert "dpa_tech" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in PET_ANIMAL_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in PET_ANIMAL_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in PET_ANIMAL_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in PET_ANIMAL_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(PET_ANIMAL_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in PET_ANIMAL_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_pet_animal_module_importable():
    assert callable(ingest_domain_pet_animal)
    assert isinstance(PET_ANIMAL_NODES, list)


def test_ingest_domain_pet_animal(db_pool):
    """Integration test: pet/animal taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_pet_animal(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_pet_animal'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_pet_animal'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_pet_animal_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_pet_animal(conn)
            count2 = await ingest_domain_pet_animal(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
