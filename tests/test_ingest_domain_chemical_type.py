"""Tests for Chemical Industry Type domain taxonomy ingester.

RED tests - written before any implementation exists.

Chemical taxonomy organizes chemical industry types into categories:
  Petrochemicals    (dch_petro*)   - basic petrochems, aromatics, olefins
  Specialty Chems   (dch_spec*)    - electronic, catalysts, surfactants
  Agrochemicals     (dch_agroc*)   - pesticides, fertilizers, plant growth
  Pharma Interm.    (dch_pharma*)  - APIs, excipients, biologics intermediates
  Industrial Gases  (dch_gas*)     - industrial, specialty, cryogenic
  Polymers          (dch_poly*)    - thermoplastics, thermosets, elastomers
  Coatings          (dch_coat*)    - architectural, industrial, inks
  Other             (dch_clean*, dch_flavor*, dch_explo*)

Source: NAICS 325 + 324 chemical industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_chemical_type import (
    CHEMICAL_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_chemical_type,
)


class TestDetermineLevel:
    def test_petro_category_is_level_1(self):
        assert _determine_level("dch_petro") == 1

    def test_petro_type_is_level_2(self):
        assert _determine_level("dch_petro_basic") == 2

    def test_poly_category_is_level_1(self):
        assert _determine_level("dch_poly") == 1

    def test_poly_type_is_level_2(self):
        assert _determine_level("dch_poly_therm") == 2

    def test_gas_category_is_level_1(self):
        assert _determine_level("dch_gas") == 1

    def test_gas_type_is_level_2(self):
        assert _determine_level("dch_gas_indust") == 2


class TestDetermineParent:
    def test_petro_category_has_no_parent(self):
        assert _determine_parent("dch_petro") is None

    def test_petro_basic_parent_is_petro(self):
        assert _determine_parent("dch_petro_basic") == "dch_petro"

    def test_poly_therm_parent_is_poly(self):
        assert _determine_parent("dch_poly_therm") == "dch_poly"

    def test_agroc_pest_parent_is_agroc(self):
        assert _determine_parent("dch_agroc_pest") == "dch_agroc"


class TestChemicalNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(CHEMICAL_NODES) > 0

    def test_has_petrochemicals_category(self):
        codes = [n[0] for n in CHEMICAL_NODES]
        assert "dch_petro" in codes

    def test_has_polymers_category(self):
        codes = [n[0] for n in CHEMICAL_NODES]
        assert "dch_poly" in codes

    def test_has_agrochemicals_category(self):
        codes = [n[0] for n in CHEMICAL_NODES]
        assert "dch_agroc" in codes

    def test_has_specialty_chemicals_category(self):
        codes = [n[0] for n in CHEMICAL_NODES]
        assert "dch_spec" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in CHEMICAL_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in CHEMICAL_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in CHEMICAL_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in CHEMICAL_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(CHEMICAL_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in CHEMICAL_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_chemical_type_module_importable():
    assert callable(ingest_domain_chemical_type)
    assert isinstance(CHEMICAL_NODES, list)


def test_ingest_domain_chemical_type(db_pool):
    """Integration test: chemical taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_chemical_type(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_chemical_type'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_chemical_type'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_chemical_type_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_chemical_type(conn)
            count2 = await ingest_domain_chemical_type(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
