"""Tests for Synthetic Biology domain taxonomy ingester.

RED tests - written before any implementation exists.

Synthetic biology taxonomy organizes synbio sector types:
  Metabolic Eng.    (dsb_meta*)    - pathway engineering, strain development, optimization
  Cell-Free Systems (dsb_cfs*)     - TXTL, enzymatic synthesis, cell-free manufacturing
  DNA Synthesis     (dsb_dna*)     - oligonucleotide synthesis, gene assembly, genome writing
  CRISPR/Editing    (dsb_crispr*)  - CRISPR-Cas9, base editing, prime editing, CRISPRi/a
  Chassis Organisms (dsb_chassis*) - E. coli, yeast, CHO, B. subtilis platforms
  Bioproducts       (dsb_prod*)    - biofuels, biochemicals, cultured meat, bioplastics
  Biosensors        (dsb_sensor*)  - cell-based sensors, reporter systems, diagnostics

Source: NAICS 5417 + 3254 synthetic biology industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_synbio import (
    SYNBIO_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_synbio,
)


class TestDetermineLevel:
    def test_meta_category_is_level_1(self):
        assert _determine_level("dsb_meta") == 1

    def test_meta_type_is_level_2(self):
        assert _determine_level("dsb_meta_pathway") == 2

    def test_crispr_category_is_level_1(self):
        assert _determine_level("dsb_crispr") == 1

    def test_crispr_type_is_level_2(self):
        assert _determine_level("dsb_crispr_cas9") == 2

    def test_prod_category_is_level_1(self):
        assert _determine_level("dsb_prod") == 1

    def test_prod_type_is_level_2(self):
        assert _determine_level("dsb_prod_biofuel") == 2


class TestDetermineParent:
    def test_meta_category_has_no_parent(self):
        assert _determine_parent("dsb_meta") is None

    def test_meta_pathway_parent_is_meta(self):
        assert _determine_parent("dsb_meta_pathway") == "dsb_meta"

    def test_crispr_cas9_parent_is_crispr(self):
        assert _determine_parent("dsb_crispr_cas9") == "dsb_crispr"

    def test_prod_biofuel_parent_is_prod(self):
        assert _determine_parent("dsb_prod_biofuel") == "dsb_prod"


class TestSynbioNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(SYNBIO_NODES) > 0

    def test_has_metabolic_engineering_category(self):
        codes = [n[0] for n in SYNBIO_NODES]
        assert "dsb_meta" in codes

    def test_has_crispr_category(self):
        codes = [n[0] for n in SYNBIO_NODES]
        assert "dsb_crispr" in codes

    def test_has_dna_synthesis_category(self):
        codes = [n[0] for n in SYNBIO_NODES]
        assert "dsb_dna" in codes

    def test_has_bioproducts_category(self):
        codes = [n[0] for n in SYNBIO_NODES]
        assert "dsb_prod" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in SYNBIO_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in SYNBIO_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in SYNBIO_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in SYNBIO_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(SYNBIO_NODES) >= 18

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in SYNBIO_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_synbio_module_importable():
    assert callable(ingest_domain_synbio)
    assert isinstance(SYNBIO_NODES, list)


def test_ingest_domain_synbio(db_pool):
    """Integration test: synbio taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_synbio(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_synbio'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_synbio'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_synbio_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_synbio(conn)
            count2 = await ingest_domain_synbio(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
