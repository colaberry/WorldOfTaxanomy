"""Tests for Biotechnology and Genomics domain taxonomy ingester.

RED tests - written before any implementation exists.

Biotech taxonomy organizes biotechnology and genomics sector types:
  Drug Discovery    (dbt_drug*)    - target ID, screening, lead optimization
  Biomanufacturing  (dbt_mfg*)     - upstream, downstream, fill-finish
  Genomics          (dbt_gen*)     - sequencing, assembly, annotation
  Cell/Gene Therapy (dbt_cgt*)     - CAR-T, gene editing, viral vectors
  Diagnostics       (dbt_diag*)    - molecular, immunoassay, POC
  Ag-Biotech        (dbt_ag*)      - GM crops, biopesticides, soil biotech
  Industrial Biotech (dbt_ind*)    - biofuels, bioenzymes, biosolvents

Source: NAICS 5417 + 3254 + 3391 biotech industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_biotech import (
    BIOTECH_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_biotech,
)


class TestDetermineLevel:
    def test_drug_category_is_level_1(self):
        assert _determine_level("dbt_drug") == 1

    def test_drug_type_is_level_2(self):
        assert _determine_level("dbt_drug_target") == 2

    def test_gen_category_is_level_1(self):
        assert _determine_level("dbt_gen") == 1

    def test_gen_type_is_level_2(self):
        assert _determine_level("dbt_gen_seq") == 2

    def test_cgt_category_is_level_1(self):
        assert _determine_level("dbt_cgt") == 1

    def test_cgt_type_is_level_2(self):
        assert _determine_level("dbt_cgt_cart") == 2


class TestDetermineParent:
    def test_drug_category_has_no_parent(self):
        assert _determine_parent("dbt_drug") is None

    def test_drug_target_parent_is_drug(self):
        assert _determine_parent("dbt_drug_target") == "dbt_drug"

    def test_gen_seq_parent_is_gen(self):
        assert _determine_parent("dbt_gen_seq") == "dbt_gen"

    def test_cgt_cart_parent_is_cgt(self):
        assert _determine_parent("dbt_cgt_cart") == "dbt_cgt"


class TestBiotechNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(BIOTECH_NODES) > 0

    def test_has_drug_discovery_category(self):
        codes = [n[0] for n in BIOTECH_NODES]
        assert "dbt_drug" in codes

    def test_has_genomics_category(self):
        codes = [n[0] for n in BIOTECH_NODES]
        assert "dbt_gen" in codes

    def test_has_diagnostics_category(self):
        codes = [n[0] for n in BIOTECH_NODES]
        assert "dbt_diag" in codes

    def test_has_cell_gene_therapy_category(self):
        codes = [n[0] for n in BIOTECH_NODES]
        assert "dbt_cgt" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in BIOTECH_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in BIOTECH_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in BIOTECH_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in BIOTECH_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(BIOTECH_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in BIOTECH_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_biotech_module_importable():
    assert callable(ingest_domain_biotech)
    assert isinstance(BIOTECH_NODES, list)


def test_ingest_domain_biotech(db_pool):
    """Integration test: biotech taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_biotech(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_biotech'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_biotech'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_biotech_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_biotech(conn)
            count2 = await ingest_domain_biotech(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
