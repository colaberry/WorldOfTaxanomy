"""Tests for domain_insurance_claims ingester."""
import asyncio
import pytest
from world_of_taxonomy.ingest.domain_insurance_claims import NODES, ingest_domain_insurance_claims


class TestDomainInsuranceClaimsNodes:
    """Structural tests on the NODES list."""

    def test_total_node_count(self):
        assert len(NODES) == 17

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NODES:
            assert title, f"empty title for {code}"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_nodes_have_no_parent(self):
        for code, title, level, parent in NODES:
            if level == 1:
                assert parent is None, f"{code} is level 1 but has parent"

    def test_level_2_plus_have_parent(self):
        for code, title, level, parent in NODES:
            if level >= 2:
                assert parent is not None, f"{code} is level {level} but has no parent"

    def test_parent_references_valid(self):
        codes = {n[0] for n in NODES}
        for code, title, level, parent in NODES:
            if parent is not None:
                assert parent in codes, f"{code} references missing parent {parent}"

    def test_has_level_1_nodes(self):
        assert any(n[2] == 1 for n in NODES)


def test_domain_insurance_claims_module_importable():
    assert callable(ingest_domain_insurance_claims)


def test_ingest_domain_insurance_claims(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_insurance_claims(conn)
            assert count == 17
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_insurance_claims_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            await ingest_domain_insurance_claims(conn)
            count = await ingest_domain_insurance_claims(conn)
            assert count == 17
    asyncio.get_event_loop().run_until_complete(_run())
