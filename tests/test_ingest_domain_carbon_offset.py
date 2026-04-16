"""Tests for Carbon Offset Protocol Types ingester."""
import asyncio
from world_of_taxonomy.ingest.domain_carbon_offset import NODES, ingest_domain_carbon_offset


class TestDomainCarbonOffsetNodes:
    def test_total_node_count(self):
        assert len(NODES) == 14

    def test_all_titles_non_empty(self):
        for code, title, level, parent in NODES:
            assert title, f"Empty title for {code}"

    def test_no_duplicate_codes(self):
        codes = [c for c, *_ in NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_nodes_have_no_parent(self):
        for code, title, level, parent in NODES:
            if level == 1:
                assert parent is None, f"{code} level-1 has parent"

    def test_level_2_plus_have_parent(self):
        for code, title, level, parent in NODES:
            if level >= 2:
                assert parent is not None, f"{code} level-{level} missing parent"

    def test_parent_references_valid(self):
        codes = {c for c, *_ in NODES}
        for code, title, level, parent in NODES:
            if parent is not None:
                assert parent in codes, f"{code} parent {parent} not in codes"

    def test_has_level_1_nodes(self):
        assert any(level == 1 for _, _, level, _ in NODES)


def test_domain_carbon_offset_module_importable():
    from world_of_taxonomy.ingest.domain_carbon_offset import ingest_domain_carbon_offset as fn
    assert callable(fn)


def test_ingest_domain_carbon_offset(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_carbon_offset(conn)
            assert count == 14
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_carbon_offset_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            c1 = await ingest_domain_carbon_offset(conn)
            c2 = await ingest_domain_carbon_offset(conn)
            assert c1 == c2
    asyncio.get_event_loop().run_until_complete(_run())
