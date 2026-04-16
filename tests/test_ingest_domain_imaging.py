"""Tests for Imaging Modality Types ingester."""
import asyncio
from world_of_taxonomy.ingest.domain_imaging import NODES, ingest_domain_imaging


class TestDomainImagingNodes:
    def test_total_node_count(self):
        assert len(NODES) == 16

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


def test_domain_imaging_module_importable():
    from world_of_taxonomy.ingest.domain_imaging import ingest_domain_imaging as fn
    assert callable(fn)


def test_ingest_domain_imaging(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_imaging(conn)
            assert count == 16
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_imaging_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            c1 = await ingest_domain_imaging(conn)
            c2 = await ingest_domain_imaging(conn)
            assert c1 == c2
    asyncio.get_event_loop().run_until_complete(_run())
