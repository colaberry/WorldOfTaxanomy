"""Tests for arXiv taxonomy (arxiv_taxonomy) ingester."""

import asyncio
import pytest


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


async def _get_system(conn, system_id: str):
    return await conn.fetchrow(
        "SELECT * FROM classification_system WHERE id = $1", system_id
    )


async def _count_nodes(conn, system_id: str) -> int:
    row = await conn.fetchrow(
        "SELECT count(*) AS cnt FROM classification_node WHERE system_id = $1",
        system_id,
    )
    return row["cnt"]


class TestIngestArxivTaxonomy:

    def test_ingest_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.arxiv_taxonomy import ingest_arxiv_taxonomy

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_arxiv_taxonomy(conn)
                assert count >= 160, f"Expected >= 160 nodes, got {count}"

                sys = await _get_system(conn, "arxiv_taxonomy")
                assert sys is not None
                assert sys["authority"] == "Cornell University / arXiv.org"
                assert sys["node_count"] == count

        _run(_test())

    def test_ingest_creates_nodes(self, db_pool):
        from world_of_taxonomy.ingest.arxiv_taxonomy import ingest_arxiv_taxonomy

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_arxiv_taxonomy(conn)
                db_count = await _count_nodes(conn, "arxiv_taxonomy")
                assert db_count == count

        _run(_test())

    def test_is_leaf_flags(self, db_pool):
        from world_of_taxonomy.ingest.arxiv_taxonomy import ingest_arxiv_taxonomy

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_arxiv_taxonomy(conn)
                # cs has children (cs.AI, etc.) - must NOT be a leaf
                cs = await conn.fetchrow(
                    "SELECT is_leaf FROM classification_node "
                    "WHERE system_id = 'arxiv_taxonomy' AND code = 'cs'"
                )
                assert cs["is_leaf"] is False, "cs has children - should not be a leaf"

                # hep-ph has no children - must be a leaf
                hep_ph = await conn.fetchrow(
                    "SELECT is_leaf FROM classification_node "
                    "WHERE system_id = 'arxiv_taxonomy' AND code = 'hep-ph'"
                )
                assert hep_ph["is_leaf"] is True, "hep-ph has no children - should be a leaf"

                # cs.AI is level-2 leaf
                cs_ai = await conn.fetchrow(
                    "SELECT is_leaf FROM classification_node "
                    "WHERE system_id = 'arxiv_taxonomy' AND code = 'cs.AI'"
                )
                assert cs_ai["is_leaf"] is True, "cs.AI is a leaf node"

        _run(_test())

    def test_idempotent(self, db_pool):
        from world_of_taxonomy.ingest.arxiv_taxonomy import ingest_arxiv_taxonomy

        async def _test():
            async with db_pool.acquire() as conn:
                count1 = await ingest_arxiv_taxonomy(conn)
                count2 = await ingest_arxiv_taxonomy(conn)
                db_count = await _count_nodes(conn, "arxiv_taxonomy")
                assert db_count == count1

        _run(_test())
