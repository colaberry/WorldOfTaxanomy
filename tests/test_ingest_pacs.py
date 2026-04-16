"""Tests for PACS (pacs) ingester."""

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


class TestIngestPacs:

    def test_ingest_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.pacs import ingest_pacs

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_pacs(conn)
                assert count >= 60, f"Expected >= 60 nodes, got {count}"

                sys = await _get_system(conn, "pacs")
                assert sys is not None
                assert sys["authority"] == "American Institute of Physics (AIP)"
                assert sys["node_count"] == count

        _run(_test())

    def test_ingest_creates_nodes(self, db_pool):
        from world_of_taxonomy.ingest.pacs import ingest_pacs

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_pacs(conn)
                db_count = await _count_nodes(conn, "pacs")
                assert db_count == count

        _run(_test())

    def test_idempotent(self, db_pool):
        from world_of_taxonomy.ingest.pacs import ingest_pacs

        async def _test():
            async with db_pool.acquire() as conn:
                count1 = await ingest_pacs(conn)
                count2 = await ingest_pacs(conn)
                db_count = await _count_nodes(conn, "pacs")
                assert db_count == count1

        _run(_test())
