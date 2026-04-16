"""Tests for MSC 2020 (msc_2020) ingester."""

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


class TestIngestMsc2020:

    def test_ingest_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.msc_2020 import ingest_msc_2020

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_msc_2020(conn)
                assert count >= 80, f"Expected >= 80 nodes, got {count}"

                sys = await _get_system(conn, "msc_2020")
                assert sys is not None
                assert sys["authority"] == "American Mathematical Society (AMS) / zbMATH"
                assert sys["node_count"] == count

        _run(_test())

    def test_ingest_creates_nodes(self, db_pool):
        from world_of_taxonomy.ingest.msc_2020 import ingest_msc_2020

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_msc_2020(conn)
                db_count = await _count_nodes(conn, "msc_2020")
                assert db_count == count

        _run(_test())

    def test_idempotent(self, db_pool):
        from world_of_taxonomy.ingest.msc_2020 import ingest_msc_2020

        async def _test():
            async with db_pool.acquire() as conn:
                count1 = await ingest_msc_2020(conn)
                count2 = await ingest_msc_2020(conn)
                db_count = await _count_nodes(conn, "msc_2020")
                assert db_count == count1

        _run(_test())
