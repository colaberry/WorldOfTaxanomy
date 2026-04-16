"""Tests for CN 2024 (cn_2024) ingester."""

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


class TestIngestCn2024:

    def test_ingest_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.cn_2024 import ingest_cn_2024

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_cn_2024(conn)
                assert count >= 100, f"Expected >= 100 nodes, got {count}"

                sys = await _get_system(conn, "cn_2024")
                assert sys is not None
                assert sys["name"] == "CN 2024"
                assert sys["node_count"] == count

        _run(_test())

    def test_ingest_creates_nodes(self, db_pool):
        from world_of_taxonomy.ingest.cn_2024 import ingest_cn_2024

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_cn_2024(conn)
                db_count = await _count_nodes(conn, "cn_2024")
                assert db_count == count

        _run(_test())

    def test_idempotent(self, db_pool):
        from world_of_taxonomy.ingest.cn_2024 import ingest_cn_2024

        async def _test():
            async with db_pool.acquire() as conn:
                count1 = await ingest_cn_2024(conn)
                count2 = await ingest_cn_2024(conn)
                db_count = await _count_nodes(conn, "cn_2024")
                assert db_count == count1

        _run(_test())
