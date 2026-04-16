"""Tests for Retail Channel Type domain taxonomy ingester.

RED tests - written before any implementation exists.

Retail channel taxonomy organizes sales channels and store formats:
  Channel Type  (drc_channel*) - brick-and-mortar, e-commerce, omnichannel, direct
  Store Format  (drc_format*)  - department, specialty, discount, convenience, warehouse
  Customer Seg  (drc_cust*)    - consumer, B2B, wholesale club, luxury, value

Source: NRF (National Retail Federation) + Census retail trade categories. Public domain.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_retail_channel import (
    RETAIL_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_retail_channel,
)


class TestDetermineLevel:
    def test_channel_category_is_level_1(self):
        assert _determine_level("drc_channel") == 1

    def test_ecommerce_is_level_2(self):
        assert _determine_level("drc_channel_ecom") == 2

    def test_format_category_is_level_1(self):
        assert _determine_level("drc_format") == 1

    def test_specialty_is_level_2(self):
        assert _determine_level("drc_format_specialty") == 2


class TestDetermineParent:
    def test_channel_category_has_no_parent(self):
        assert _determine_parent("drc_channel") is None

    def test_ecom_parent_is_channel(self):
        assert _determine_parent("drc_channel_ecom") == "drc_channel"

    def test_specialty_parent_is_format(self):
        assert _determine_parent("drc_format_specialty") == "drc_format"


class TestRetailNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(RETAIL_NODES) > 0

    def test_has_channel_category(self):
        codes = [n[0] for n in RETAIL_NODES]
        assert "drc_channel" in codes

    def test_has_format_category(self):
        codes = [n[0] for n in RETAIL_NODES]
        assert "drc_format" in codes

    def test_has_ecommerce(self):
        codes = [n[0] for n in RETAIL_NODES]
        assert "drc_channel_ecom" in codes

    def test_has_brick_and_mortar(self):
        codes = [n[0] for n in RETAIL_NODES]
        assert "drc_channel_bam" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in RETAIL_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in RETAIL_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in RETAIL_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in RETAIL_NODES:
            if level == 2:
                assert parent is not None


def test_domain_retail_channel_module_importable():
    assert callable(ingest_domain_retail_channel)
    assert isinstance(RETAIL_NODES, list)


def test_ingest_domain_retail_channel(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_retail_channel(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_retail_channel'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_retail_channel_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_retail_channel(conn)
            count2 = await ingest_domain_retail_channel(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
