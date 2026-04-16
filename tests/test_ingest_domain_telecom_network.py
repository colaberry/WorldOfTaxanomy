"""Tests for Telecom Network domain taxonomy ingester.

Telecom Network taxonomy organizes network infrastructure types:
  Access Network   (dtn_access*)    - RAN, PON, HFC, FTTx
  Core Network     (dtn_core*)      - 5G core, EPC, IMS, SDN/NFV, signaling
  Transport        (dtn_transport*) - DWDM, OTN, MPLS, submarine
  Edge Computing   (dtn_edge*)      - MEC, CORD, edge CDN, IoT gateway
  Spectrum         (dtn_spectrum*)  - licensed, shared, unlicensed

Source: NAICS 5171 wired telecom industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_telecom_network import (
    TELECOM_NETWORK_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_telecom_network,
)


class TestDetermineLevel:
    def test_access_category_is_level_1(self):
        assert _determine_level("dtn_access") == 1

    def test_access_type_is_level_2(self):
        assert _determine_level("dtn_access_ran") == 2

    def test_core_category_is_level_1(self):
        assert _determine_level("dtn_core") == 1

    def test_transport_type_is_level_2(self):
        assert _determine_level("dtn_transport_dwdm") == 2

    def test_edge_category_is_level_1(self):
        assert _determine_level("dtn_edge") == 1

    def test_spectrum_type_is_level_2(self):
        assert _determine_level("dtn_spectrum_shared") == 2


class TestDetermineParent:
    def test_access_category_has_no_parent(self):
        assert _determine_parent("dtn_access") is None

    def test_access_ran_parent_is_access(self):
        assert _determine_parent("dtn_access_ran") == "dtn_access"

    def test_core_5gc_parent_is_core(self):
        assert _determine_parent("dtn_core_5gc") == "dtn_core"

    def test_edge_mec_parent_is_edge(self):
        assert _determine_parent("dtn_edge_mec") == "dtn_edge"


class TestTelecomNetworkNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(TELECOM_NETWORK_NODES) > 0

    def test_has_access_category(self):
        codes = [n[0] for n in TELECOM_NETWORK_NODES]
        assert "dtn_access" in codes

    def test_has_core_category(self):
        codes = [n[0] for n in TELECOM_NETWORK_NODES]
        assert "dtn_core" in codes

    def test_has_transport_category(self):
        codes = [n[0] for n in TELECOM_NETWORK_NODES]
        assert "dtn_transport" in codes

    def test_has_edge_category(self):
        codes = [n[0] for n in TELECOM_NETWORK_NODES]
        assert "dtn_edge" in codes

    def test_has_spectrum_category(self):
        codes = [n[0] for n in TELECOM_NETWORK_NODES]
        assert "dtn_spectrum" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in TELECOM_NETWORK_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in TELECOM_NETWORK_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in TELECOM_NETWORK_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in TELECOM_NETWORK_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(TELECOM_NETWORK_NODES) >= 20

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in TELECOM_NETWORK_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_telecom_network_module_importable():
    assert callable(ingest_domain_telecom_network)
    assert isinstance(TELECOM_NETWORK_NODES, list)


def test_ingest_domain_telecom_network(db_pool):
    """Integration test: telecom network taxonomy rows."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_telecom_network(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_telecom_network'"
            )
            assert row is not None
            assert row["code_count"] == count

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_telecom_network_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_telecom_network(conn)
            count2 = await ingest_domain_telecom_network(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
