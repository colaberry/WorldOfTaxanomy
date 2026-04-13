"""Tests for Digital Assets and Web3 domain taxonomy ingester.

RED tests - written before any implementation exists.

Digital assets taxonomy organizes blockchain and Web3 sector types:
  Blockchain Infra  (dda_chain*)   - L1 blockchains, L2 solutions, sidechains
  DeFi Protocols    (dda_defi*)    - DEX, lending, yield, derivatives
  Digital Tokens    (dda_token*)   - NFTs, utility tokens, governance tokens
  Stablecoins/CBDC  (dda_stable*)  - fiat-backed, algo-stable, CBDC
  Crypto Services   (dda_svc*)     - exchanges, custody, wallets, payments
  Web3 Infrastructure (dda_web3*)  - identity, storage, oracles, bridges

Source: NAICS 522390 + 5415 digital assets industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_digital_assets import (
    DIGITAL_ASSETS_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_digital_assets,
)


class TestDetermineLevel:
    def test_chain_category_is_level_1(self):
        assert _determine_level("dda_chain") == 1

    def test_chain_type_is_level_2(self):
        assert _determine_level("dda_chain_l1") == 2

    def test_defi_category_is_level_1(self):
        assert _determine_level("dda_defi") == 1

    def test_defi_type_is_level_2(self):
        assert _determine_level("dda_defi_dex") == 2

    def test_stable_category_is_level_1(self):
        assert _determine_level("dda_stable") == 1

    def test_stable_type_is_level_2(self):
        assert _determine_level("dda_stable_fiat") == 2


class TestDetermineParent:
    def test_chain_category_has_no_parent(self):
        assert _determine_parent("dda_chain") is None

    def test_chain_l1_parent_is_chain(self):
        assert _determine_parent("dda_chain_l1") == "dda_chain"

    def test_defi_dex_parent_is_defi(self):
        assert _determine_parent("dda_defi_dex") == "dda_defi"

    def test_stable_fiat_parent_is_stable(self):
        assert _determine_parent("dda_stable_fiat") == "dda_stable"


class TestDigitalAssetsNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(DIGITAL_ASSETS_NODES) > 0

    def test_has_blockchain_infra_category(self):
        codes = [n[0] for n in DIGITAL_ASSETS_NODES]
        assert "dda_chain" in codes

    def test_has_defi_category(self):
        codes = [n[0] for n in DIGITAL_ASSETS_NODES]
        assert "dda_defi" in codes

    def test_has_stablecoins_category(self):
        codes = [n[0] for n in DIGITAL_ASSETS_NODES]
        assert "dda_stable" in codes

    def test_has_crypto_services_category(self):
        codes = [n[0] for n in DIGITAL_ASSETS_NODES]
        assert "dda_svc" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in DIGITAL_ASSETS_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in DIGITAL_ASSETS_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in DIGITAL_ASSETS_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in DIGITAL_ASSETS_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(DIGITAL_ASSETS_NODES) >= 18

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in DIGITAL_ASSETS_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_digital_assets_module_importable():
    assert callable(ingest_domain_digital_assets)
    assert isinstance(DIGITAL_ASSETS_NODES, list)


def test_ingest_domain_digital_assets(db_pool):
    """Integration test: digital assets taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_digital_assets(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_digital_assets'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_digital_assets'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_digital_assets_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_digital_assets(conn)
            count2 = await ingest_domain_digital_assets(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
