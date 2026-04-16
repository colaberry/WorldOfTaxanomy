"""Tests for Quantum Computing domain taxonomy ingester.

RED tests - written before any implementation exists.

Quantum computing taxonomy organizes quantum tech sector types:
  Qubit Technologies (dqc_qubit*)  - superconducting, trapped ion, photonic, topological
  Error Correction  (dqc_ecc*)    - surface code, stabilizer codes, fault-tolerant
  Quantum Sensing   (dqc_sense*)  - gravimetry, magnetometry, atomic clocks
  Quantum Networking (dqc_net*)   - QKD, quantum repeaters, entanglement distribution
  Quantum Software  (dqc_soft*)   - algorithms, compilers, simulators
  Quantum Platforms (dqc_plat*)   - cloud access, hardware services, simulators

Source: NAICS 334 + 5417 quantum computing industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxonomy.ingest.domain_quantum import (
    QUANTUM_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_quantum,
)


class TestDetermineLevel:
    def test_qubit_category_is_level_1(self):
        assert _determine_level("dqc_qubit") == 1

    def test_qubit_type_is_level_2(self):
        assert _determine_level("dqc_qubit_sc") == 2

    def test_soft_category_is_level_1(self):
        assert _determine_level("dqc_soft") == 1

    def test_soft_type_is_level_2(self):
        assert _determine_level("dqc_soft_algo") == 2

    def test_sense_category_is_level_1(self):
        assert _determine_level("dqc_sense") == 1

    def test_sense_type_is_level_2(self):
        assert _determine_level("dqc_sense_grav") == 2


class TestDetermineParent:
    def test_qubit_category_has_no_parent(self):
        assert _determine_parent("dqc_qubit") is None

    def test_qubit_sc_parent_is_qubit(self):
        assert _determine_parent("dqc_qubit_sc") == "dqc_qubit"

    def test_soft_algo_parent_is_soft(self):
        assert _determine_parent("dqc_soft_algo") == "dqc_soft"

    def test_net_qkd_parent_is_net(self):
        assert _determine_parent("dqc_net_qkd") == "dqc_net"


class TestQuantumNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(QUANTUM_NODES) > 0

    def test_has_qubit_technologies_category(self):
        codes = [n[0] for n in QUANTUM_NODES]
        assert "dqc_qubit" in codes

    def test_has_quantum_sensing_category(self):
        codes = [n[0] for n in QUANTUM_NODES]
        assert "dqc_sense" in codes

    def test_has_quantum_software_category(self):
        codes = [n[0] for n in QUANTUM_NODES]
        assert "dqc_soft" in codes

    def test_has_quantum_networking_category(self):
        codes = [n[0] for n in QUANTUM_NODES]
        assert "dqc_net" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in QUANTUM_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in QUANTUM_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in QUANTUM_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in QUANTUM_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(QUANTUM_NODES) >= 18

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in QUANTUM_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_quantum_module_importable():
    assert callable(ingest_domain_quantum)
    assert isinstance(QUANTUM_NODES, list)


def test_ingest_domain_quantum(db_pool):
    """Integration test: quantum taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_quantum(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_quantum'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_quantum'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_quantum_idempotent(db_pool):
    async def _run():
        from world_of_taxonomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_quantum(conn)
            count2 = await ingest_domain_quantum(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
