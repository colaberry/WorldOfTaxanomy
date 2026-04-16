"""Tests for NACE-derived classification ingesters (WZ, ONACE, NOGA).

These tests rely on the conftest.py session fixtures (db_pool, setup_and_teardown)
which create a test_wot schema and seed NAICS, ISIC, and SIC data.

Since the derived ingesters require nace_rev2 data, each test seeds a small
set of NACE Rev 2 nodes before running the ingester under test.
"""

import asyncio
import pytest


def _run(coro):
    """Run a coroutine synchronously."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ── NACE Rev 2 seed data ───────────────────────────────────────

NACE_NODES = [
    # (code, title, description, level, parent_code, sector_code, is_leaf, seq_order)
    ("A", "Agriculture, forestry and fishing", None, 0, None, "A", False, 1),
    ("C", "Manufacturing", None, 0, None, "C", False, 2),
    ("01", "Crop and animal production", None, 1, "A", "A", False, 3),
    ("10", "Manufacture of food products", None, 1, "C", "C", False, 4),
    ("01.1", "Growing of non-perennial crops", None, 2, "01", "A", False, 5),
    ("10.1", "Processing and preserving of meat", None, 2, "10", "C", False, 6),
    ("01.11", "Growing of cereals", None, 3, "01.1", "A", True, 7),
    ("10.11", "Processing and preserving of meat", None, 3, "10.1", "C", True, 8),
]


async def _seed_nace_rev2(conn):
    """Insert a minimal nace_rev2 system and sample nodes."""
    await conn.execute("""
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ('nace_rev2', 'NACE Rev 2',
                'Statistical Classification of Economic Activities in the European Community, Rev. 2',
                'European Union (27 countries)', 'Rev 2', 'Eurostat', '#1E40AF')
    """)
    for code, title, desc, level, parent, sector, leaf, seq in NACE_NODES:
        await conn.execute("""
            INSERT INTO classification_node
                (system_id, code, title, description, level,
                 parent_code, sector_code, is_leaf, seq_order)
            VALUES ('nace_rev2', $1, $2, $3, $4, $5, $6, $7, $8)
        """, code, title, desc, level, parent, sector, leaf, seq)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'nace_rev2'",
        len(NACE_NODES),
    )


@pytest.fixture
def seed_nace(db_pool):
    """Seed NACE Rev 2 data for tests that need it."""
    async def _seed():
        async with db_pool.acquire() as conn:
            await _seed_nace_rev2(conn)
    _run(_seed())


# ── Helpers ─────────────────────────────────────────────────────


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


async def _count_equivalences(conn, source_system: str, target_system: str) -> int:
    row = await conn.fetchrow(
        """SELECT count(*) AS cnt FROM equivalence
           WHERE source_system = $1 AND target_system = $2""",
        source_system,
        target_system,
    )
    return row["cnt"]


async def _get_node(conn, system_id: str, code: str):
    return await conn.fetchrow(
        "SELECT * FROM classification_node WHERE system_id = $1 AND code = $2",
        system_id,
        code,
    )


# ── WZ 2008 Tests ──────────────────────────────────────────────


class TestIngestWZ2008:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_wz_2008

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_wz_2008(conn)
                assert count == len(NACE_NODES)

                sys = await _get_system(conn, "wz_2008")
                assert sys is not None
                assert sys["name"] == "WZ 2008"
                assert sys["region"] == "Germany"
                assert sys["authority"] == "Statistisches Bundesamt (Destatis)"
                assert sys["tint_color"] == "#EF4444"
                assert sys["node_count"] == len(NACE_NODES)

        _run(_test())

    def test_copies_all_nodes(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_wz_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_wz_2008(conn)

                node_count = await _count_nodes(conn, "wz_2008")
                assert node_count == len(NACE_NODES)

                # Verify a specific node was copied correctly
                node = await _get_node(conn, "wz_2008", "01.11")
                assert node is not None
                assert node["title"] == "Growing of cereals"
                assert node["level"] == 3
                assert node["parent_code"] == "01.1"
                assert node["sector_code"] == "A"
                assert node["is_leaf"] is True

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_wz_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_wz_2008(conn)

                # Forward edges: wz_2008 -> nace_rev2
                fwd = await _count_equivalences(conn, "wz_2008", "nace_rev2")
                assert fwd == len(NACE_NODES)

                # Reverse edges: nace_rev2 -> wz_2008
                rev = await _count_equivalences(conn, "nace_rev2", "wz_2008")
                assert rev == len(NACE_NODES)

                # All edges should be exact match
                row = await conn.fetchrow("""
                    SELECT count(*) AS cnt FROM equivalence
                    WHERE source_system = 'wz_2008'
                      AND target_system = 'nace_rev2'
                      AND match_type = 'exact'
                """)
                assert row["cnt"] == len(NACE_NODES)

        _run(_test())


# ── ONACE 2008 Tests ────────────────────────────────────────────


class TestIngestONACE2008:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_onace_2008

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_onace_2008(conn)
                assert count == len(NACE_NODES)

                sys = await _get_system(conn, "onace_2008")
                assert sys is not None
                assert sys["name"] == "ÖNACE 2008"
                assert sys["region"] == "Austria"
                assert sys["authority"] == "Statistik Austria"
                assert sys["tint_color"] == "#DC2626"

        _run(_test())

    def test_copies_all_nodes(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_onace_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_onace_2008(conn)

                node_count = await _count_nodes(conn, "onace_2008")
                assert node_count == len(NACE_NODES)

                # Verify section node
                node = await _get_node(conn, "onace_2008", "A")
                assert node is not None
                assert node["title"] == "Agriculture, forestry and fishing"
                assert node["level"] == 0
                assert node["is_leaf"] is False

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_onace_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_onace_2008(conn)

                fwd = await _count_equivalences(conn, "onace_2008", "nace_rev2")
                assert fwd == len(NACE_NODES)

                rev = await _count_equivalences(conn, "nace_rev2", "onace_2008")
                assert rev == len(NACE_NODES)

        _run(_test())


# ── NOGA 2008 Tests ─────────────────────────────────────────────


class TestIngestNOGA2008:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_noga_2008

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_noga_2008(conn)
                assert count == len(NACE_NODES)

                sys = await _get_system(conn, "noga_2008")
                assert sys is not None
                assert sys["name"] == "NOGA 2008"
                assert sys["region"] == "Switzerland"
                assert sys["authority"] == "Swiss Federal Statistical Office (BFS)"
                assert sys["tint_color"] == "#B91C1C"

        _run(_test())

    def test_copies_all_nodes(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_noga_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_noga_2008(conn)

                node_count = await _count_nodes(conn, "noga_2008")
                assert node_count == len(NACE_NODES)

                # Verify a leaf node
                node = await _get_node(conn, "noga_2008", "10.11")
                assert node is not None
                assert node["title"] == "Processing and preserving of meat"
                assert node["level"] == 3
                assert node["parent_code"] == "10.1"
                assert node["is_leaf"] is True

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_noga_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_noga_2008(conn)

                fwd = await _count_equivalences(conn, "noga_2008", "nace_rev2")
                assert fwd == len(NACE_NODES)

                rev = await _count_equivalences(conn, "nace_rev2", "noga_2008")
                assert rev == len(NACE_NODES)

        _run(_test())


# ── Edge case: no NACE data ─────────────────────────────────────


class TestDerivedWithoutNACE:

    def test_returns_zero_when_no_nace_data(self, db_pool):
        """Ingester should return 0 if nace_rev2 has no nodes."""
        from world_of_taxonomy.ingest.nace_derived import ingest_wz_2008

        async def _test():
            async with db_pool.acquire() as conn:
                # nace_rev2 system doesn't exist, so no nodes to copy
                count = await ingest_wz_2008(conn)
                assert count == 0

                # System should still be registered
                sys = await _get_system(conn, "wz_2008")
                assert sys is not None
                assert sys["node_count"] == 0

        _run(_test())


# ── ATECO 2007 Tests ─────────────────────────────────────────────


class TestIngestATECO2007:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_ateco_2007

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_ateco_2007(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "ateco_2007")
                assert sys is not None
                assert sys["name"] == "ATECO 2007"
                assert sys["region"] == "Italy"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_ateco_2007

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_ateco_2007(conn)
                fwd = await _count_equivalences(conn, "ateco_2007", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NAF Rev 2 Tests ──────────────────────────────────────────────


class TestIngestNAFRev2:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_naf_rev2

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_naf_rev2(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "naf_rev2")
                assert sys is not None
                assert sys["name"] == "NAF Rev 2"
                assert sys["region"] == "France"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_naf_rev2

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_naf_rev2(conn)
                fwd = await _count_equivalences(conn, "naf_rev2", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── PKD 2007 Tests ───────────────────────────────────────────────


class TestIngestPKD2007:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_pkd_2007

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_pkd_2007(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "pkd_2007")
                assert sys is not None
                assert sys["name"] == "PKD 2007"
                assert sys["region"] == "Poland"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_pkd_2007

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_pkd_2007(conn)
                fwd = await _count_equivalences(conn, "pkd_2007", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── SBI 2008 Tests ───────────────────────────────────────────────


class TestIngestSBI2008:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_sbi_2008

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_sbi_2008(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "sbi_2008")
                assert sys is not None
                assert sys["name"] == "SBI 2008"
                assert sys["region"] == "Netherlands"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_sbi_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_sbi_2008(conn)
                fwd = await _count_equivalences(conn, "sbi_2008", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── SNI 2007 Tests ───────────────────────────────────────────────


class TestIngestSNI2007:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_sni_2007

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_sni_2007(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "sni_2007")
                assert sys is not None
                assert sys["name"] == "SNI 2007"
                assert sys["region"] == "Sweden"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_sni_2007

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_sni_2007(conn)
                fwd = await _count_equivalences(conn, "sni_2007", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── DB07 Tests ───────────────────────────────────────────────────


class TestIngestDB07:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_db07

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_db07(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "db07")
                assert sys is not None
                assert sys["name"] == "DB07"
                assert sys["region"] == "Denmark"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_db07

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_db07(conn)
                fwd = await _count_equivalences(conn, "db07", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── TOL 2008 Tests ───────────────────────────────────────────────


class TestIngestTOL2008:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_tol_2008

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_tol_2008(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "tol_2008")
                assert sys is not None
                assert sys["name"] == "TOL 2008"
                assert sys["region"] == "Finland"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_tol_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_tol_2008(conn)
                fwd = await _count_equivalences(conn, "tol_2008", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── CAE Rev 3 Tests (Portugal) ───────────────────────────────────


class TestIngestCAERev3:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_cae_rev3

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_cae_rev3(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "cae_rev3")
                assert sys is not None
                assert sys["region"] == "Portugal"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_cae_rev3

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_cae_rev3(conn)
                fwd = await _count_equivalences(conn, "cae_rev3", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── CZ-NACE Tests (Czech Republic) ──────────────────────────────


class TestIngestCZNACE:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_cz_nace

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_cz_nace(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "cz_nace")
                assert sys is not None
                assert sys["region"] == "Czech Republic"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_cz_nace

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_cz_nace(conn)
                fwd = await _count_equivalences(conn, "cz_nace", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── TEAOR 2008 Tests (Hungary) ───────────────────────────────────


class TestIngestTEAOR2008:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_teaor_2008

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_teaor_2008(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "teaor_2008")
                assert sys is not None
                assert sys["region"] == "Hungary"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_teaor_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_teaor_2008(conn)
                fwd = await _count_equivalences(conn, "teaor_2008", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── CAEN Rev 2 Tests (Romania) ───────────────────────────────────


class TestIngestCAENRev2:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_caen_rev2

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_caen_rev2(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "caen_rev2")
                assert sys is not None
                assert sys["region"] == "Romania"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_caen_rev2

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_caen_rev2(conn)
                fwd = await _count_equivalences(conn, "caen_rev2", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NKD 2007 Tests (Croatia) ────────────────────────────────────


class TestIngestNKD2007:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nkd_2007

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nkd_2007(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nkd_2007")
                assert sys is not None
                assert sys["region"] == "Croatia"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nkd_2007

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nkd_2007(conn)
                fwd = await _count_equivalences(conn, "nkd_2007", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── SK NACE Tests (Slovakia) ────────────────────────────────────


class TestIngestSKNACE:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_sk_nace

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_sk_nace(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "sk_nace")
                assert sys is not None
                assert sys["region"] == "Slovakia"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_sk_nace

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_sk_nace(conn)
                fwd = await _count_equivalences(conn, "sk_nace", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NKID Tests (Bulgaria) ────────────────────────────────────────


class TestIngestNKID:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nkid

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nkid(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nkid")
                assert sys is not None
                assert sys["region"] == "Bulgaria"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nkid

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nkid(conn)
                fwd = await _count_equivalences(conn, "nkid", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── EMTAK Tests (Estonia) ────────────────────────────────────────


class TestIngestEMTAK:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_emtak

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_emtak(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "emtak")
                assert sys is not None
                assert sys["region"] == "Estonia"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_emtak

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_emtak(conn)
                fwd = await _count_equivalences(conn, "emtak", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NACE-LT Tests (Lithuania) ────────────────────────────────────


class TestIngestNACELT:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_lt

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nace_lt(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nace_lt")
                assert sys is not None
                assert sys["region"] == "Lithuania"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_lt

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nace_lt(conn)
                fwd = await _count_equivalences(conn, "nace_lt", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NK-LV Tests (Latvia) ─────────────────────────────────────────


class TestIngestNKLV:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nk_lv

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nk_lv(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nk_lv")
                assert sys is not None
                assert sys["region"] == "Latvia"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nk_lv

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nk_lv(conn)
                fwd = await _count_equivalences(conn, "nk_lv", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NACE-TR Tests (Turkey) ───────────────────────────────────────


class TestIngestNACETR:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_tr

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nace_tr(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nace_tr")
                assert sys is not None
                assert sys["region"] == "Turkey"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_tr

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nace_tr(conn)
                fwd = await _count_equivalences(conn, "nace_tr", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── CNAE 2009 Tests (Spain) ─────────────────────────────────────


class TestIngestCNAE2009:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_cnae_2009

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_cnae_2009(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "cnae_2009")
                assert sys is not None
                assert sys["name"] == "CNAE 2009"
                assert sys["region"] == "Spain"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_cnae_2009

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_cnae_2009(conn)
                fwd = await _count_equivalences(conn, "cnae_2009", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NACE-BEL Tests (Belgium) ───────────────────────────────────


class TestIngestNACEBEL:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_bel

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nace_bel(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nace_bel")
                assert sys is not None
                assert sys["name"] == "NACE-BEL 2008"
                assert sys["region"] == "Belgium"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_bel

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nace_bel(conn)
                fwd = await _count_equivalences(conn, "nace_bel", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NACE-LU Tests (Luxembourg) ─────────────────────────────────


class TestIngestNACELU:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_lu

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nace_lu(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nace_lu")
                assert sys is not None
                assert sys["name"] == "NACE-LU 2008"
                assert sys["region"] == "Luxembourg"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_lu

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nace_lu(conn)
                fwd = await _count_equivalences(conn, "nace_lu", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NACE-IE Tests (Ireland) ────────────────────────────────────


class TestIngestNACEIE:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_ie

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nace_ie(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nace_ie")
                assert sys is not None
                assert sys["name"] == "NACE Rev 2 (Ireland)"
                assert sys["region"] == "Ireland"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_ie

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nace_ie(conn)
                fwd = await _count_equivalences(conn, "nace_ie", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── STAKOD 08 Tests (Greece) ───────────────────────────────────


class TestIngestSTAKOD08:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_stakod_08

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_stakod_08(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "stakod_08")
                assert sys is not None
                assert sys["name"] == "STAKOD 08"
                assert sys["region"] == "Greece"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_stakod_08

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_stakod_08(conn)
                fwd = await _count_equivalences(conn, "stakod_08", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NACE-CY Tests (Cyprus) ─────────────────────────────────────


class TestIngestNACECY:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_cy

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nace_cy(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nace_cy")
                assert sys is not None
                assert sys["name"] == "NACE Rev 2 (Cyprus)"
                assert sys["region"] == "Cyprus"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_cy

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nace_cy(conn)
                fwd = await _count_equivalences(conn, "nace_cy", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── NACE-MT Tests (Malta) ──────────────────────────────────────


class TestIngestNACEMT:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_mt

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nace_mt(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nace_mt")
                assert sys is not None
                assert sys["name"] == "NACE Rev 2 (Malta)"
                assert sys["region"] == "Malta"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_mt

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nace_mt(conn)
                fwd = await _count_equivalences(conn, "nace_mt", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── SKD 2008 Tests (Slovenia) ──────────────────────────────────


class TestIngestSKD2008:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_skd_2008

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_skd_2008(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "skd_2008")
                assert sys is not None
                assert sys["name"] == "SKD 2008"
                assert sys["region"] == "Slovenia"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_skd_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_skd_2008(conn)
                fwd = await _count_equivalences(conn, "skd_2008", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── SN 2007 Tests (Norway) ─────────────────────────────────────


class TestIngestSN2007:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_sn_2007

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_sn_2007(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "sn_2007")
                assert sys is not None
                assert sys["name"] == "SN 2007"
                assert sys["region"] == "Norway"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_sn_2007

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_sn_2007(conn)
                fwd = await _count_equivalences(conn, "sn_2007", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── ISAT 2008 Tests (Iceland) ──────────────────────────────────


class TestIngestISAT2008:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_isat_2008

        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_isat_2008(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "isat_2008")
                assert sys is not None
                assert sys["name"] == "ISAT 2008"
                assert sys["region"] == "Iceland"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_isat_2008

        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_isat_2008(conn)
                fwd = await _count_equivalences(conn, "isat_2008", "nace_rev2")
                assert fwd == len(NACE_NODES)

        _run(_test())


# ── KD 2010 Tests (Serbia) ─────────────────────────────────────


class TestIngestKDRS:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kd_rs
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_kd_rs(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "kd_rs")
                assert sys is not None
                assert sys["region"] == "Serbia"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kd_rs
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_kd_rs(conn)
                fwd = await _count_equivalences(conn, "kd_rs", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())


# ── NKD Rev 2 Tests (North Macedonia) ──────────────────────────


class TestIngestNKDMK:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nkd_mk
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nkd_mk(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nkd_mk")
                assert sys is not None
                assert sys["region"] == "North Macedonia"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nkd_mk
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nkd_mk(conn)
                fwd = await _count_equivalences(conn, "nkd_mk", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())


# ── KD BiH 2010 Tests (Bosnia) ─────────────────────────────────


class TestIngestKDBA:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kd_ba
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_kd_ba(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "kd_ba")
                assert sys is not None
                assert sys["region"] == "Bosnia and Herzegovina"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kd_ba
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_kd_ba(conn)
                fwd = await _count_equivalences(conn, "kd_ba", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())


# ── KD 2010 Tests (Montenegro) ─────────────────────────────────


class TestIngestKDME:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kd_me
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_kd_me(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "kd_me")
                assert sys is not None
                assert sys["region"] == "Montenegro"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kd_me
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_kd_me(conn)
                fwd = await _count_equivalences(conn, "kd_me", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())


# ── NVE Rev 2 Tests (Albania) ──────────────────────────────────


class TestIngestNVEAL:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nve_al
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nve_al(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nve_al")
                assert sys is not None
                assert sys["region"] == "Albania"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nve_al
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nve_al(conn)
                fwd = await _count_equivalences(conn, "nve_al", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())


# ── KD 2010 Tests (Kosovo) ─────────────────────────────────────


class TestIngestKDXK:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kd_xk
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_kd_xk(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "kd_xk")
                assert sys is not None
                assert sys["region"] == "Kosovo"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kd_xk
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_kd_xk(conn)
                fwd = await _count_equivalences(conn, "kd_xk", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())


# ── CAEM Rev 2 Tests (Moldova) ─────────────────────────────────


class TestIngestCAEMMD:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_caem_md
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_caem_md(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "caem_md")
                assert sys is not None
                assert sys["region"] == "Moldova"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_caem_md
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_caem_md(conn)
                fwd = await _count_equivalences(conn, "caem_md", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())


# ── KVED 2010 Tests (Ukraine) ──────────────────────────────────


class TestIngestKVEDUA:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kved_ua
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_kved_ua(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "kved_ua")
                assert sys is not None
                assert sys["region"] == "Ukraine"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_kved_ua
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_kved_ua(conn)
                fwd = await _count_equivalences(conn, "kved_ua", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())


# ── NACE Rev 2 Tests (Georgia) ─────────────────────────────────


class TestIngestNACEGE:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_ge
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nace_ge(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nace_ge")
                assert sys is not None
                assert sys["region"] == "Georgia"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_ge
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nace_ge(conn)
                fwd = await _count_equivalences(conn, "nace_ge", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())


# ── NACE Rev 2 Tests (Armenia) ─────────────────────────────────


class TestIngestNACEAM:

    def test_creates_system(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_am
        async def _test():
            async with db_pool.acquire() as conn:
                count = await ingest_nace_am(conn)
                assert count == len(NACE_NODES)
                sys = await _get_system(conn, "nace_am")
                assert sys is not None
                assert sys["region"] == "Armenia"
        _run(_test())

    def test_creates_equivalence_edges(self, db_pool, seed_nace):
        from world_of_taxonomy.ingest.nace_derived import ingest_nace_am
        async def _test():
            async with db_pool.acquire() as conn:
                await ingest_nace_am(conn)
                fwd = await _count_equivalences(conn, "nace_am", "nace_rev2")
                assert fwd == len(NACE_NODES)
        _run(_test())
