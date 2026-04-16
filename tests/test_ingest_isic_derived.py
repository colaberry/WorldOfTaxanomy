"""Tests for ISIC-derived classification ingesters (CIIU Colombia, Argentina, Chile).

These tests rely on the conftest.py session fixtures (db_pool, setup_and_teardown)
which create a test_wot schema and seed ISIC Rev 4 data among others.

Since the derived ingesters copy all isic_rev4 nodes, tests query the actual
node count dynamically rather than asserting against a hardcoded constant.
"""

import asyncio
import pytest


def _run(coro):
    """Run a coroutine synchronously."""
    return asyncio.get_event_loop().run_until_complete(coro)


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


# ── CIIU Colombia Tests ─────────────────────────────────────────


class TestIngestCIIUColombia:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_co

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                assert isic_count > 0, "conftest must seed isic_rev4 nodes"

                count = await ingest_ciiu_co(conn)
                assert count == isic_count

                sys = await _get_system(conn, "ciiu_co")
                assert sys is not None
                assert "Colombia" in sys["name"]
                assert sys["region"] == "Colombia"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_co

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_co(conn)

                fwd = await _count_equivalences(conn, "ciiu_co", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "ciiu_co")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())

    def test_idempotent(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_co

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_co(conn)
                await ingest_ciiu_co(conn)

                node_count = await _count_nodes(conn, "ciiu_co")
                assert node_count == isic_count
                fwd = await _count_equivalences(conn, "ciiu_co", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── CIIU Argentina Tests ────────────────────────────────────────


class TestIngestCIIUArgentina:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_ar

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_ar(conn)
                assert count == isic_count

                sys = await _get_system(conn, "ciiu_ar")
                assert sys is not None
                assert "Argentina" in sys["name"]
                assert sys["region"] == "Argentina"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_ar

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_ar(conn)

                fwd = await _count_equivalences(conn, "ciiu_ar", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "ciiu_ar")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── CIIU Chile Tests ────────────────────────────────────────────


class TestIngestCIIUChile:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_cl

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_cl(conn)
                assert count == isic_count

                sys = await _get_system(conn, "ciiu_cl")
                assert sys is not None
                assert "Chile" in sys["name"]
                assert sys["region"] == "Chile"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_cl

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_cl(conn)

                fwd = await _count_equivalences(conn, "ciiu_cl", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "ciiu_cl")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── CIIU Rev 4 (Peru) Tests (Peru) ──────────────────────────────────────────────────


class TestIngestCiiuPe:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_pe

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_pe(conn)
                assert count == isic_count
                sys = await _get_system(conn, "ciiu_pe")
                assert sys is not None
                assert sys["region"] == "Peru"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_pe

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_pe(conn)
                fwd = await _count_equivalences(conn, "ciiu_pe", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "ciiu_pe")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── CIIU Rev 4 (Ecuador) Tests (Ecuador) ──────────────────────────────────────────────────


class TestIngestCiiuEc:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_ec

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_ec(conn)
                assert count == isic_count
                sys = await _get_system(conn, "ciiu_ec")
                assert sys is not None
                assert sys["region"] == "Ecuador"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_ec

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_ec(conn)
                fwd = await _count_equivalences(conn, "ciiu_ec", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "ciiu_ec")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── CAEB Tests (Bolivia) ──────────────────────────────────────────────────


class TestIngestCaeb:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_caeb

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_caeb(conn)
                assert count == isic_count
                sys = await _get_system(conn, "caeb")
                assert sys is not None
                assert sys["region"] == "Bolivia"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_caeb

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_caeb(conn)
                fwd = await _count_equivalences(conn, "caeb", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "caeb")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── CIIU Rev 4 (Venezuela) Tests (Venezuela) ──────────────────────────────────────────────────


class TestIngestCiiuVe:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_ve

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_ve(conn)
                assert count == isic_count
                sys = await _get_system(conn, "ciiu_ve")
                assert sys is not None
                assert sys["region"] == "Venezuela"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_ve

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_ve(conn)
                fwd = await _count_equivalences(conn, "ciiu_ve", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "ciiu_ve")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── CIIU Rev 4 (Costa Rica) Tests (Costa Rica) ──────────────────────────────────────────────────


class TestIngestCiiuCr:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_cr

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_cr(conn)
                assert count == isic_count
                sys = await _get_system(conn, "ciiu_cr")
                assert sys is not None
                assert sys["region"] == "Costa Rica"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_cr

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_cr(conn)
                fwd = await _count_equivalences(conn, "ciiu_cr", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "ciiu_cr")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── CIIU Rev 4 (Guatemala) Tests (Guatemala) ──────────────────────────────────────────────────


class TestIngestCiiuGt:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_gt

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_gt(conn)
                assert count == isic_count
                sys = await _get_system(conn, "ciiu_gt")
                assert sys is not None
                assert sys["region"] == "Guatemala"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_gt

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_gt(conn)
                fwd = await _count_equivalences(conn, "ciiu_gt", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "ciiu_gt")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── CIIU Rev 4 (Panama) Tests (Panama) ──────────────────────────────────────────────────


class TestIngestCiiuPa:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_pa

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_pa(conn)
                assert count == isic_count
                sys = await _get_system(conn, "ciiu_pa")
                assert sys is not None
                assert sys["region"] == "Panama"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_pa

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_pa(conn)
                fwd = await _count_equivalences(conn, "ciiu_pa", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "ciiu_pa")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── VSIC 2018 Tests (Vietnam) ──────────────────────────────────────────────────


class TestIngestVsic2018:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_vsic_2018

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_vsic_2018(conn)
                assert count == isic_count
                sys = await _get_system(conn, "vsic_2018")
                assert sys is not None
                assert sys["region"] == "Vietnam"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_vsic_2018

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_vsic_2018(conn)
                fwd = await _count_equivalences(conn, "vsic_2018", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "vsic_2018")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── BSIC Tests (Bangladesh) ──────────────────────────────────────────────────


class TestIngestBsic:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_bsic

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_bsic(conn)
                assert count == isic_count
                sys = await _get_system(conn, "bsic")
                assert sys is not None
                assert sys["region"] == "Bangladesh"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_bsic

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_bsic(conn)
                fwd = await _count_equivalences(conn, "bsic", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "bsic")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── PSIC (Pakistan) Tests (Pakistan) ──────────────────────────────────────────────────


class TestIngestPsicPk:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_psic_pk

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_psic_pk(conn)
                assert count == isic_count
                sys = await _get_system(conn, "psic_pk")
                assert sys is not None
                assert sys["region"] == "Pakistan"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_psic_pk

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_psic_pk(conn)
                fwd = await _count_equivalences(conn, "psic_pk", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "psic_pk")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── ISIC Rev 4 (Nigeria) Tests (Nigeria) ──────────────────────────────────────────────────


class TestIngestIsicNg:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ng

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_ng(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_ng")
                assert sys is not None
                assert sys["region"] == "Nigeria"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ng

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_ng(conn)
                fwd = await _count_equivalences(conn, "isic_ng", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "isic_ng")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── ISIC Rev 4 (Kenya) Tests (Kenya) ──────────────────────────────────────────────────


class TestIngestIsicKe:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ke

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_ke(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_ke")
                assert sys is not None
                assert sys["region"] == "Kenya"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ke

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_ke(conn)
                fwd = await _count_equivalences(conn, "isic_ke", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "isic_ke")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── ISIC Rev 4 (Egypt) Tests (Egypt) ──────────────────────────────────────────────────


class TestIngestIsicEg:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_eg

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_eg(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_eg")
                assert sys is not None
                assert sys["region"] == "Egypt"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_eg

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_eg(conn)
                fwd = await _count_equivalences(conn, "isic_eg", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "isic_eg")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── ISIC Rev 4 (Saudi Arabia) Tests (Saudi Arabia) ──────────────────────────────────────────────────


class TestIngestIsicSa:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sa

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_sa(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_sa")
                assert sys is not None
                assert sys["region"] == "Saudi Arabia"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sa

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_sa(conn)
                fwd = await _count_equivalences(conn, "isic_sa", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "isic_sa")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── ISIC Rev 4 (UAE) Tests (United Arab Emirates) ──────────────────────────────────────────────────


class TestIngestIsicAe:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ae

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_ae(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_ae")
                assert sys is not None
                assert sys["region"] == "United Arab Emirates"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ae

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_ae(conn)
                fwd = await _count_equivalences(conn, "isic_ae", "isic_rev4")
                rev = await _count_equivalences(conn, "isic_rev4", "isic_ae")
                assert fwd == isic_count
                assert rev == isic_count

        _run(_test())


# ── KBLI 2020 Tests (Indonesia) ────────────────────────────────


class TestIngestKbli2020:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_kbli_id

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_kbli_id(conn)
                assert count == isic_count
                sys = await _get_system(conn, "kbli_id")
                assert sys is not None
                assert sys["region"] == "Indonesia"
                assert sys["node_count"] == isic_count

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_kbli_id

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_kbli_id(conn)
                fwd = await _count_equivalences(conn, "kbli_id", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── SLSIC Tests (Sri Lanka) ────────────────────────────────────


class TestIngestSlsic:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_slsic

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_slsic(conn)
                assert count == isic_count
                sys = await _get_system(conn, "slsic")
                assert sys is not None
                assert sys["region"] == "Sri Lanka"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_slsic

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_slsic(conn)
                fwd = await _count_equivalences(conn, "slsic", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Myanmar Tests ─────────────────────────────────────────


class TestIngestIsicMm:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mm

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_mm(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_mm")
                assert sys is not None
                assert sys["region"] == "Myanmar"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mm

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_mm(conn)
                fwd = await _count_equivalences(conn, "isic_mm", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Cambodia Tests ────────────────────────────────────────


class TestIngestIsicKh:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kh

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_kh(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_kh")
                assert sys is not None
                assert sys["region"] == "Cambodia"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kh

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_kh(conn)
                fwd = await _count_equivalences(conn, "isic_kh", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Laos Tests ────────────────────────────────────────────


class TestIngestIsicLa:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_la

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_la(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_la")
                assert sys is not None
                assert sys["region"] == "Laos"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_la

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_la(conn)
                fwd = await _count_equivalences(conn, "isic_la", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Ethiopia Tests ────────────────────────────────────────


class TestIngestIsicEt:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_et

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_et(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_et")
                assert sys is not None
                assert sys["region"] == "Ethiopia"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_et

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_et(conn)
                fwd = await _count_equivalences(conn, "isic_et", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Tanzania Tests ────────────────────────────────────────


class TestIngestIsicTz:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tz

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_tz(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_tz")
                assert sys is not None
                assert sys["region"] == "Tanzania"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tz

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_tz(conn)
                fwd = await _count_equivalences(conn, "isic_tz", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Ghana Tests ───────────────────────────────────────────


class TestIngestIsicGh:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gh

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_gh(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_gh")
                assert sys is not None
                assert sys["region"] == "Ghana"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gh

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_gh(conn)
                fwd = await _count_equivalences(conn, "isic_gh", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Morocco Tests ─────────────────────────────────────────


class TestIngestIsicMa:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ma

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_ma(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_ma")
                assert sys is not None
                assert sys["region"] == "Morocco"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ma

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_ma(conn)
                fwd = await _count_equivalences(conn, "isic_ma", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── CIIU Paraguay Tests ────────────────────────────────────────


class TestIngestCiiuPy:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_py

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_py(conn)
                assert count == isic_count
                sys = await _get_system(conn, "ciiu_py")
                assert sys is not None
                assert sys["region"] == "Paraguay"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_py

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_py(conn)
                fwd = await _count_equivalences(conn, "ciiu_py", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── CIIU Uruguay Tests ─────────────────────────────────────────


class TestIngestCiiuUy:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_uy

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_uy(conn)
                assert count == isic_count
                sys = await _get_system(conn, "ciiu_uy")
                assert sys is not None
                assert sys["region"] == "Uruguay"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_uy

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_uy(conn)
                fwd = await _count_equivalences(conn, "ciiu_uy", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── CIIU Dominican Republic Tests ──────────────────────────────


class TestIngestCiiuDo:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_do

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_ciiu_do(conn)
                assert count == isic_count
                sys = await _get_system(conn, "ciiu_do")
                assert sys is not None
                assert sys["region"] == "Dominican Republic"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_ciiu_do

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_ciiu_do(conn)
                fwd = await _count_equivalences(conn, "ciiu_do", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Honduras Tests ────────────────────────────────────────


class TestIngestIsicHn:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_hn

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_hn(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_hn")
                assert sys is not None
                assert sys["region"] == "Honduras"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_hn

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_hn(conn)
                fwd = await _count_equivalences(conn, "isic_hn", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC El Salvador Tests ─────────────────────────────────────


class TestIngestIsicSv:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sv

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_sv(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_sv")
                assert sys is not None
                assert sys["region"] == "El Salvador"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sv

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_sv(conn)
                fwd = await _count_equivalences(conn, "isic_sv", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Nicaragua Tests ───────────────────────────────────────


class TestIngestIsicNi:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ni

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_ni(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_ni")
                assert sys is not None
                assert sys["region"] == "Nicaragua"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ni

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_ni(conn)
                fwd = await _count_equivalences(conn, "isic_ni", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Iraq Tests ────────────────────────────────────────────


class TestIngestIsicIq:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_iq

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_iq(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_iq")
                assert sys is not None
                assert sys["region"] == "Iraq"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_iq

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_iq(conn)
                fwd = await _count_equivalences(conn, "isic_iq", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Jordan Tests ──────────────────────────────────────────


class TestIngestIsicJo:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_jo

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_jo(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_jo")
                assert sys is not None
                assert sys["region"] == "Jordan"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_jo

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_jo(conn)
                fwd = await _count_equivalences(conn, "isic_jo", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Tunisia Tests ─────────────────────────────────────────


class TestIngestIsicTn:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tn

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_tn(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_tn")
                assert sys is not None
                assert sys["region"] == "Tunisia"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tn

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_tn(conn)
                fwd = await _count_equivalences(conn, "isic_tn", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Algeria Tests ─────────────────────────────────────────


class TestIngestIsicDz:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_dz

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_dz(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_dz")
                assert sys is not None
                assert sys["region"] == "Algeria"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_dz

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_dz(conn)
                fwd = await _count_equivalences(conn, "isic_dz", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Senegal Tests ─────────────────────────────────────────


class TestIngestIsicSn:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sn

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_sn(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_sn")
                assert sys is not None
                assert sys["region"] == "Senegal"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sn

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_sn(conn)
                fwd = await _count_equivalences(conn, "isic_sn", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Cameroon Tests ────────────────────────────────────────


class TestIngestIsicCm:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cm

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_cm(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_cm")
                assert sys is not None
                assert sys["region"] == "Cameroon"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cm

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_cm(conn)
                fwd = await _count_equivalences(conn, "isic_cm", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Uganda Tests ──────────────────────────────────────────


class TestIngestIsicUg:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ug

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_ug(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_ug")
                assert sys is not None
                assert sys["region"] == "Uganda"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ug

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_ug(conn)
                fwd = await _count_equivalences(conn, "isic_ug", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Mozambique Tests ──────────────────────────────────────


class TestIngestIsicMz:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mz

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_mz(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_mz")
                assert sys is not None
                assert sys["region"] == "Mozambique"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mz

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_mz(conn)
                fwd = await _count_equivalences(conn, "isic_mz", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Nepal Tests ───────────────────────────────────────────


class TestIngestIsicNp:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_np

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_np(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_np")
                assert sys is not None
                assert sys["region"] == "Nepal"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_np

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_np(conn)
                fwd = await _count_equivalences(conn, "isic_np", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── ISIC Zimbabwe Tests ────────────────────────────────────────


class TestIngestIsicZw:

    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_zw

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                count = await ingest_isic_zw(conn)
                assert count == isic_count
                sys = await _get_system(conn, "isic_zw")
                assert sys is not None
                assert sys["region"] == "Zimbabwe"

        _run(_test())

    def test_creates_equivalence_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_zw

        async def _test():
            async with db_pool.acquire() as conn:
                isic_count = await _count_nodes(conn, "isic_rev4")
                await ingest_isic_zw(conn)
                fwd = await _count_equivalences(conn, "isic_zw", "isic_rev4")
                assert fwd == isic_count

        _run(_test())


# ── Trinidad and Tobago ────────────────────────────────────────

class TestIngestIsicTt:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tt
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_tt(conn)
                assert c == ic; s = await _get_system(conn, "isic_tt"); assert s["region"] == "Trinidad and Tobago"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tt
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_tt(conn)
                assert await _count_equivalences(conn, "isic_tt", "isic_rev4") == ic
        _run(_t())

# ── Jamaica ────────────────────────────────────────────────────

class TestIngestIsicJm:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_jm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_jm(conn)
                assert c == ic; s = await _get_system(conn, "isic_jm"); assert s["region"] == "Jamaica"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_jm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_jm(conn)
                assert await _count_equivalences(conn, "isic_jm", "isic_rev4") == ic
        _run(_t())

# ── Haiti ──────────────────────────────────────────────────────

class TestIngestIsicHt:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ht
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ht(conn)
                assert c == ic; s = await _get_system(conn, "isic_ht"); assert s["region"] == "Haiti"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ht
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ht(conn)
                assert await _count_equivalences(conn, "isic_ht", "isic_rev4") == ic
        _run(_t())

# ── Fiji ───────────────────────────────────────────────────────

class TestIngestIsicFj:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_fj
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_fj(conn)
                assert c == ic; s = await _get_system(conn, "isic_fj"); assert s["region"] == "Fiji"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_fj
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_fj(conn)
                assert await _count_equivalences(conn, "isic_fj", "isic_rev4") == ic
        _run(_t())

# ── Papua New Guinea ───────────────────────────────────────────

class TestIngestIsicPg:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_pg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_pg(conn)
                assert c == ic; s = await _get_system(conn, "isic_pg"); assert s["region"] == "Papua New Guinea"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_pg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_pg(conn)
                assert await _count_equivalences(conn, "isic_pg", "isic_rev4") == ic
        _run(_t())

# ── Mongolia ───────────────────────────────────────────────────

class TestIngestIsicMn:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mn
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_mn(conn)
                assert c == ic; s = await _get_system(conn, "isic_mn"); assert s["region"] == "Mongolia"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mn
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_mn(conn)
                assert await _count_equivalences(conn, "isic_mn", "isic_rev4") == ic
        _run(_t())

# ── Kazakhstan ─────────────────────────────────────────────────

class TestIngestIsicKz:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kz
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_kz(conn)
                assert c == ic; s = await _get_system(conn, "isic_kz"); assert s["region"] == "Kazakhstan"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kz
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_kz(conn)
                assert await _count_equivalences(conn, "isic_kz", "isic_rev4") == ic
        _run(_t())

# ── Uzbekistan ─────────────────────────────────────────────────

class TestIngestIsicUz:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_uz
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_uz(conn)
                assert c == ic; s = await _get_system(conn, "isic_uz"); assert s["region"] == "Uzbekistan"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_uz
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_uz(conn)
                assert await _count_equivalences(conn, "isic_uz", "isic_rev4") == ic
        _run(_t())

# ── Azerbaijan ─────────────────────────────────────────────────

class TestIngestIsicAz:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_az
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_az(conn)
                assert c == ic; s = await _get_system(conn, "isic_az"); assert s["region"] == "Azerbaijan"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_az
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_az(conn)
                assert await _count_equivalences(conn, "isic_az", "isic_rev4") == ic
        _run(_t())

# ── Ivory Coast ────────────────────────────────────────────────

class TestIngestIsicCi:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ci
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ci(conn)
                assert c == ic; s = await _get_system(conn, "isic_ci"); assert s["region"] == "Ivory Coast"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ci
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ci(conn)
                assert await _count_equivalences(conn, "isic_ci", "isic_rev4") == ic
        _run(_t())

# ── Rwanda ─────────────────────────────────────────────────────

class TestIngestIsicRw:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_rw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_rw(conn)
                assert c == ic; s = await _get_system(conn, "isic_rw"); assert s["region"] == "Rwanda"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_rw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_rw(conn)
                assert await _count_equivalences(conn, "isic_rw", "isic_rev4") == ic
        _run(_t())

# ── Zambia ─────────────────────────────────────────────────────

class TestIngestIsicZm:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_zm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_zm(conn)
                assert c == ic; s = await _get_system(conn, "isic_zm"); assert s["region"] == "Zambia"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_zm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_zm(conn)
                assert await _count_equivalences(conn, "isic_zm", "isic_rev4") == ic
        _run(_t())

# ── Botswana ───────────────────────────────────────────────────

class TestIngestIsicBw:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bw(conn)
                assert c == ic; s = await _get_system(conn, "isic_bw"); assert s["region"] == "Botswana"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bw(conn)
                assert await _count_equivalences(conn, "isic_bw", "isic_rev4") == ic
        _run(_t())

# ── Namibia ────────────────────────────────────────────────────

class TestIngestIsicNa:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_na
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_na(conn)
                assert c == ic; s = await _get_system(conn, "isic_na"); assert s["region"] == "Namibia"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_na
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_na(conn)
                assert await _count_equivalences(conn, "isic_na", "isic_rev4") == ic
        _run(_t())

# ── Madagascar ─────────────────────────────────────────────────

class TestIngestIsicMg:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_mg(conn)
                assert c == ic; s = await _get_system(conn, "isic_mg"); assert s["region"] == "Madagascar"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_mg(conn)
                assert await _count_equivalences(conn, "isic_mg", "isic_rev4") == ic
        _run(_t())

# ── Mauritius ──────────────────────────────────────────────────

class TestIngestIsicMu:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mu
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_mu(conn)
                assert c == ic; s = await _get_system(conn, "isic_mu"); assert s["region"] == "Mauritius"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mu
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_mu(conn)
                assert await _count_equivalences(conn, "isic_mu", "isic_rev4") == ic
        _run(_t())

# ── Burkina Faso ───────────────────────────────────────────────

class TestIngestIsicBf:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bf
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bf(conn)
                assert c == ic; s = await _get_system(conn, "isic_bf"); assert s["region"] == "Burkina Faso"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bf
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bf(conn)
                assert await _count_equivalences(conn, "isic_bf", "isic_rev4") == ic
        _run(_t())

# ── Mali ───────────────────────────────────────────────────────

class TestIngestIsicMl:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ml
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ml(conn)
                assert c == ic; s = await _get_system(conn, "isic_ml"); assert s["region"] == "Mali"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ml
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ml(conn)
                assert await _count_equivalences(conn, "isic_ml", "isic_rev4") == ic
        _run(_t())

# ── Malawi ─────────────────────────────────────────────────────

class TestIngestIsicMw:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_mw(conn)
                assert c == ic; s = await _get_system(conn, "isic_mw"); assert s["region"] == "Malawi"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_mw(conn)
                assert await _count_equivalences(conn, "isic_mw", "isic_rev4") == ic
        _run(_t())

# ── Afghanistan ────────────────────────────────────────────────

class TestIngestIsicAf:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_af
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_af(conn)
                assert c == ic; s = await _get_system(conn, "isic_af"); assert s["region"] == "Afghanistan"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_af
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_af(conn)
                assert await _count_equivalences(conn, "isic_af", "isic_rev4") == ic
        _run(_t())

# -- Lebanon --

class TestIngestIsicLb:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_lb
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_lb(conn)
                assert c == ic; s = await _get_system(conn, "isic_lb"); assert s["region"] == "Lebanon"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_lb
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_lb(conn)
                assert await _count_equivalences(conn, "isic_lb", "isic_rev4") == ic
        _run(_t())

# -- Oman --

class TestIngestIsicOm:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_om
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_om(conn)
                assert c == ic; s = await _get_system(conn, "isic_om"); assert s["region"] == "Oman"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_om
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_om(conn)
                assert await _count_equivalences(conn, "isic_om", "isic_rev4") == ic
        _run(_t())

# -- Qatar --

class TestIngestIsicQa:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_qa
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_qa(conn)
                assert c == ic; s = await _get_system(conn, "isic_qa"); assert s["region"] == "Qatar"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_qa
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_qa(conn)
                assert await _count_equivalences(conn, "isic_qa", "isic_rev4") == ic
        _run(_t())

# -- Bahrain --

class TestIngestIsicBh:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bh
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bh(conn)
                assert c == ic; s = await _get_system(conn, "isic_bh"); assert s["region"] == "Bahrain"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bh
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bh(conn)
                assert await _count_equivalences(conn, "isic_bh", "isic_rev4") == ic
        _run(_t())

# -- Kuwait --

class TestIngestIsicKw:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_kw(conn)
                assert c == ic; s = await _get_system(conn, "isic_kw"); assert s["region"] == "Kuwait"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_kw(conn)
                assert await _count_equivalences(conn, "isic_kw", "isic_rev4") == ic
        _run(_t())

# -- Yemen --

class TestIngestIsicYe:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ye
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ye(conn)
                assert c == ic; s = await _get_system(conn, "isic_ye"); assert s["region"] == "Yemen"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ye
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ye(conn)
                assert await _count_equivalences(conn, "isic_ye", "isic_rev4") == ic
        _run(_t())

# -- Iran --

class TestIngestIsicIr:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ir
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ir(conn)
                assert c == ic; s = await _get_system(conn, "isic_ir"); assert s["region"] == "Iran"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ir
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ir(conn)
                assert await _count_equivalences(conn, "isic_ir", "isic_rev4") == ic
        _run(_t())

# -- Libya --

class TestIngestIsicLy:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ly
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ly(conn)
                assert c == ic; s = await _get_system(conn, "isic_ly"); assert s["region"] == "Libya"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ly
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ly(conn)
                assert await _count_equivalences(conn, "isic_ly", "isic_rev4") == ic
        _run(_t())

# -- Israel --

class TestIngestIsicIl:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_il
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_il(conn)
                assert c == ic; s = await _get_system(conn, "isic_il"); assert s["region"] == "Israel"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_il
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_il(conn)
                assert await _count_equivalences(conn, "isic_il", "isic_rev4") == ic
        _run(_t())

# -- Palestine --

class TestIngestIsicPs:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ps
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ps(conn)
                assert c == ic; s = await _get_system(conn, "isic_ps"); assert s["region"] == "Palestine"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ps
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ps(conn)
                assert await _count_equivalences(conn, "isic_ps", "isic_rev4") == ic
        _run(_t())

# -- Syria --

class TestIngestIsicSy:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sy
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_sy(conn)
                assert c == ic; s = await _get_system(conn, "isic_sy"); assert s["region"] == "Syria"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sy
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_sy(conn)
                assert await _count_equivalences(conn, "isic_sy", "isic_rev4") == ic
        _run(_t())

# -- Kyrgyzstan --

class TestIngestIsicKg:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_kg(conn)
                assert c == ic; s = await _get_system(conn, "isic_kg"); assert s["region"] == "Kyrgyzstan"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_kg(conn)
                assert await _count_equivalences(conn, "isic_kg", "isic_rev4") == ic
        _run(_t())

# -- Tajikistan --

class TestIngestIsicTj:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tj
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_tj(conn)
                assert c == ic; s = await _get_system(conn, "isic_tj"); assert s["region"] == "Tajikistan"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tj
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_tj(conn)
                assert await _count_equivalences(conn, "isic_tj", "isic_rev4") == ic
        _run(_t())

# -- Turkmenistan --

class TestIngestIsicTm:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_tm(conn)
                assert c == ic; s = await _get_system(conn, "isic_tm"); assert s["region"] == "Turkmenistan"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_tm(conn)
                assert await _count_equivalences(conn, "isic_tm", "isic_rev4") == ic
        _run(_t())

# -- Cuba --

class TestIngestIsicCu:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cu
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_cu(conn)
                assert c == ic; s = await _get_system(conn, "isic_cu"); assert s["region"] == "Cuba"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cu
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_cu(conn)
                assert await _count_equivalences(conn, "isic_cu", "isic_rev4") == ic
        _run(_t())

# -- Barbados --

class TestIngestIsicBb:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bb
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bb(conn)
                assert c == ic; s = await _get_system(conn, "isic_bb"); assert s["region"] == "Barbados"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bb
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bb(conn)
                assert await _count_equivalences(conn, "isic_bb", "isic_rev4") == ic
        _run(_t())

# -- Bahamas --

class TestIngestIsicBs:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bs
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bs(conn)
                assert c == ic; s = await _get_system(conn, "isic_bs"); assert s["region"] == "Bahamas"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bs
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bs(conn)
                assert await _count_equivalences(conn, "isic_bs", "isic_rev4") == ic
        _run(_t())

# -- Guyana --

class TestIngestIsicGy:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gy
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_gy(conn)
                assert c == ic; s = await _get_system(conn, "isic_gy"); assert s["region"] == "Guyana"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gy
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_gy(conn)
                assert await _count_equivalences(conn, "isic_gy", "isic_rev4") == ic
        _run(_t())

# -- Suriname --

class TestIngestIsicSr:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sr
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_sr(conn)
                assert c == ic; s = await _get_system(conn, "isic_sr"); assert s["region"] == "Suriname"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sr
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_sr(conn)
                assert await _count_equivalences(conn, "isic_sr", "isic_rev4") == ic
        _run(_t())

# -- Belize --

class TestIngestIsicBz:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bz
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bz(conn)
                assert c == ic; s = await _get_system(conn, "isic_bz"); assert s["region"] == "Belize"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bz
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bz(conn)
                assert await _count_equivalences(conn, "isic_bz", "isic_rev4") == ic
        _run(_t())

# -- Antigua and Barbuda --

class TestIngestIsicAg:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ag
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ag(conn)
                assert c == ic; s = await _get_system(conn, "isic_ag"); assert s["region"] == "Antigua and Barbuda"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ag
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ag(conn)
                assert await _count_equivalences(conn, "isic_ag", "isic_rev4") == ic
        _run(_t())

# -- Saint Lucia --

class TestIngestIsicLc:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_lc
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_lc(conn)
                assert c == ic; s = await _get_system(conn, "isic_lc"); assert s["region"] == "Saint Lucia"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_lc
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_lc(conn)
                assert await _count_equivalences(conn, "isic_lc", "isic_rev4") == ic
        _run(_t())

# -- Grenada --

class TestIngestIsicGd:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gd
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_gd(conn)
                assert c == ic; s = await _get_system(conn, "isic_gd"); assert s["region"] == "Grenada"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gd
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_gd(conn)
                assert await _count_equivalences(conn, "isic_gd", "isic_rev4") == ic
        _run(_t())

# -- Saint Vincent and the Grenadines --

class TestIngestIsicVc:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_vc
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_vc(conn)
                assert c == ic; s = await _get_system(conn, "isic_vc"); assert s["region"] == "Saint Vincent and the Grenadines"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_vc
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_vc(conn)
                assert await _count_equivalences(conn, "isic_vc", "isic_rev4") == ic
        _run(_t())

# -- Dominica --

class TestIngestIsicDm:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_dm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_dm(conn)
                assert c == ic; s = await _get_system(conn, "isic_dm"); assert s["region"] == "Dominica"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_dm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_dm(conn)
                assert await _count_equivalences(conn, "isic_dm", "isic_rev4") == ic
        _run(_t())

# -- Saint Kitts and Nevis --

class TestIngestIsicKn:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kn
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_kn(conn)
                assert c == ic; s = await _get_system(conn, "isic_kn"); assert s["region"] == "Saint Kitts and Nevis"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_kn
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_kn(conn)
                assert await _count_equivalences(conn, "isic_kn", "isic_rev4") == ic
        _run(_t())

# -- South Sudan --

class TestIngestIsicSs:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ss
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ss(conn)
                assert c == ic; s = await _get_system(conn, "isic_ss"); assert s["region"] == "South Sudan"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ss
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ss(conn)
                assert await _count_equivalences(conn, "isic_ss", "isic_rev4") == ic
        _run(_t())

# -- Somalia --

class TestIngestIsicSo:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_so
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_so(conn)
                assert c == ic; s = await _get_system(conn, "isic_so"); assert s["region"] == "Somalia"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_so
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_so(conn)
                assert await _count_equivalences(conn, "isic_so", "isic_rev4") == ic
        _run(_t())

# -- Guinea --

class TestIngestIsicGn:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gn
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_gn(conn)
                assert c == ic; s = await _get_system(conn, "isic_gn"); assert s["region"] == "Guinea"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gn
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_gn(conn)
                assert await _count_equivalences(conn, "isic_gn", "isic_rev4") == ic
        _run(_t())

# -- Sierra Leone --

class TestIngestIsicSl:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sl
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_sl(conn)
                assert c == ic; s = await _get_system(conn, "isic_sl"); assert s["region"] == "Sierra Leone"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sl
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_sl(conn)
                assert await _count_equivalences(conn, "isic_sl", "isic_rev4") == ic
        _run(_t())

# -- Liberia --

class TestIngestIsicLr:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_lr
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_lr(conn)
                assert c == ic; s = await _get_system(conn, "isic_lr"); assert s["region"] == "Liberia"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_lr
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_lr(conn)
                assert await _count_equivalences(conn, "isic_lr", "isic_rev4") == ic
        _run(_t())

# -- Togo --

class TestIngestIsicTg:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_tg(conn)
                assert c == ic; s = await _get_system(conn, "isic_tg"); assert s["region"] == "Togo"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_tg(conn)
                assert await _count_equivalences(conn, "isic_tg", "isic_rev4") == ic
        _run(_t())

# -- Benin --

class TestIngestIsicBj:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bj
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bj(conn)
                assert c == ic; s = await _get_system(conn, "isic_bj"); assert s["region"] == "Benin"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bj
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bj(conn)
                assert await _count_equivalences(conn, "isic_bj", "isic_rev4") == ic
        _run(_t())

# -- Niger --

class TestIngestIsicNe:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ne
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ne(conn)
                assert c == ic; s = await _get_system(conn, "isic_ne"); assert s["region"] == "Niger"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ne
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ne(conn)
                assert await _count_equivalences(conn, "isic_ne", "isic_rev4") == ic
        _run(_t())

# -- Chad --

class TestIngestIsicTd:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_td
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_td(conn)
                assert c == ic; s = await _get_system(conn, "isic_td"); assert s["region"] == "Chad"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_td
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_td(conn)
                assert await _count_equivalences(conn, "isic_td", "isic_rev4") == ic
        _run(_t())

# -- Democratic Republic of the Congo --

class TestIngestIsicCd:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cd
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_cd(conn)
                assert c == ic; s = await _get_system(conn, "isic_cd"); assert s["region"] == "Democratic Republic of the Congo"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cd
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_cd(conn)
                assert await _count_equivalences(conn, "isic_cd", "isic_rev4") == ic
        _run(_t())

# -- Angola --

class TestIngestIsicAo:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ao
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ao(conn)
                assert c == ic; s = await _get_system(conn, "isic_ao"); assert s["region"] == "Angola"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ao
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ao(conn)
                assert await _count_equivalences(conn, "isic_ao", "isic_rev4") == ic
        _run(_t())

# -- Gabon --

class TestIngestIsicGa:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ga
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ga(conn)
                assert c == ic; s = await _get_system(conn, "isic_ga"); assert s["region"] == "Gabon"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ga
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ga(conn)
                assert await _count_equivalences(conn, "isic_ga", "isic_rev4") == ic
        _run(_t())

# -- Equatorial Guinea --

class TestIngestIsicGq:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gq
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_gq(conn)
                assert c == ic; s = await _get_system(conn, "isic_gq"); assert s["region"] == "Equatorial Guinea"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gq
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_gq(conn)
                assert await _count_equivalences(conn, "isic_gq", "isic_rev4") == ic
        _run(_t())

# -- Republic of the Congo --

class TestIngestIsicCg:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_cg(conn)
                assert c == ic; s = await _get_system(conn, "isic_cg"); assert s["region"] == "Republic of the Congo"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cg
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_cg(conn)
                assert await _count_equivalences(conn, "isic_cg", "isic_rev4") == ic
        _run(_t())

# -- Comoros --

class TestIngestIsicKm:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_km
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_km(conn)
                assert c == ic; s = await _get_system(conn, "isic_km"); assert s["region"] == "Comoros"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_km
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_km(conn)
                assert await _count_equivalences(conn, "isic_km", "isic_rev4") == ic
        _run(_t())

# -- Djibouti --

class TestIngestIsicDj:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_dj
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_dj(conn)
                assert c == ic; s = await _get_system(conn, "isic_dj"); assert s["region"] == "Djibouti"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_dj
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_dj(conn)
                assert await _count_equivalences(conn, "isic_dj", "isic_rev4") == ic
        _run(_t())

# -- Cabo Verde --

class TestIngestIsicCv:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cv
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_cv(conn)
                assert c == ic; s = await _get_system(conn, "isic_cv"); assert s["region"] == "Cabo Verde"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_cv
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_cv(conn)
                assert await _count_equivalences(conn, "isic_cv", "isic_rev4") == ic
        _run(_t())

# -- Gambia --

class TestIngestIsicGm:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_gm(conn)
                assert c == ic; s = await _get_system(conn, "isic_gm"); assert s["region"] == "Gambia"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gm
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_gm(conn)
                assert await _count_equivalences(conn, "isic_gm", "isic_rev4") == ic
        _run(_t())

# -- Guinea-Bissau --

class TestIngestIsicGw:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_gw(conn)
                assert c == ic; s = await _get_system(conn, "isic_gw"); assert s["region"] == "Guinea-Bissau"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_gw
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_gw(conn)
                assert await _count_equivalences(conn, "isic_gw", "isic_rev4") == ic
        _run(_t())

# -- Mauritania --

class TestIngestIsicMr:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mr
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_mr(conn)
                assert c == ic; s = await _get_system(conn, "isic_mr"); assert s["region"] == "Mauritania"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mr
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_mr(conn)
                assert await _count_equivalences(conn, "isic_mr", "isic_rev4") == ic
        _run(_t())

# -- Eswatini --

class TestIngestIsicSz:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sz
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_sz(conn)
                assert c == ic; s = await _get_system(conn, "isic_sz"); assert s["region"] == "Eswatini"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sz
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_sz(conn)
                assert await _count_equivalences(conn, "isic_sz", "isic_rev4") == ic
        _run(_t())

# -- Lesotho --

class TestIngestIsicLs:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ls
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ls(conn)
                assert c == ic; s = await _get_system(conn, "isic_ls"); assert s["region"] == "Lesotho"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ls
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ls(conn)
                assert await _count_equivalences(conn, "isic_ls", "isic_rev4") == ic
        _run(_t())

# -- Burundi --

class TestIngestIsicBi:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bi
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bi(conn)
                assert c == ic; s = await _get_system(conn, "isic_bi"); assert s["region"] == "Burundi"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bi
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bi(conn)
                assert await _count_equivalences(conn, "isic_bi", "isic_rev4") == ic
        _run(_t())

# -- Eritrea --

class TestIngestIsicEr:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_er
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_er(conn)
                assert c == ic; s = await _get_system(conn, "isic_er"); assert s["region"] == "Eritrea"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_er
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_er(conn)
                assert await _count_equivalences(conn, "isic_er", "isic_rev4") == ic
        _run(_t())

# -- Seychelles --

class TestIngestIsicSc:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sc
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_sc(conn)
                assert c == ic; s = await _get_system(conn, "isic_sc"); assert s["region"] == "Seychelles"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sc
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_sc(conn)
                assert await _count_equivalences(conn, "isic_sc", "isic_rev4") == ic
        _run(_t())

# -- Samoa --

class TestIngestIsicWs:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ws
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_ws(conn)
                assert c == ic; s = await _get_system(conn, "isic_ws"); assert s["region"] == "Samoa"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_ws
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_ws(conn)
                assert await _count_equivalences(conn, "isic_ws", "isic_rev4") == ic
        _run(_t())

# -- Tonga --

class TestIngestIsicTo:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_to
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_to(conn)
                assert c == ic; s = await _get_system(conn, "isic_to"); assert s["region"] == "Tonga"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_to
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_to(conn)
                assert await _count_equivalences(conn, "isic_to", "isic_rev4") == ic
        _run(_t())

# -- Vanuatu --

class TestIngestIsicVu:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_vu
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_vu(conn)
                assert c == ic; s = await _get_system(conn, "isic_vu"); assert s["region"] == "Vanuatu"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_vu
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_vu(conn)
                assert await _count_equivalences(conn, "isic_vu", "isic_rev4") == ic
        _run(_t())

# -- Solomon Islands --

class TestIngestIsicSb:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sb
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_sb(conn)
                assert c == ic; s = await _get_system(conn, "isic_sb"); assert s["region"] == "Solomon Islands"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_sb
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_sb(conn)
                assert await _count_equivalences(conn, "isic_sb", "isic_rev4") == ic
        _run(_t())

# -- Brunei --

class TestIngestIsicBn:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bn
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bn(conn)
                assert c == ic; s = await _get_system(conn, "isic_bn"); assert s["region"] == "Brunei"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bn
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bn(conn)
                assert await _count_equivalences(conn, "isic_bn", "isic_rev4") == ic
        _run(_t())

# -- East Timor --

class TestIngestIsicTl:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tl
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_tl(conn)
                assert c == ic; s = await _get_system(conn, "isic_tl"); assert s["region"] == "East Timor"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_tl
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_tl(conn)
                assert await _count_equivalences(conn, "isic_tl", "isic_rev4") == ic
        _run(_t())

# -- Bhutan --

class TestIngestIsicBt:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bt
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_bt(conn)
                assert c == ic; s = await _get_system(conn, "isic_bt"); assert s["region"] == "Bhutan"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_bt
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_bt(conn)
                assert await _count_equivalences(conn, "isic_bt", "isic_rev4") == ic
        _run(_t())

# -- Maldives --

class TestIngestIsicMv:
    def test_creates_system(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mv
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); c = await ingest_isic_mv(conn)
                assert c == ic; s = await _get_system(conn, "isic_mv"); assert s["region"] == "Maldives"
        _run(_t())
    def test_edges(self, db_pool):
        from world_of_taxonomy.ingest.isic_derived import ingest_isic_mv
        async def _t():
            async with db_pool.acquire() as conn:
                ic = await _count_nodes(conn, "isic_rev4"); await ingest_isic_mv(conn)
                assert await _count_equivalences(conn, "isic_mv", "isic_rev4") == ic
        _run(_t())