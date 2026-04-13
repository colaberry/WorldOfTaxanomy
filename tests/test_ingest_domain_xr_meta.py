"""Tests for Extended Reality and Metaverse domain taxonomy ingester.

RED tests - written before any implementation exists.

XR/Metaverse taxonomy organizes extended reality and metaverse sector types:
  Virtual Reality   (dxr_vr*)     - headsets, standalone VR, tethered VR, CAVE
  Augmented Reality (dxr_ar*)     - smart glasses, mobile AR, industrial AR
  Mixed Reality     (dxr_mr*)     - spatial computing, holographic displays
  XR Hardware       (dxr_hw*)     - displays, tracking, haptics, input devices
  XR Platforms      (dxr_plat*)   - metaverse platforms, app stores, SDK
  XR Content        (dxr_content*)- games, enterprise training, virtual events
  Digital Twins     (dxr_dt*)     - industrial DT, city DT, product DT

Source: NAICS 5112 + 5415 + 3342 XR/metaverse industry structure. Hand-coded. Open.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.domain_xr_meta import (
    XR_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_xr_meta,
)


class TestDetermineLevel:
    def test_vr_category_is_level_1(self):
        assert _determine_level("dxr_vr") == 1

    def test_vr_type_is_level_2(self):
        assert _determine_level("dxr_vr_standalone") == 2

    def test_ar_category_is_level_1(self):
        assert _determine_level("dxr_ar") == 1

    def test_ar_type_is_level_2(self):
        assert _determine_level("dxr_ar_glasses") == 2

    def test_dt_category_is_level_1(self):
        assert _determine_level("dxr_dt") == 1

    def test_dt_type_is_level_2(self):
        assert _determine_level("dxr_dt_industrial") == 2


class TestDetermineParent:
    def test_vr_category_has_no_parent(self):
        assert _determine_parent("dxr_vr") is None

    def test_vr_standalone_parent_is_vr(self):
        assert _determine_parent("dxr_vr_standalone") == "dxr_vr"

    def test_ar_glasses_parent_is_ar(self):
        assert _determine_parent("dxr_ar_glasses") == "dxr_ar"

    def test_dt_industrial_parent_is_dt(self):
        assert _determine_parent("dxr_dt_industrial") == "dxr_dt"


class TestXRNodes:
    def test_nodes_list_is_non_empty(self):
        assert len(XR_NODES) > 0

    def test_has_vr_category(self):
        codes = [n[0] for n in XR_NODES]
        assert "dxr_vr" in codes

    def test_has_ar_category(self):
        codes = [n[0] for n in XR_NODES]
        assert "dxr_ar" in codes

    def test_has_xr_platforms_category(self):
        codes = [n[0] for n in XR_NODES]
        assert "dxr_plat" in codes

    def test_has_digital_twins_category(self):
        codes = [n[0] for n in XR_NODES]
        assert "dxr_dt" in codes

    def test_all_titles_non_empty(self):
        for code, title, level, parent in XR_NODES:
            assert title.strip(), f"Empty title for '{code}'"

    def test_no_duplicate_codes(self):
        codes = [n[0] for n in XR_NODES]
        assert len(codes) == len(set(codes))

    def test_level_1_has_no_parent(self):
        for code, title, level, parent in XR_NODES:
            if level == 1:
                assert parent is None

    def test_level_2_has_parent(self):
        for code, title, level, parent in XR_NODES:
            if level == 2:
                assert parent is not None

    def test_minimum_node_count(self):
        assert len(XR_NODES) >= 18

    def test_no_em_dashes_in_titles(self):
        for code, title, level, parent in XR_NODES:
            assert "\u2014" not in title, f"Em-dash in title for '{code}'"


def test_domain_xr_meta_module_importable():
    assert callable(ingest_domain_xr_meta)
    assert isinstance(XR_NODES, list)


def test_ingest_domain_xr_meta(db_pool):
    """Integration test: XR/metaverse taxonomy rows + NAICS links."""
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count = await ingest_domain_xr_meta(conn)
            assert count > 0

            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_xr_meta'"
            )
            assert row is not None
            assert row["code_count"] == count

            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM node_taxonomy_link "
                "WHERE taxonomy_id = 'domain_xr_meta'"
            )
            assert link_count > 0

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_xr_meta_idempotent(db_pool):
    async def _run():
        from world_of_taxanomy.ingest.naics import ingest_naics_2022
        async with db_pool.acquire() as conn:
            await ingest_naics_2022(conn)
            count1 = await ingest_domain_xr_meta(conn)
            count2 = await ingest_domain_xr_meta(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
