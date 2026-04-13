"""Tests for domain_workforce_safety ingester (Phase 29 - Cross-cutting)."""
from __future__ import annotations

import asyncio
import pytest
from world_of_taxanomy.ingest.domain_workforce_safety import (
    WORKFORCE_SAFETY_NODES,
    _determine_level,
    _determine_parent,
    ingest_domain_workforce_safety,
)


class TestWorkforceSafetyDetermineLevel:
    def test_top_level_category(self):
        assert _determine_level("dws_osha") == 1

    def test_sub_level_node(self):
        assert _determine_level("dws_osha_general") == 2

    def test_another_top_level(self):
        assert _determine_level("dws_hazard") == 1

    def test_another_sub_level(self):
        assert _determine_level("dws_hazard_chemical") == 2

    def test_ppe_top(self):
        assert _determine_level("dws_ppe") == 1

    def test_ppe_sub(self):
        assert _determine_level("dws_ppe_head") == 2


class TestWorkforceSafetyDetermineParent:
    def test_top_level_has_no_parent(self):
        assert _determine_parent("dws_osha") is None

    def test_sub_level_returns_parent(self):
        assert _determine_parent("dws_osha_general") == "dws_osha"

    def test_another_top_level_none(self):
        assert _determine_parent("dws_hazard") is None

    def test_another_sub_returns_parent(self):
        assert _determine_parent("dws_hazard_chemical") == "dws_hazard"

    def test_ppe_sub_returns_parent(self):
        assert _determine_parent("dws_ppe_head") == "dws_ppe"

    def test_incident_sub_returns_parent(self):
        assert _determine_parent("dws_incident_near") == "dws_incident"


class TestWorkforceSafetyNodes:
    def test_nodes_is_list(self):
        assert isinstance(WORKFORCE_SAFETY_NODES, list)

    def test_at_least_15_nodes(self):
        assert len(WORKFORCE_SAFETY_NODES) >= 15

    def test_all_tuples_four_elements(self):
        for node in WORKFORCE_SAFETY_NODES:
            assert len(node) == 4

    def test_top_level_nodes_have_no_parent(self):
        for code, _title, level, parent in WORKFORCE_SAFETY_NODES:
            if level == 1:
                assert parent is None, f"{code} level 1 should have no parent"

    def test_sub_nodes_have_parent(self):
        for code, _title, level, parent in WORKFORCE_SAFETY_NODES:
            if level == 2:
                assert parent is not None, f"{code} level 2 should have a parent"

    def test_no_em_dashes(self):
        for code, title, _level, _parent in WORKFORCE_SAFETY_NODES:
            assert "\u2014" not in title, f"em-dash found in title: {title}"
            assert "\u2014" not in code, f"em-dash found in code: {code}"


def test_ingest_domain_workforce_safety(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_domain_workforce_safety(conn)
            assert count == len(WORKFORCE_SAFETY_NODES)
            assert count >= 15
            row = await conn.fetchrow(
                "SELECT id, code_count FROM domain_taxonomy "
                "WHERE id = 'domain_workforce_safety'"
            )
            assert row is not None
            assert row["code_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_domain_workforce_safety_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_domain_workforce_safety(conn)
            count2 = await ingest_domain_workforce_safety(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
