"""Tests for ICD-11 -> ISIC Rev 4 semantic crosswalk.

RED tests - written before any implementation exists.

This is a hand-coded semantic crosswalk linking ICD-11 chapter-level codes
to ISIC Rev 4 industry divisions. These edges represent the relationship
"this disease/condition is predominantly associated with this industry sector."

~50 edges: ICD-11 chapters (L1) -> ISIC Rev 4 sections/divisions.
match_type: 'broad' (many-to-many semantic associations).

No external data file required - links are hand-coded in the ingester.
"""
import asyncio
import pytest

from world_of_taxanomy.ingest.crosswalk_icd_isic import (
    ICD_ISIC_LINKS,
    ingest_crosswalk_icd_isic,
)


def test_crosswalk_icd_isic_module_importable():
    """Function and links table are importable."""
    assert callable(ingest_crosswalk_icd_isic)
    assert isinstance(ICD_ISIC_LINKS, list)


def test_icd_isic_links_not_empty():
    """At least 40 semantic links are defined."""
    assert len(ICD_ISIC_LINKS) >= 40, f"Expected >= 40 links, got {len(ICD_ISIC_LINKS)}"


def test_icd_isic_links_have_required_fields():
    """Every link is a 5-tuple with expected types."""
    for link in ICD_ISIC_LINKS:
        assert len(link) == 5, f"Link must have 5 fields: {link}"
        src_sys, src_code, tgt_sys, tgt_code, match = link
        assert src_sys == "icd_11", f"source_system must be icd_11: {link}"
        assert tgt_sys == "isic_rev4", f"target_system must be isic_rev4: {link}"
        assert match in ("broad", "partial", "exact"), f"match_type invalid: {link}"
        assert src_code, "src_code must be non-empty"
        assert tgt_code, "tgt_code must be non-empty"


def test_icd_isic_links_no_duplicates():
    """No duplicate (src_code, tgt_code) pairs."""
    pairs = [(src, tgt) for _, src, _, tgt, _ in ICD_ISIC_LINKS]
    assert len(pairs) == len(set(pairs)), "Duplicate links found"


def test_ingest_crosswalk_icd_isic(db_pool):
    """Integration test - inserts ICD-11 -> ISIC semantic edges."""
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_crosswalk_icd_isic(conn)
            # All links should insert (test DB has ISIC seeded in conftest)
            assert count >= 30, f"Expected >= 30 edges, got {count}"

            # All edges should be match_type='broad'
            broad = await conn.fetchval(
                "SELECT COUNT(*) FROM equivalence "
                "WHERE source_system = 'icd_11' AND target_system = 'isic_rev4'"
            )
            assert broad >= 30, f"Expected >= 30 icd_11->isic edges, got {broad}"

    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_crosswalk_icd_isic_idempotent(db_pool):
    """Running ingest twice returns consistent count."""
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_crosswalk_icd_isic(conn)
            count2 = await ingest_crosswalk_icd_isic(conn)
            assert count1 == count2

    asyncio.get_event_loop().run_until_complete(_run())
