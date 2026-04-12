# Contributing to WorldOfTaxanomy

Thank you for your interest in contributing. This guide covers how to add a new classification system or domain deep-dive taxonomy.

---

## Non-Negotiable Rules

1. **TDD - Red, Green, Refactor.** Write the failing test first. Run it red. Then write the minimum implementation to make it green. Never skip the red step.
2. **No em-dashes.** Never use the em-dash character (U+2014) anywhere - code, comments, docstrings, markdown, or config. Use a hyphen `-` instead. CI enforces this.
3. **One system per PR.** Complete all steps (tests, ingester, CLI registration, docs update) in a single PR.

---

## How to Add a New Classification System

### Step 1 - Write RED tests first (mandatory)

Create `tests/test_ingest_<system>.py` before writing any implementation code.

```python
"""Tests for <System> ingester.

RED tests - written before any implementation exists.
"""
import asyncio
import pytest
from world_of_taxanomy.ingest.<system> import (
    _determine_level,
    _determine_parent,
    _determine_sector,
    ingest_<system>,
)


class TestDetermineLevel:
    def test_top_level_is_1(self):
        assert _determine_level("A") == 1

    def test_second_level_is_2(self):
        assert _determine_level("A01") == 2

    # 4-6 test cases total


class TestDetermineParent:
    def test_root_has_no_parent(self):
        assert _determine_parent("A") is None

    def test_subsection_parent_is_section(self):
        assert _determine_parent("A01") == "A"

    # 4-6 test cases total


class TestDetermineSector:
    def test_sector_of_root(self):
        assert _determine_sector("A") == "A"

    # 4-6 test cases total


def test_ingest_<system>(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count = await ingest_<system>(conn)
            assert count > 0
            row = await conn.fetchrow(
                "SELECT node_count FROM classification_system WHERE id = '<system_id>'"
            )
            assert row["node_count"] == count
    asyncio.get_event_loop().run_until_complete(_run())


def test_ingest_<system>_idempotent(db_pool):
    async def _run():
        async with db_pool.acquire() as conn:
            count1 = await ingest_<system>(conn)
            count2 = await ingest_<system>(conn)
            assert count1 == count2
    asyncio.get_event_loop().run_until_complete(_run())
```

Run it - every test must FAIL (red). If any test passes without any implementation, the test is wrong.

```bash
python3 -m pytest tests/test_ingest_<system>.py -v
# Expected: all FAIL or ERROR
```

Commit the failing tests:

```bash
git add tests/test_ingest_<system>.py
git commit -m "test: RED tests for <system> ingester"
```

### Step 2 - Write the ingester

Create `world_of_taxanomy/ingest/<system>.py`:

```python
"""<System Name> ingester.

Source: <URL>
License: <License>
"""
from __future__ import annotations
from typing import Optional
from world_of_taxanomy.ingest.base import ensure_data_file

DATA_URL = "<download_url>"
DATA_PATH = "data/<filename>"


def _determine_level(code: str) -> int:
    """Return hierarchy depth: 1 = top-level sector."""
    ...


def _determine_parent(code: str) -> Optional[str]:
    """Return parent code, or None for root nodes."""
    ...


def _determine_sector(code: str) -> str:
    """Return the top-level sector code."""
    ...


async def ingest_<system>(conn, path=None) -> int:
    """Ingest <System> into the database. Returns node count."""
    path = path or DATA_PATH
    await ensure_data_file(DATA_URL, path)

    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "<system_id>", "<Name>", "<Full Name>", "<version>", "<region>", "<authority>",
    )

    nodes = []  # parse file into (code, title, level, parent, sector) tuples
    parent_set = {n[3] for n in nodes if n[3] is not None}

    rows = [
        ("<system_id>", code, title, level, parent, sector, code not in parent_set)
        for code, title, level, parent, sector in nodes
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(nodes)
    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = '<system_id>'",
        count,
    )
    return count
```

For large systems (>10K nodes), use chunked inserts (chunk size 500).

For hand-coded systems (small, no download needed), skip `ensure_data_file` and define the data inline.

### Step 3 - Run tests green

```bash
python3 -m pytest tests/test_ingest_<system>.py -v
# Expected: all PASS

python3 -m pytest tests/ -v --tb=short
# Expected: zero failures
```

If any test still fails, fix the implementation. Do not change the test to make it pass.

### Step 4 - Register in the CLI

Edit `world_of_taxanomy/__main__.py`:

1. Add `"<system_id>"` to the `choices` list
2. Add a dispatch block:

```python
if target in ("<system_id>", "all"):
    from world_of_taxanomy.ingest.<module> import ingest_<system>
    print("\n-- <Description> --")
    n = await ingest_<system>(conn)
    print(f"  {n} nodes")
```

### Step 5 - Update documentation

- `CLAUDE.md` - add a row to the systems table with actual code count
- `DATA_SOURCES.md` - add attribution row (system ID, authority, license, URL)
- `CHANGELOG.md` - add entry under `[Unreleased]`

### Step 6 - Commit

```bash
git add world_of_taxanomy/ingest/<system>.py tests/test_ingest_<system>.py
git add world_of_taxanomy/__main__.py CLAUDE.md DATA_SOURCES.md CHANGELOG.md
git commit -m "feat: ingest <system> (<N> codes, TDD green)"
```

Verify the RED commit appears before the GREEN commit:

```bash
git log --oneline -5
# Should show:
# <hash> feat: ingest <system> (<N> codes, TDD green)
# <hash> test: RED tests for <system> ingester
```

---

## How to Add a Domain Deep-Dive Taxonomy

Domain deep-dives attach below a single classification node to model the internal structure of a specific industry. The Truck Transportation deep-dive (Phase 9) is the reference implementation.

### Pattern differences from regular ingesters

1. Register in **both** `classification_system` (FK requirement) and `domain_taxonomy`
2. Update **both** `classification_system.node_count` and `domain_taxonomy.code_count`
3. Link parent industry nodes via `node_taxonomy_link` (not `equivalence`)
4. Code prefix convention: 3-letter prefix, e.g. `dtf_` (domain truck freight)

```python
# Register in classification_system (required by FK on classification_node)
await conn.execute(
    """INSERT INTO classification_system (id, name, full_name, version, region, authority, node_count)
       VALUES ($1, $2, $3, $4, $5, $6, 0) ON CONFLICT (id) DO UPDATE SET node_count = 0""",
    "domain_<industry>_<type>", ...
)

# Also register in domain_taxonomy
await conn.execute(
    """INSERT INTO domain_taxonomy (id, name, full_name, authority, url, code_count)
       VALUES ($1, $2, $3, $4, $5, 0) ON CONFLICT (id) DO UPDATE SET code_count = 0""",
    "domain_<industry>_<type>", ...
)

# After inserting nodes, update both tables
await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = '...'", count)
await conn.execute("UPDATE domain_taxonomy SET code_count = $1 WHERE id = '...'", count)

# Link industry nodes to this domain taxonomy
await conn.executemany(
    """INSERT INTO node_taxonomy_link (system_id, node_code, taxonomy_id, relevance)
       VALUES ($1, $2, $3, $4) ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
    [("naics_2022", naics_code, "domain_<industry>_<type>", "primary") for naics_code in naics_codes],
)
```

See `world_of_taxanomy/ingest/domain_truck_freight.py` as the canonical reference.

---

## Code Style

- No em-dashes (U+2014) anywhere - CI will reject the PR
- No speculative code - implement only what a test requires
- Backend: Python type hints on all public functions
- Frontend: TypeScript strict mode, no `any`
- All async database functions use `asyncpg` connection parameter `conn`

## Running Tests

```bash
# Full suite
python3 -m pytest tests/ -v

# Single file
python3 -m pytest tests/test_ingest_naics.py -v

# With coverage
python3 -m pytest tests/ --cov=world_of_taxanomy
```

Tests use a `test_wot` PostgreSQL schema isolated from production. Never query the `public` schema in tests.

## Questions

Open an issue at https://github.com/colaberry/WorldOfTaxanomy/issues.
