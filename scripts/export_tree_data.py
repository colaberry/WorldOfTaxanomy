#!/usr/bin/env python3
"""Export first 3 levels of each classification system as static JSON.

Follows the Karpathy Wiki Pattern: data lives as files in the repo,
copied to frontend/src/content/ at build time, read from filesystem
at render time. Zero network calls on page load.

Usage:
    python scripts/export_tree_data.py

Requires DATABASE_URL in environment (same as the API server).
Writes to tree-data/ at the repo root.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUTPUT_DIR = PROJECT_ROOT / "tree-data"


async def main():
    from world_of_taxonomy.db import get_pool, close_pool

    pool = await get_pool()
    async with pool.acquire() as conn:

        # Get all systems
        system_rows = await conn.fetch(
            "SELECT id, name, node_count FROM classification_system ORDER BY name"
        )
        print(f"Found {len(system_rows)} systems")

        index = {}
        total_nodes = 0

        for sys_row in system_rows:
            system_id = sys_row["id"]

            # Find the minimum level for this system
            min_level = await conn.fetchval(
                "SELECT MIN(level) FROM classification_node WHERE system_id = $1",
                system_id,
            )
            if min_level is None:
                continue

            max_level = min_level + 2  # 3 levels total

            # Fetch first 3 levels of nodes
            node_rows = await conn.fetch(
                """SELECT code, title, level, parent_code, sector_code, is_leaf,
                          COALESCE(seq_order, 0) AS seq_order
                   FROM classification_node
                   WHERE system_id = $1 AND level <= $2
                   ORDER BY level, seq_order, code""",
                system_id, max_level,
            )

            if not node_rows:
                continue

            # Pre-compute children_count for each node
            # (how many direct children exist in the DB, not just in our subset)
            child_counts = await conn.fetch(
                """SELECT parent_code, COUNT(*) AS cnt
                   FROM classification_node
                   WHERE system_id = $1 AND parent_code IS NOT NULL
                   GROUP BY parent_code""",
                system_id,
            )
            child_count_map = {r["parent_code"]: int(r["cnt"]) for r in child_counts}

            nodes = []
            for r in node_rows:
                nodes.append({
                    "code": r["code"],
                    "title": r["title"],
                    "level": r["level"],
                    "parent_code": r["parent_code"],
                    "sector_code": r["sector_code"],
                    "is_leaf": r["is_leaf"],
                    "children_count": child_count_map.get(r["code"], 0),
                })

            index[system_id] = {
                "node_count": len(nodes),
                "max_level": max_level,
                "total_system_nodes": sys_row["node_count"],
            }
            total_nodes += len(nodes)

            # Write per-system file
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            file_path = OUTPUT_DIR / f"{system_id}.json"
            file_path.write_text(json.dumps(nodes, separators=(",", ":")))

        # Write index
        index_path = OUTPUT_DIR / "index.json"
        index_path.write_text(json.dumps(index, indent=2))

    await close_pool()

    print(f"Wrote {len(index)} system files to {OUTPUT_DIR}/")
    print(f"Total nodes exported: {total_nodes:,}")
    print(f"Index: {index_path}")


if __name__ == "__main__":
    asyncio.run(main())
