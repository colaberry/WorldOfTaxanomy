#!/usr/bin/env python3
"""Export systems list and crosswalk stats as static JSON for the frontend.

Follows the Karpathy Wiki Pattern: data lives as files in the repo,
copied to frontend/src/content/ at build time, read from filesystem
at render time. Zero network calls on page load.

Usage:
    python scripts/export_crosswalk_data.py

Requires DATABASE_URL in environment (same as the API server).
Writes to crosswalk-data/ at the repo root.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUTPUT_DIR = PROJECT_ROOT / "crosswalk-data"


async def main():
    from world_of_taxonomy.db import get_pool, close_pool

    pool = await get_pool()
    async with pool.acquire() as conn:

        # Export systems
        rows = await conn.fetch("""
            SELECT id, name, full_name, region, version, authority, url,
                   tint_color, node_count, source_url, source_date,
                   data_provenance, license, source_file_hash
            FROM classification_system
            ORDER BY name
        """)
        systems = [dict(r) for r in rows]
        # Convert date objects to strings
        for s in systems:
            for k, v in s.items():
                if hasattr(v, 'isoformat'):
                    s[k] = v.isoformat()

        # Export crosswalk stats
        stat_rows = await conn.fetch("""
            SELECT source_system, target_system,
                   COUNT(*) AS edge_count,
                   COUNT(*) FILTER (WHERE match_type = 'exact') AS exact_count,
                   COUNT(*) FILTER (WHERE match_type != 'exact') AS partial_count
            FROM equivalence
            GROUP BY source_system, target_system
            ORDER BY source_system, target_system
        """)
        stats = [dict(r) for r in stat_rows]
        # int conversion for json
        for s in stats:
            s['edge_count'] = int(s['edge_count'])
            s['exact_count'] = int(s['exact_count'])
            s['partial_count'] = int(s['partial_count'])

    await close_pool()

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    systems_path = OUTPUT_DIR / "systems.json"
    systems_path.write_text(json.dumps(systems, indent=2))
    print(f"Wrote {systems_path} ({len(systems)} systems)")

    stats_path = OUTPUT_DIR / "stats.json"
    stats_path.write_text(json.dumps(stats, indent=2))
    print(f"Wrote {stats_path} ({len(stats)} stat pairs)")


if __name__ == "__main__":
    asyncio.run(main())
