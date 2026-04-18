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

        # ── Per-pair crosswalk data (sections + graph) ──────────────────
        print("\nComputing per-pair crosswalk data...")

        # Fetch all edges with titles (one query for everything)
        all_edges = await conn.fetch("""
            SELECT e.source_system, e.source_code, e.target_system, e.target_code,
                   e.match_type,
                   s.title AS source_title, t.title AS target_title
            FROM equivalence e
            LEFT JOIN classification_node s
              ON s.system_id = e.source_system AND s.code = e.source_code
            LEFT JOIN classification_node t
              ON t.system_id = e.target_system AND t.code = e.target_code
        """)
        print(f"  Fetched {len(all_edges)} total edges")

        # Collect systems that appear in crosswalks
        crosswalk_sys_ids = set()
        for e in all_edges:
            crosswalk_sys_ids.add(e["source_system"])
            crosswalk_sys_ids.add(e["target_system"])

        # Fetch node hierarchy for crosswalked systems only
        node_rows = await conn.fetch(
            "SELECT system_id, code, parent_code, title "
            "FROM classification_node WHERE system_id = ANY($1::text[])",
            list(crosswalk_sys_ids),
        )
        print(f"  Fetched {len(node_rows)} nodes for {len(crosswalk_sys_ids)} systems")

        # Build parent/title lookups
        parent_map: dict[tuple, str | None] = {}
        title_map: dict[tuple, str] = {}
        for r in node_rows:
            parent_map[(r["system_id"], r["code"])] = r["parent_code"]
            title_map[(r["system_id"], r["code"])] = r["title"]

        # Root ancestor cache
        root_cache: dict[tuple, str] = {}

        def find_root(sys_id: str, code: str) -> str:
            key = (sys_id, code)
            if key in root_cache:
                return root_cache[key]
            chain: list[str] = []
            cur = code
            visited: set[str] = set()
            while cur and (sys_id, cur) in parent_map:
                chain.append(cur)
                parent = parent_map[(sys_id, cur)]
                if parent is None or parent in visited:
                    break
                visited.add(cur)
                cur = parent
            root = cur if cur else code
            for c in chain:
                root_cache[(sys_id, c)] = root
            return root

        # Group edges by canonical pair (sorted system IDs)
        from collections import defaultdict

        pair_edges: dict[tuple, list] = defaultdict(list)
        for e in all_edges:
            pair_key = tuple(sorted([e["source_system"], e["target_system"]]))
            pair_edges[pair_key].append(dict(e))

        print(f"  Found {len(pair_edges)} unique system pairs")

    await close_pool()

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    systems_path = OUTPUT_DIR / "systems.json"
    systems_path.write_text(json.dumps(systems, indent=2))
    print(f"Wrote {systems_path} ({len(systems)} systems)")

    stats_path = OUTPUT_DIR / "stats.json"
    stats_path.write_text(json.dumps(stats, indent=2))
    print(f"Wrote {stats_path} ({len(stats)} stat pairs)")

    # Write per-pair files
    pair_count = 0
    total_pair_bytes = 0
    for (sys_a, sys_b), edges in pair_edges.items():
        source_system, target_system = sys_a, sys_b

        # Deduplicate edges (canonical direction)
        seen: set[tuple] = set()
        deduped: list[dict] = []
        for e in edges:
            canon = tuple(sorted([
                (e["source_system"], e["source_code"]),
                (e["target_system"], e["target_code"]),
            ]))
            if canon not in seen:
                seen.add(canon)
                deduped.append(e)

        # Build sections (group by root ancestor pair)
        section_groups: dict[tuple, dict] = {}
        for e in deduped:
            src_root = find_root(e["source_system"], e["source_code"])
            tgt_root = find_root(e["target_system"], e["target_code"])
            if e["source_system"] == source_system:
                key = (src_root, tgt_root)
            else:
                key = (tgt_root, src_root)
            if key not in section_groups:
                s_code, t_code = key
                section_groups[key] = {
                    "source_section": s_code,
                    "source_title": title_map.get(
                        (source_system, s_code), s_code
                    ),
                    "target_section": t_code,
                    "target_title": title_map.get(
                        (target_system, t_code), t_code
                    ),
                    "edge_count": 0,
                    "exact_count": 0,
                }
            section_groups[key]["edge_count"] += 1
            if e["match_type"] == "exact":
                section_groups[key]["exact_count"] += 1

        sections_list = sorted(
            section_groups.values(), key=lambda s: -s["edge_count"]
        )

        # Build graph nodes (with root annotation for section filtering)
        node_map: dict[str, dict] = {}
        graph_edges: list[dict] = []
        for e in deduped:
            src_id = f"{e['source_system']}:{e['source_code']}"
            tgt_id = f"{e['target_system']}:{e['target_code']}"
            if src_id not in node_map:
                node_map[src_id] = {
                    "id": src_id,
                    "system": e["source_system"],
                    "code": e["source_code"],
                    "title": e["source_title"] or e["source_code"],
                    "root": find_root(e["source_system"], e["source_code"]),
                }
            if tgt_id not in node_map:
                node_map[tgt_id] = {
                    "id": tgt_id,
                    "system": e["target_system"],
                    "code": e["target_code"],
                    "title": e["target_title"] or e["target_code"],
                    "root": find_root(e["target_system"], e["target_code"]),
                }
            graph_edges.append({
                "source": src_id,
                "target": tgt_id,
                "match_type": e["match_type"],
            })

        pair_data = {
            "source_system": source_system,
            "target_system": target_system,
            "sections": {
                "source_system": source_system,
                "target_system": target_system,
                "sections": sections_list,
                "total_edges": len(deduped),
            },
            "graph": {
                "source_system": source_system,
                "target_system": target_system,
                "nodes": list(node_map.values()),
                "edges": graph_edges,
                "total_edges": len(deduped),
                "truncated": False,
            },
        }

        pair_path = OUTPUT_DIR / f"pair__{source_system}___{target_system}.json"
        content = json.dumps(pair_data, separators=(",", ":"))
        pair_path.write_text(content)
        total_pair_bytes += len(content)
        pair_count += 1

    print(
        f"Wrote {pair_count} pair files "
        f"({total_pair_bytes / 1024 / 1024:.1f} MB total)"
    )


if __name__ == "__main__":
    asyncio.run(main())
