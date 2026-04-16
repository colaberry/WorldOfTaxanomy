"""Cross-system equivalence queries."""

from typing import Dict, List, Optional

from world_of_taxonomy.models import Equivalence


def _row_to_equivalence(row) -> Equivalence:
    """Convert a database row to an Equivalence."""
    return Equivalence(
        source_system=row["source_system"],
        source_code=row["source_code"],
        target_system=row["target_system"],
        target_code=row["target_code"],
        match_type=row["match_type"],
        notes=row.get("notes"),
        source_title=row.get("source_title"),
        target_title=row.get("target_title"),
    )


async def get_equivalences(
    conn, system_id: str, code: str
) -> List[Equivalence]:
    """Get all equivalences for a node (outgoing edges)."""
    rows = await conn.fetch(
        """SELECT e.*,
                  s.title AS source_title,
                  t.title AS target_title
           FROM equivalence e
           LEFT JOIN classification_node s
             ON s.system_id = e.source_system AND s.code = e.source_code
           LEFT JOIN classification_node t
             ON t.system_id = e.target_system AND t.code = e.target_code
           WHERE e.source_system = $1 AND e.source_code = $2
           ORDER BY e.target_system, e.target_code""",
        system_id, code,
    )
    return [_row_to_equivalence(r) for r in rows]


async def translate_code(
    conn,
    source_system: str,
    source_code: str,
    target_system: str,
) -> List[Equivalence]:
    """Translate a code from one system to another."""
    rows = await conn.fetch(
        """SELECT e.*,
                  s.title AS source_title,
                  t.title AS target_title
           FROM equivalence e
           LEFT JOIN classification_node s
             ON s.system_id = e.source_system AND s.code = e.source_code
           LEFT JOIN classification_node t
             ON t.system_id = e.target_system AND t.code = e.target_code
           WHERE e.source_system = $1
             AND e.source_code = $2
             AND e.target_system = $3
           ORDER BY e.match_type, e.target_code""",
        source_system, source_code, target_system,
    )
    return [_row_to_equivalence(r) for r in rows]


async def get_crosswalk_graph(
    conn,
    source_system: str,
    target_system: str,
    limit: int = 500,
) -> Dict:
    """Get a graph of crosswalk edges between two systems.

    Returns deduplicated edges (only source->target direction) with
    node metadata for building a Cytoscape.js visualization.
    """
    # Get deduplicated edges (use LEAST/GREATEST to pick canonical direction)
    all_rows = await conn.fetch(
        """SELECT DISTINCT ON (
                LEAST(source_system, target_system),
                LEAST(source_code, target_code),
                GREATEST(source_code, target_code)
           )
           e.source_system, e.source_code, e.target_system, e.target_code,
           e.match_type,
           s.title AS source_title,
           t.title AS target_title
           FROM equivalence e
           LEFT JOIN classification_node s
             ON s.system_id = e.source_system AND s.code = e.source_code
           LEFT JOIN classification_node t
             ON t.system_id = e.target_system AND t.code = e.target_code
           WHERE (e.source_system = $1 AND e.target_system = $2)
              OR (e.source_system = $2 AND e.target_system = $1)
           ORDER BY
             LEAST(source_system, target_system),
             LEAST(source_code, target_code),
             GREATEST(source_code, target_code),
             e.id
        """,
        source_system, target_system,
    )

    total_edges = len(all_rows)
    truncated = total_edges > limit
    rows = all_rows[:limit]

    # Build unique nodes from edges
    node_map: Dict[str, Dict] = {}
    edges = []
    for r in rows:
        src_id = f"{r['source_system']}:{r['source_code']}"
        tgt_id = f"{r['target_system']}:{r['target_code']}"
        if src_id not in node_map:
            node_map[src_id] = {
                "id": src_id,
                "system": r["source_system"],
                "code": r["source_code"],
                "title": r["source_title"] or r["source_code"],
            }
        if tgt_id not in node_map:
            node_map[tgt_id] = {
                "id": tgt_id,
                "system": r["target_system"],
                "code": r["target_code"],
                "title": r["target_title"] or r["target_code"],
            }
        edges.append({
            "source": src_id,
            "target": tgt_id,
            "match_type": r["match_type"],
        })

    return {
        "source_system": source_system,
        "target_system": target_system,
        "nodes": list(node_map.values()),
        "edges": edges,
        "total_edges": total_edges,
        "truncated": truncated,
    }


async def get_crosswalk_stats(conn) -> List[Dict]:
    """Get counts of equivalence edges per system pair."""
    rows = await conn.fetch(
        """SELECT source_system, target_system,
                  COUNT(*) AS edge_count,
                  COUNT(CASE WHEN match_type = 'exact' THEN 1 END) AS exact_count,
                  COUNT(CASE WHEN match_type = 'partial' THEN 1 END) AS partial_count
           FROM equivalence
           GROUP BY source_system, target_system
           ORDER BY source_system, target_system"""
    )
    return [dict(r) for r in rows]
