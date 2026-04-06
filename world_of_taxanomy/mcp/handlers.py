"""MCP tool handler functions.

Each handler takes a database connection and a dict of arguments,
calls the appropriate query layer function, and returns a
JSON-serializable result.
"""

from typing import Any, Dict, List

from world_of_taxanomy.exceptions import NodeNotFoundError, SystemNotFoundError
from world_of_taxanomy.query.browse import (
    get_systems, get_system, get_roots, get_node, get_children, get_ancestors,
)
from world_of_taxanomy.query.search import search_nodes
from world_of_taxanomy.query.equivalence import (
    get_equivalences as _get_equivalences,
    translate_code as _translate_code,
)


def _node_to_dict(node) -> Dict[str, Any]:
    """Convert a ClassificationNode to a JSON-serializable dict."""
    return {
        "system_id": node.system_id,
        "code": node.code,
        "title": node.title,
        "description": node.description,
        "level": node.level,
        "parent_code": node.parent_code,
        "sector_code": node.sector_code,
        "is_leaf": node.is_leaf,
    }


def _system_to_dict(system) -> Dict[str, Any]:
    """Convert a ClassificationSystem to a JSON-serializable dict."""
    return {
        "id": system.id,
        "name": system.name,
        "full_name": system.full_name,
        "region": system.region,
        "version": system.version,
        "authority": system.authority,
        "url": system.url,
        "node_count": system.node_count,
    }


def _equiv_to_dict(equiv) -> Dict[str, Any]:
    """Convert an Equivalence to a JSON-serializable dict."""
    return {
        "source_system": equiv.source_system,
        "source_code": equiv.source_code,
        "target_system": equiv.target_system,
        "target_code": equiv.target_code,
        "match_type": equiv.match_type,
        "source_title": equiv.source_title,
        "target_title": equiv.target_title,
    }


# ── Tool handlers ────────────────────────────────────────────


async def handle_list_classification_systems(
    conn, args: Dict[str, Any]
) -> List[Dict]:
    """List all classification systems."""
    systems = await get_systems(conn)
    return [_system_to_dict(s) for s in systems]


async def handle_get_industry(
    conn, args: Dict[str, Any]
) -> Dict[str, Any]:
    """Get details for a specific industry code."""
    system_id = args.get("system_id", "")
    code = args.get("code", "")
    try:
        node = await get_node(conn, system_id, code)
        return _node_to_dict(node)
    except NodeNotFoundError:
        return {"error": f"Node '{code}' not found in '{system_id}'"}


async def handle_browse_children(
    conn, args: Dict[str, Any]
) -> List[Dict]:
    """Get direct children of an industry code."""
    system_id = args.get("system_id", "")
    parent_code = args.get("parent_code", "")
    children = await get_children(conn, system_id, parent_code)
    return [_node_to_dict(c) for c in children]


async def handle_get_ancestors(
    conn, args: Dict[str, Any]
) -> List[Dict]:
    """Get the path from root to a specific code."""
    system_id = args.get("system_id", "")
    code = args.get("code", "")
    ancestors_list = await get_ancestors(conn, system_id, code)
    return [_node_to_dict(a) for a in ancestors_list]


async def handle_search_classifications(
    conn, args: Dict[str, Any]
) -> List[Dict]:
    """Search across classification systems."""
    query = args.get("query", "")
    system_id = args.get("system_id")
    limit = args.get("limit", 20)
    results = await search_nodes(conn, query, system_id=system_id, limit=limit)
    return [_node_to_dict(r) for r in results]


async def handle_get_equivalences(
    conn, args: Dict[str, Any]
) -> List[Dict]:
    """Get cross-system equivalences for a code."""
    system_id = args.get("system_id", "")
    code = args.get("code", "")
    equivs = await _get_equivalences(conn, system_id, code)
    return [_equiv_to_dict(e) for e in equivs]


async def handle_translate_code(
    conn, args: Dict[str, Any]
) -> List[Dict]:
    """Translate a code from one system to another."""
    source_system = args.get("source_system", "")
    source_code = args.get("source_code", "")
    target_system = args.get("target_system", "")
    results = await _translate_code(conn, source_system, source_code, target_system)
    return [_equiv_to_dict(r) for r in results]


async def handle_get_sector_overview(
    conn, args: Dict[str, Any]
) -> List[Dict]:
    """Get top-level sectors/sections for a system."""
    system_id = args.get("system_id", "")
    roots = await get_roots(conn, system_id)
    return [_node_to_dict(r) for r in roots]
