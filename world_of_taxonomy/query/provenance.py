"""Provenance enrichment and audit queries.

Provides helpers to attach system-level provenance metadata to node
responses, and aggregate audit queries for data trustworthiness review.
"""

from typing import Any, Dict, List, Optional


async def get_system_provenance_map(
    conn, system_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    """Fetch provenance fields for a set of system IDs.

    Returns a dict mapping system_id -> {data_provenance, license,
    source_url, source_date, source_file_hash}.
    """
    if not system_ids:
        return {}
    rows = await conn.fetch(
        """SELECT id, data_provenance, license, source_url,
                  source_date, source_file_hash
           FROM classification_system
           WHERE id = ANY($1)""",
        system_ids,
    )
    result = {}
    for r in rows:
        source_date = r["source_date"]
        if source_date is not None:
            source_date = str(source_date)
        result[r["id"]] = {
            "data_provenance": r["data_provenance"],
            "license": r["license"],
            "source_url": r["source_url"],
            "source_date": source_date,
            "source_file_hash": r["source_file_hash"],
        }
    return result


def enrich_node_dict(node_dict: Dict[str, Any], prov: Dict[str, Any]) -> Dict[str, Any]:
    """Attach provenance fields from a system provenance entry to a node dict."""
    node_dict["data_provenance"] = prov.get("data_provenance")
    node_dict["license"] = prov.get("license")
    node_dict["source_url"] = prov.get("source_url")
    node_dict["source_date"] = prov.get("source_date")
    node_dict["source_file_hash"] = prov.get("source_file_hash")
    return node_dict


async def get_audit_report(conn) -> Dict[str, Any]:
    """Generate an aggregate audit report for data trustworthiness review.

    Returns provenance tier breakdown, systems missing file hashes,
    structural derivation accounting, and skeleton system detection.
    """
    # Total systems and nodes
    totals = await conn.fetchrow(
        "SELECT count(*) AS systems, coalesce(sum(node_count), 0) AS nodes "
        "FROM classification_system"
    )

    # Provenance tier breakdown
    tier_rows = await conn.fetch(
        """SELECT data_provenance,
                  count(*) AS system_count,
                  coalesce(sum(node_count), 0) AS node_count
           FROM classification_system
           GROUP BY data_provenance
           ORDER BY node_count DESC"""
    )
    tiers = [
        {
            "data_provenance": r["data_provenance"],
            "system_count": r["system_count"],
            "node_count": r["node_count"],
        }
        for r in tier_rows
    ]

    # Official download systems missing file hash
    missing_hash_rows = await conn.fetch(
        """SELECT * FROM classification_system
           WHERE data_provenance = 'official_download'
             AND source_file_hash IS NULL
           ORDER BY name"""
    )

    # Structural derivation stats
    deriv = await conn.fetchrow(
        """SELECT count(*) AS system_count,
                  coalesce(sum(node_count), 0) AS node_count
           FROM classification_system
           WHERE data_provenance = 'structural_derivation'"""
    )

    # Skeleton systems (node_count < 30)
    skeleton_rows = await conn.fetch(
        """SELECT * FROM classification_system
           WHERE node_count < 30
           ORDER BY node_count, name"""
    )

    return {
        "total_systems": totals["systems"],
        "total_nodes": totals["nodes"],
        "provenance_tiers": tiers,
        "official_missing_hash": missing_hash_rows,
        "structural_derivation_count": deriv["system_count"],
        "structural_derivation_nodes": deriv["node_count"],
        "skeleton_systems": skeleton_rows,
    }
