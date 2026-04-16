"""NAICS 484xxx -> Truck Domain Taxonomies crosswalk ingester.

Links specific NAICS 484xxx nodes to the four truck domain taxonomies:
  naics_2022 -> domain_truck_freight  (industry -> freight type)
  naics_2022 -> domain_truck_vehicle  (industry -> vehicle class)
  naics_2022 -> domain_truck_cargo    (industry -> cargo type)
  naics_2022 -> domain_truck_ops      (industry -> operations model)

All edges use match_type='broad' (industry node encompasses these domain concepts).
Derived from NAICS 484 sector structure. Open.
"""
from __future__ import annotations

# (naics_code, naics_system, domain_taxonomy_id, domain_system_id)
# Each tuple: this NAICS industry classification encompasses this domain taxonomy
NAICS_DOMAIN_LINKS: list[tuple[str, str, str, str]] = [
    # 4841 - General Freight Trucking -> all four domain taxonomies
    ("4841",   "naics_2022", "domain_truck_freight", "domain_truck_freight"),
    ("4841",   "naics_2022", "domain_truck_vehicle",  "domain_truck_vehicle"),
    ("4841",   "naics_2022", "domain_truck_cargo",    "domain_truck_cargo"),
    ("4841",   "naics_2022", "domain_truck_ops",      "domain_truck_ops"),

    # 48411 - General Freight Trucking, Local -> freight + vehicle + ops
    ("48411",  "naics_2022", "domain_truck_freight", "domain_truck_freight"),
    ("48411",  "naics_2022", "domain_truck_vehicle",  "domain_truck_vehicle"),
    ("48411",  "naics_2022", "domain_truck_ops",      "domain_truck_ops"),

    # 484110 - General Freight Trucking, Local -> freight + ops
    ("484110", "naics_2022", "domain_truck_freight", "domain_truck_freight"),
    ("484110", "naics_2022", "domain_truck_ops",      "domain_truck_ops"),

    # 48412 - General Freight Trucking, Long-Distance -> all four
    ("48412",  "naics_2022", "domain_truck_freight", "domain_truck_freight"),
    ("48412",  "naics_2022", "domain_truck_vehicle",  "domain_truck_vehicle"),
    ("48412",  "naics_2022", "domain_truck_cargo",    "domain_truck_cargo"),
    ("48412",  "naics_2022", "domain_truck_ops",      "domain_truck_ops"),

    # 484121 - General Freight Trucking, Long-Distance, TL -> freight + vehicle
    ("484121", "naics_2022", "domain_truck_freight", "domain_truck_freight"),
    ("484121", "naics_2022", "domain_truck_vehicle",  "domain_truck_vehicle"),

    # 484122 - General Freight Trucking, Long-Distance, LTL -> freight
    ("484122", "naics_2022", "domain_truck_freight", "domain_truck_freight"),

    # 4842 - Specialized Freight Trucking -> cargo + vehicle + freight
    ("4842",   "naics_2022", "domain_truck_cargo",    "domain_truck_cargo"),
    ("4842",   "naics_2022", "domain_truck_vehicle",  "domain_truck_vehicle"),
    ("4842",   "naics_2022", "domain_truck_freight",  "domain_truck_freight"),
    ("4842",   "naics_2022", "domain_truck_ops",      "domain_truck_ops"),

    # 48421 - Used Household and Office Goods Moving -> cargo + ops
    ("48421",  "naics_2022", "domain_truck_cargo",    "domain_truck_cargo"),
    ("48421",  "naics_2022", "domain_truck_ops",      "domain_truck_ops"),

    # 484210 - Used Household and Office Goods Moving -> cargo
    ("484210", "naics_2022", "domain_truck_cargo",    "domain_truck_cargo"),

    # 48422 - Specialized Freight (except Used Goods) Trucking, Local -> cargo + vehicle
    ("48422",  "naics_2022", "domain_truck_cargo",    "domain_truck_cargo"),
    ("48422",  "naics_2022", "domain_truck_vehicle",  "domain_truck_vehicle"),

    # 484220 - Specialized Freight (except Used Goods) Trucking, Local -> cargo
    ("484220", "naics_2022", "domain_truck_cargo",    "domain_truck_cargo"),

    # 48423 - Specialized Freight (except Used Goods) Trucking, Long-Distance -> all four
    ("48423",  "naics_2022", "domain_truck_cargo",    "domain_truck_cargo"),
    ("48423",  "naics_2022", "domain_truck_vehicle",  "domain_truck_vehicle"),
    ("48423",  "naics_2022", "domain_truck_freight",  "domain_truck_freight"),
    ("48423",  "naics_2022", "domain_truck_ops",      "domain_truck_ops"),

    # 484230 - Specialized Freight (except Used Goods) Trucking, Long-Distance -> cargo + vehicle
    ("484230", "naics_2022", "domain_truck_cargo",    "domain_truck_cargo"),
    ("484230", "naics_2022", "domain_truck_vehicle",  "domain_truck_vehicle"),

    # 484 sector root -> all four
    ("484",    "naics_2022", "domain_truck_freight",  "domain_truck_freight"),
    ("484",    "naics_2022", "domain_truck_vehicle",   "domain_truck_vehicle"),
    ("484",    "naics_2022", "domain_truck_cargo",     "domain_truck_cargo"),
    ("484",    "naics_2022", "domain_truck_ops",       "domain_truck_ops"),
]


async def ingest_crosswalk_naics484_domains(conn) -> int:
    """Ingest NAICS 484 -> Truck Domain crosswalk edges.

    Inserts into the equivalence table linking NAICS 484xxx nodes to
    all four truck domain taxonomy roots (freight, vehicle, cargo, ops).
    All edges use match_type='broad'.

    Returns total edge count inserted.
    """
    # Collect valid NAICS codes
    naics_codes = {
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node WHERE system_id = 'naics_2022'"
        )
    }

    # Collect valid domain taxonomy IDs (used as "target codes" in equivalence)
    # We link to the domain system root by using the system ID itself as pseudo-code
    # Actually we link NAICS code -> domain taxonomy name as a broad linkage
    # The equivalence table target_code should be a valid domain node or taxonomy ID
    # We'll use the top-level category codes from each domain taxonomy
    domain_roots = {
        "domain_truck_freight": ["dtf_mode", "dtf_equip", "dtf_svc", "dtf_cargo"],
        "domain_truck_vehicle": ["dtv_dot", "dtv_body"],
        "domain_truck_cargo":   ["dtc_com", "dtc_haz", "dtc_hdl", "dtc_reg"],
        "domain_truck_ops":     ["dto_type", "dto_fleet", "dto_biz", "dto_route"],
    }

    domain_codes: dict[str, set] = {}
    for sys_id in domain_roots:
        domain_codes[sys_id] = {
            row["code"]
            for row in await conn.fetch(
                "SELECT code FROM classification_node WHERE system_id = $1 AND level = 1",
                sys_id,
            )
        }

    rows = []
    for naics_code, naics_sys, domain_taxonomy, domain_sys in NAICS_DOMAIN_LINKS:
        if naics_code not in naics_codes:
            continue
        # Link to each top-level category in the domain taxonomy
        for root_code in domain_codes.get(domain_sys, []):
            rows.append((naics_sys, naics_code, domain_sys, root_code, "broad"))

    # Deduplicate
    rows = list(set(rows))

    await conn.executemany(
        """INSERT INTO equivalence
               (source_system, source_code, target_system, target_code, match_type)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
        rows,
    )

    return len(rows)
