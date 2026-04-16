## Crosswalk Map - How Classification Systems Connect

WorldOfTaxonomy contains 321,937+ crosswalk edges linking classification systems. These edges let you translate a code in one system to equivalent codes in another. This guide explains the crosswalk topology, match types, and how to navigate translation paths.

## What Is a Crosswalk?

A crosswalk (or concordance) is a mapping between codes in two different classification systems. For example, NAICS 6211 ("Offices of Physicians") maps to ISIC 8620 ("Medical and dental practice activities"). Crosswalks can be:

- **Exact**: one-to-one mapping with the same scope
- **Partial**: the source code's scope partially overlaps with the target
- **Broader**: the target code has a wider scope than the source
- **Narrower**: the target code has a narrower scope than the source

## Core Crosswalk Topology

The knowledge graph has several major crosswalk clusters:

### Industry Classification Hub

ISIC Rev 4 serves as the central hub for industry classification:

```
NAICS 2022 <--> ISIC Rev 4 <--> NACE Rev 2 (and all EU national variants)
                    |
                    +--> NIC 2008 (India)
                    +--> ANZSIC 2006 (Australia/NZ)
                    +--> GB/T 4754-2017 (China)
                    +--> 80+ national ISIC adaptations
```

Key edge counts:
- ISIC Rev 4 / NAICS 2022: ~3,418 bidirectional edges
- NACE Rev 2 national variants: 1:1 mappings (identical structure)

### Product and Trade Hub

CPC v2.1 bridges product classification and trade codes:

```
HS 2022 <--> CPC v2.1 <--> ISIC Rev 4
  |            |
  +--> HTS     +--> UNSPSC v24
  +--> CN 2024
  +--> SITC Rev 4
```

Key edge counts:
- HS 2022 / CPC v2.1: 11,686 edges
- CPC v2.1 / ISIC Rev 4: 5,430 edges

### Occupation and Education Hub

SOC 2018 and ISCO-08 are the twin hubs for occupation data:

```
CIP 2020 --> SOC 2018 <--> ISCO-08 <--> ISIC Rev 4
  |            |              |
  +--> ISCED-F +--> NAICS     +--> ESCO Occupations
                              |
                              +--> O*NET-SOC
```

Key edge counts:
- CIP 2020 / SOC 2018: 5,903 edges
- SOC 2018 / ISCO-08: 992 edges
- ESCO Occupations / ISCO-08: 6,048 edges
- O*NET-SOC / SOC 2018: 1,734 edges
- CIP 2020 / ISCED-F 2013: 1,615 edges
- ISCED 2011 / ISCO-08: 25 edges
- ISCO-08 / ISIC Rev 4: 44 edges

### Geographic Hub

```
ISO 3166-1 <--> ISO 3166-2 (subdivisions)
     |
     +--> UN M.49 (regions)
     +--> Nation-Sector synergy crosswalk (98 edges)
```

### Domain Crosswalks

Each domain taxonomy links back to its parent NAICS sector:

```
NAICS 484 (Truck Transportation) --> domain_truck_freight, domain_truck_vehicle, ...
NAICS 11  (Agriculture)          --> domain_ag_crop, domain_ag_livestock, ...
NAICS 21  (Mining)               --> domain_mining_mineral, domain_mining_method, ...
NAICS 22  (Utilities)            --> domain_util_energy, domain_util_grid, ...
NAICS 23  (Construction)         --> domain_const_trade, domain_const_building, ...
```

### Regulatory Crosswalks

```
CFR Title 49 <--> NAICS 2022 (437 edges)
FMCSA <--> domain_truck (regulatory compliance)
```

## Translation Paths

Not all systems have direct crosswalks. You can translate between systems by following a path through intermediate hubs.

### Example: Translate a German WZ code to a US SOC occupation

1. WZ 2008 --> NACE Rev 2 (1:1 national variant)
2. NACE Rev 2 --> ISIC Rev 4 (1:1 structure)
3. ISIC Rev 4 --> ISCO-08 (44 edges)
4. ISCO-08 --> SOC 2018 (992 edges)

### Example: Find which HS trade codes relate to a NAICS industry

1. NAICS 2022 --> ISIC Rev 4 (~3,418 edges)
2. ISIC Rev 4 --> CPC v2.1 (5,430 edges)
3. CPC v2.1 --> HS 2022 (11,686 edges)

## API for Crosswalk Navigation

### Direct Equivalences

```bash
# Get all systems that NAICS 6211 maps to
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022/nodes/6211/equivalences

# Translate to all connected systems at once
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022/nodes/6211/translations
```

### Crosswalk Statistics

```bash
# Overall crosswalk stats
curl https://worldoftaxonomy.com/api/v1/equivalences/stats

# Stats for a specific system
curl "https://worldoftaxonomy.com/api/v1/equivalences/stats?system_id=naics_2022"
```

### Compare Systems

```bash
# Side-by-side top-level comparison
curl "https://worldoftaxonomy.com/api/v1/compare?a=naics_2022&b=isic_rev4"

# Codes in system A with no mapping to B
curl "https://worldoftaxonomy.com/api/v1/diff?a=naics_2022&b=isic_rev4"
```

## Match Type Reference

| Type | Meaning | Example |
|------|---------|---------|
| `exact` | Identical scope and definition | NAICS 111110 "Soybean Farming" = ISIC 0111 |
| `partial` | Overlapping but not identical scope | NAICS 6211 partially overlaps ISIC 8620 |
| `broader` | Target has wider scope | A 6-digit NAICS to a 2-digit ISIC |
| `narrower` | Target has narrower scope | A section-level ISIC to a detailed NAICS |
| `related` | Conceptually related but structurally different | Domain taxonomy to parent NAICS sector |

## MCP Tools for Crosswalks

| Tool | Purpose |
|------|---------|
| `get_equivalences` | Direct crosswalk mappings for a code |
| `translate_code` | Translate a code to a specific target system |
| `translate_across_all_systems` | Translate to all connected systems |
| `get_crosswalk_coverage` | Coverage statistics for a crosswalk pair |
| `get_system_diff` | Codes with no mapping between two systems |
| `compare_sector` | Side-by-side sector comparison |
| `describe_match_types` | Explain the match type categories |
