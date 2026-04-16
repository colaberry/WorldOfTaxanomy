## Trade and Product Classification Guide

WorldOfTaxonomy includes the major product and trade classification systems used for customs, procurement, and statistical reporting. This guide explains how HS, CPC, UNSPSC, SITC, and BEC relate, and which system to use for different purposes.

## System Comparison

| System | Codes | Purpose | Maintained By |
|--------|-------|---------|---------------|
| HS 2022 | 6,960 | International customs tariffs | World Customs Organization |
| CPC v2.1 | 4,596 | Statistical product classification | United Nations |
| UNSPSC v24 | 77,337 | Procurement and spend analysis | GS1 US |
| SITC Rev 4 | 77 | Trade statistics (aggregated) | United Nations |
| BEC Rev 5 | 29 | Broad economic categories | United Nations |
| HTS (US) | 120 | US-specific tariff schedule | US International Trade Commission |
| CN 2024 | 118 | EU Combined Nomenclature | European Commission |

## How These Systems Relate

### The HS Family Tree

The Harmonized System (HS) is the foundation. Other systems build on it:

```
HS 2022 (WCO)
  |
  +--> HTS (US) - adds US-specific subheadings
  +--> CN 2024 (EU) - adds EU-specific subheadings
  +--> ASEAN Tariff (AHTN) - regional extension
  +--> MERCOSUR Tariff (NCM) - regional extension
  +--> AfCFTA Tariff - African regional extension
```

### The Statistical Bridge

CPC v2.1 bridges product classification and industry classification:

```
HS 2022 <-- 11,686 edges --> CPC v2.1 <-- 5,430 edges --> ISIC Rev 4
```

This means you can trace: a trade code (HS) to what product category it belongs to (CPC) to which industry produces it (ISIC/NAICS).

### Aggregation for Statistics

SITC and BEC aggregate trade data at higher levels:

```
HS 2022 (6,960 detailed codes)
  |
  +--> SITC Rev 4 (77 codes) - for trade flow analysis
  +--> BEC Rev 5 (29 codes) - for economic category analysis
```

## Which System to Use

| Purpose | Recommended System | Why |
|---------|-------------------|-----|
| Customs declarations | HS 2022 (or national variant) | Legally required for trade |
| US import/export filings | HTS (US) | Required by US Customs |
| EU trade compliance | CN 2024 | Required by EU customs |
| Procurement/spend analysis | UNSPSC v24 | Most granular (77K codes) |
| International trade statistics | SITC Rev 4 | Designed for aggregate analysis |
| Economic modeling | BEC Rev 5 | Maps to SNA categories |
| Product-to-industry mapping | CPC v2.1 | Bridges HS to ISIC |

## HS Code Structure

HS codes use a hierarchical 6-digit structure:

```
01        Chapter (2 digits) - Live animals
0101      Heading (4 digits) - Horses, asses, mules
010121    Subheading (6 digits) - Pure-bred horses
```

National extensions add further digits:
- HTS (US): up to 10 digits
- CN (EU): 8 digits

## CPC Code Structure

CPC v2.1 uses a 5-level hierarchy:

```
0     Section - Agriculture, forestry and fishery products
01    Division - Products of agriculture, horticulture
011   Group - Cereals
0111  Class - Wheat
01110 Subclass - Wheat, unmilled
```

## UNSPSC Structure

UNSPSC uses an 8-digit hierarchy across 4 levels:

```
10        Segment - Live Plant and Animal Material
1010      Family - Live animals
101015    Class - Dogs
10101501  Commodity - Guard dogs
```

With 77,337 codes, UNSPSC is the most detailed product classification available.

## Crosswalk Navigation

### Translate an HS code to an industry

```bash
# Get CPC equivalences for an HS code
curl https://worldoftaxonomy.com/api/v1/systems/hs_2022/nodes/0101/equivalences

# Then translate CPC to ISIC
curl https://worldoftaxonomy.com/api/v1/systems/cpc_v21/nodes/0111/equivalences
```

### Find trade codes for an industry

```bash
# Start with NAICS, translate to ISIC, then to CPC, then to HS
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022/nodes/1111/translations
```

## MCP Tools for Trade Classification

| Tool | Purpose |
|------|---------|
| `search_classifications` | Find trade codes by product name |
| `get_equivalences` | Get crosswalk to other systems |
| `translate_code` | Direct translation between systems |
| `browse_children` | Explore HS/CPC/UNSPSC hierarchy |
| `get_crosswalk_coverage` | Check crosswalk completeness |
