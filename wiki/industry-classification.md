## Industry Classification Guide - Which System to Use

Choosing the right industry classification system depends on your geographic scope, regulatory requirements, and level of detail needed. This guide provides a decision tree to help you select the appropriate system.

## Decision Tree

### Step 1: What Is Your Geographic Scope?

**Single country** - Use the national system for that country (see table below).

**Multi-country or global** - Use ISIC Rev 4 as your common denominator, then translate to national systems as needed.

**North America (US, Canada, Mexico)** - Use NAICS 2022.

**European Union** - Use NACE Rev 2 (or your country's national variant).

### Step 2: What Level of Detail Do You Need?

| Granularity | Typical Use | Recommended System |
|-------------|-------------|-------------------|
| Broad sectors (10-20 categories) | Executive dashboards, market sizing | ISIC sections (A-U) or NAICS 2-digit |
| Divisions (~100 categories) | Industry reports, portfolio analysis | ISIC 2-digit or NAICS 3-digit |
| Groups (~300 categories) | Detailed market analysis | ISIC 3-digit or NAICS 4-digit |
| Classes (~500+ categories) | Regulatory filings, detailed reporting | ISIC 4-digit or NAICS 5-6 digit |

### Step 3: Is This for Regulatory Compliance?

If you are filing with a government agency, use the system they require:

| Agency/Purpose | Required System |
|----------------|----------------|
| US Census Bureau / BLS | NAICS 2022 |
| US SEC filings | SIC 1987 |
| Eurostat / EU statistical reporting | NACE Rev 2 |
| UN statistical reporting | ISIC Rev 4 |
| Australian Bureau of Statistics | ANZSIC 2006 |
| Indian Ministry of Statistics | NIC 2008 |
| World Bank projects | ISIC Rev 4 |

## Country-to-System Quick Reference

### Major Economies

| Country | Primary System | Codes | Notes |
|---------|---------------|-------|-------|
| United States | NAICS 2022 | 2,125 | Also SIC 1987 for SEC filings |
| Canada | NAICS 2022 | 2,125 | Shared with US and Mexico |
| United Kingdom | SIC 1987 / UK SOC | 1,176 | Companies House uses SIC |
| Germany | WZ 2008 | 996 | National NACE variant |
| France | NAF Rev 2 | 996 | National NACE variant |
| India | NIC 2008 | 2,070 | Based on ISIC Rev 4 |
| China | GB/T 4754-2017 | 118 | National standard |
| Japan | JSIC 2013 | 20 | Statistical survey use |
| Australia | ANZSIC 2006 | 825 | Shared with New Zealand |
| South Korea | KSIC 2017 | 108 | KOSTAT standard |

### Latin America

All countries use CIIU Rev 4 (the Spanish translation of ISIC Rev 4) with 766 codes: Colombia, Argentina, Chile, Peru, Ecuador, Bolivia, Venezuela, Costa Rica, Guatemala, Panama, Paraguay, Uruguay, Dominican Republic.

### European Union (27 members + EEA)

All EU member states use NACE Rev 2 with national naming: ATECO (Italy), NAF (France), WZ (Germany), CNAE (Spain), PKD (Poland), SBI (Netherlands), SNI (Sweden), and others.

## Comparing the Major Systems

### NAICS 2022 vs ISIC Rev 4

| Feature | NAICS 2022 | ISIC Rev 4 |
|---------|-----------|-----------|
| Codes | 2,125 | 766 |
| Levels | 6 (2-6 digit) | 4 (section, division, group, class) |
| Region | North America | Global |
| Detail | Very granular | Moderate |
| Crosswalk | 3,418 edges to ISIC | 3,418 edges to NAICS |
| Best for | US regulatory, detailed analysis | International comparison |

### NAICS 2022 vs NACE Rev 2

| Feature | NAICS 2022 | NACE Rev 2 |
|---------|-----------|-----------|
| Codes | 2,125 | 996 |
| Levels | 6 | 4 |
| Region | North America | European Union |
| Detail | Very granular | Moderate |
| Best for | US/Canada/Mexico | EU regulatory, Eurostat |

### NAICS 2022 vs SIC 1987

| Feature | NAICS 2022 | SIC 1987 |
|---------|-----------|---------|
| Codes | 2,125 | 1,176 |
| Status | Current | Legacy (but still used) |
| Region | North America | USA/UK |
| Best for | Current analysis | SEC filings, historical data |

## How to Translate Between Systems

Use the WorldOfTaxonomy API to translate codes:

```bash
# Translate NAICS 6211 to ISIC
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022/nodes/6211/equivalences

# Translate to ALL connected systems
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022/nodes/6211/translations
```

For systems without direct crosswalks, follow the translation path through hub systems (see the Crosswalk Map guide).

## Domain-Specific Extensions

When a standard industry code is too broad for your use case, WorldOfTaxonomy provides domain-specific vocabularies. For example:

- NAICS 484 "Truck Transportation" links to: truck freight types (44 codes), vehicle classes (23), cargo classification (46), carrier operations (27)
- NAICS 11 "Agriculture" links to: crop types (46), livestock categories (27), farming methods (28), commodity grades (30)

These domain taxonomies are crosswalked back to their parent NAICS/ISIC sector codes.
