## Occupation Classification Systems Compared

WorldOfTaxonomy includes the major occupation and skills classification systems used for labor market analysis, job matching, education-to-career pathways, and workforce planning. This guide compares SOC, ISCO, ESCO, O*NET, and national systems.

## System Overview

| System | Codes | Region | Purpose | Authority |
|--------|-------|--------|---------|-----------|
| ISCO-08 | 619 | Global (ILO) | International occupation standard | International Labour Organization |
| SOC 2018 | 1,447 | United States | US federal occupation classification | Bureau of Labor Statistics |
| O*NET-SOC | 867 | United States | Detailed occupation database with skills | Department of Labor |
| ESCO Occupations | 3,045 | Europe (EU) | European occupation taxonomy | European Commission |
| ESCO Skills | 14,247 | Europe (EU) | Skills and competences taxonomy | European Commission |
| ANZSCO 2022 | 1,590 | Australia/NZ | AU/NZ occupation standard | ABS/Stats NZ |
| NOC 2021 | 51 | Canada | Canadian occupation classification | Statistics Canada |
| UK SOC 2020 | 43 | United Kingdom | UK occupation standard | ONS |
| KldB 2010 | 54 | Germany | German occupation classification | Federal Employment Agency |
| ROME v4 | 93 | France | French job/occupation repertoire | Pole emploi |

## SOC vs ISCO: The Two Major Frameworks

### SOC 2018 (Standard Occupational Classification)

- **1,447 detailed occupations** across 6 levels
- **Structure**: 2-digit major groups (23) down to 6-digit detailed occupations
- **Used for**: US government statistics, labor market data, visa classifications (H-1B), wage surveys
- **Updated**: approximately every 10 years

### ISCO-08 (International Standard Classification of Occupations)

- **619 occupations** across 4 levels
- **Structure**: 1-digit major groups (10) down to 4-digit unit groups
- **Used for**: International labor statistics, ILO reporting, basis for national systems
- **Key difference**: Broader categories than SOC; designed for international comparison

### Crosswalk Between SOC and ISCO

SOC 2018 and ISCO-08 are connected by 992 crosswalk edges. The mapping is many-to-many because SOC is more granular than ISCO.

```bash
# Translate SOC to ISCO
curl https://worldoftaxonomy.com/api/v1/systems/soc_2018/nodes/29-1211/equivalences
```

## ESCO - European Skills and Occupations

ESCO is the EU's multilingual classification connecting occupations to skills:

- **3,045 occupations** mapped to ISCO-08 (6,048 crosswalk edges)
- **14,247 skills and competences** linked to occupations
- **Key advantage**: Skills-based matching across EU labor markets
- **Use cases**: Job portals, skills gap analysis, career guidance, Europass

## O*NET - Occupation Information Network

O*NET extends SOC with rich attribute data:

- **867 occupations** mapped to SOC 2018 (1,734 crosswalk edges)
- **Includes**: Knowledge areas, abilities, work activities, work context, interests (RIASEC), work values, work styles
- **Key advantage**: Most detailed occupation attribute data available
- **Use cases**: Career exploration, job analysis, workforce development

## Education-to-Occupation Pathways

The crosswalk topology connects education to occupations:

```
CIP 2020 (instructional programs)
  |-- 5,903 edges --> SOC 2018 (US occupations)
  |-- 1,615 edges --> ISCED-F 2013 (fields of education)
  |
ISCED 2011 (education levels)
  |-- 25 edges --> ISCO-08 (global occupations)
```

This lets you answer questions like "What occupations do CIP 51.0912 (Physician Assistant) graduates work in?"

```bash
curl https://worldoftaxonomy.com/api/v1/systems/cip_2020/nodes/51.0912/equivalences
```

## Occupation-to-Industry Mapping

Occupations connect to industries:

```
SOC 2018 --> NAICS 2022 (55 edges)
ISCO-08 --> ISIC Rev 4 (44 edges)
```

## Which System to Use

| Purpose | Recommended System | Why |
|---------|-------------------|-----|
| US labor statistics | SOC 2018 | Required by BLS/Census |
| International comparison | ISCO-08 | ILO standard |
| European job matching | ESCO | EU multilingual, skills-linked |
| Career exploration | O*NET-SOC | Rich attribute data |
| Australian/NZ workforce | ANZSCO 2022 | National standard |
| Canadian workforce | NOC 2021 | National standard |
| Skills gap analysis | ESCO Skills | 14K skills taxonomy |
| Education-to-career mapping | CIP 2020 + SOC | 5,903 crosswalk edges |

## MCP Tools for Occupation Data

| Tool | Purpose |
|------|---------|
| `search_classifications` | Find occupations by job title |
| `get_equivalences` | Cross-system occupation mapping |
| `translate_code` | Translate between SOC, ISCO, ESCO |
| `browse_children` | Navigate occupation hierarchy |
| `get_country_taxonomy_profile` | What occupation systems apply to a country |
