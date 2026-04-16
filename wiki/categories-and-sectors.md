## Categories and Sectors - How Systems Are Organized

WorldOfTaxonomy organizes its 1,000+ classification systems into 16 categories. Each category groups systems that share a common domain. This guide explains the category structure and how to navigate it.

## The 16 Categories

| Category | Systems | Description |
|----------|---------|-------------|
| Industry | ~150+ | Economic activity classification (NAICS, ISIC, NACE, SIC, national variants) |
| Product/Trade | ~20+ | Goods and services classification (HS, CPC, UNSPSC, SITC) |
| Occupation | ~15+ | Job and skills classification (SOC, ISCO, ESCO, O*NET) |
| Education | ~10+ | Educational programs and levels (ISCED, CIP) |
| Health/Clinical | ~30+ | Disease, procedure, drug, and lab codes (ICD, LOINC, MeSH, ATC) |
| Geographic | ~10+ | Country, region, and subdivision codes (ISO 3166, NUTS, FIPS) |
| Financial/Environmental | ~20+ | Sustainability, accounting, and governance (SASB, EU Taxonomy, GHG, COFOG) |
| Regulatory | ~100+ | Laws, standards, and compliance frameworks (HIPAA, GDPR, OSHA, FDA, SEC) |
| ISO Standards | ~25+ | Management system standards (ISO 9001, 14001, 27001, 45001) |
| International Agreements | ~25+ | Treaties and global frameworks (Basel, FATF, Paris Agreement, ILO) |
| Academic/Research | ~15+ | Subject classification for scholarly work (arXiv, MSC, JEL, ACM CCS) |
| Patent | 1 | Patent classification (CPC - 254K codes) |
| Domain: Technology | ~50+ | Software, AI, cybersecurity, cloud, data taxonomies |
| Domain: Healthcare | ~40+ | Clinical specialties, pharmacy, devices, therapy types |
| Domain: Finance | ~30+ | Insurance, banking, investment, payment taxonomies |
| Domain: Sector-Specific | ~200+ | Transportation, agriculture, mining, construction, energy, and other sector vocabularies |

## How Categories Map to API Queries

### Browse by Category

```bash
# Get all systems (includes category metadata)
curl https://worldoftaxonomy.com/api/v1/systems

# Group by region
curl "https://worldoftaxonomy.com/api/v1/systems?group_by=region"

# Filter by country to find relevant systems
curl "https://worldoftaxonomy.com/api/v1/systems?country=US"
```

### Search Within a Category

The search endpoint searches across all systems. To find results in a specific domain, use keywords:

```bash
# Find health-related codes
curl "https://worldoftaxonomy.com/api/v1/search?q=diabetes&grouped=true"

# Find trade codes
curl "https://worldoftaxonomy.com/api/v1/search?q=cotton&grouped=true"
```

## Domain-Specific Vocabularies

Domain taxonomies extend the standard classification systems with specialized vocabularies. They are organized by NAICS 2-digit sector:

### Agriculture (NAICS 11)
Crop types, livestock categories, farming methods, commodity grades, equipment and machinery, input supply, business structure, market channels, regulatory framework, land classification, post-harvest value chain.

### Mining (NAICS 21)
Mineral types, extraction methods, reserve classification, equipment, project lifecycle, safety and regulatory compliance.

### Utilities (NAICS 22)
Energy sources, grid regions, tariff structures, infrastructure assets, regulatory ownership.

### Construction (NAICS 23)
Trade types, building types, project delivery methods, material systems, sustainability and green building.

### Manufacturing (NAICS 31-33)
Process types, quality and compliance, operations models, industry verticals, supply chain integration, facility configuration.

### Retail (NAICS 44-45)
Channel types, merchandise categories, fulfillment and delivery, pricing strategies, store formats.

### Finance (NAICS 52)
Instrument types, market structure, regulatory frameworks, client segments, insurance, banking, investment.

### Healthcare (NAICS 62)
Care settings, specialty service lines, payer types, delivery models, health IT systems.

### Technology and Information (NAICS 51)
Media types, revenue models, platform distribution, content formats.

### Emerging Sectors
Chemical industry, defence, water and environment, AI and data, biotechnology, space and satellite, climate technology, advanced materials, quantum computing, digital assets, autonomous systems, energy storage, semiconductors, synthetic biology, extended reality.

## Category Counts in the Knowledge Graph

The knowledge graph distributes across categories roughly as:

- **Industry classification**: ~150 systems, ~50K nodes (many national NACE/ISIC variants)
- **Health/Clinical**: ~30 systems, ~700K+ nodes (ICD-10-CM, LOINC, NCI Thesaurus drive the count)
- **Patent**: 1 system, ~254K nodes (Patent CPC)
- **Product/Trade**: ~20 systems, ~100K nodes (UNSPSC dominates with 77K)
- **Occupation/Skills**: ~15 systems, ~40K nodes (ESCO Skills at 14K, ESCO Occupations at 3K)
- **Domain vocabularies**: ~300+ systems, ~10K nodes (typically 15-30 codes each)
- **Regulatory/Compliance**: ~100+ systems, ~5K nodes
- **Everything else**: ~300 systems, ~15K nodes
