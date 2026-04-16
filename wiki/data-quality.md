## Data Quality and Provenance

WorldOfTaxonomy uses a four-tier provenance framework to track the origin and reliability of every classification system. This guide explains the provenance tiers, how data is verified, and how to report issues.

## Four-Tier Provenance Framework

Every classification system in the knowledge graph is tagged with one of four provenance tiers:

| Tier | Label | Description | Verification |
|------|-------|-------------|-------------|
| 1 | `official_download` | Data downloaded directly from the authoritative source | File hash stored for audit |
| 2 | `structural_derivation` | Derived from an official system (e.g., NACE national variants) | 1:1 structural mapping verified |
| 3 | `manual_transcription` | Transcribed from official publications (PDF, HTML, print) | Cross-checked against source |
| 4 | `expert_curated` | Curated by domain experts based on industry knowledge | Peer-reviewed structure |

### Tier 1: Official Download

The gold standard. Data files (CSV, Excel, XML) are downloaded directly from the standards body's website. A SHA-256 hash of the source file is stored in the `source_file_hash` column for reproducibility.

**Examples**: NAICS 2022 (Census Bureau CSV), HS 2022 (WCO), ISIC Rev 4 (UN CSV), LOINC (Regenstrief download)

### Tier 2: Structural Derivation

Systems that reuse the structure of an official system with localized naming. For example, all EU NACE Rev 2 national variants (WZ 2008, NAF Rev 2, ATECO 2007, etc.) share the identical code structure. These are derived from the official NACE publication with 1:1 mapping.

**Examples**: WZ 2008 (Germany), ONACE 2008 (Austria), NOGA 2008 (Switzerland), all EU NACE national variants

### Tier 3: Manual Transcription

Data transcribed from official documents that do not provide machine-readable downloads. The original source URL and date are recorded for audit.

**Examples**: SIC 1987 (transcribed from OSHA HTML), some Asian and African national classifications

### Tier 4: Expert Curated

Domain-specific vocabularies created by subject matter experts. These fill gaps where no official standard exists (e.g., truck freight types, healthcare delivery models, cybersecurity threat categories).

**Examples**: All domain_* taxonomies (truck, agriculture, mining, construction, manufacturing, etc.)

## Provenance Metadata Fields

Each classification system carries these audit fields:

| Field | Description |
|-------|-------------|
| `data_provenance` | Provenance tier (official_download, structural_derivation, manual_transcription, expert_curated) |
| `source_url` | URL of the authoritative data source |
| `source_date` | Date the source data was accessed/published |
| `license` | License terms for the data |
| `source_file_hash` | SHA-256 hash of the original file (Tier 1 only) |

## Querying Provenance via API

### Get Provenance for a System

```bash
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022
```

Response includes `data_provenance`, `source_url`, `source_date`, `license`, and `source_file_hash`.

### Audit Report

```bash
# Full provenance audit across all systems
curl https://worldoftaxonomy.com/api/v1/audit
```

The audit report shows:
- Breakdown by provenance tier (system count, node count per tier)
- Tier 1 systems missing a file hash
- Tier 2 structural derivation count and node coverage
- Skeleton systems (placeholder entries awaiting full data)

### MCP Audit Tool

```bash
# Via MCP
tools/call get_audit_report {}
```

Returns the same audit data in a format suitable for AI agent consumption.

## Data Verification Practices

### Hash Verification

For Tier 1 (official_download) systems, the `source_file_hash` lets you verify data integrity:

1. Download the original file from `source_url`
2. Compute its SHA-256 hash
3. Compare against the stored `source_file_hash`
4. If they match, the data in WorldOfTaxonomy matches the original file

### Structural Verification

For Tier 2 (structural_derivation) systems, you can verify:

1. The code structure matches the parent system exactly
2. Crosswalk edges are 1:1 (every code in the derived system maps to exactly one code in the parent)

### Cross-Reference Verification

For any system, you can cross-reference node counts and structure against the authoritative publication.

## Reporting Data Quality Issues

If you find incorrect data, missing codes, or wrong crosswalk mappings:

1. **API**: Use the contact endpoint to report issues
2. **GitHub**: File an issue on the project repository
3. **Include**: System ID, code in question, expected vs actual value, and a link to the authoritative source

## Data Disclaimer

All classification data in WorldOfTaxonomy is provided for informational purposes only. It should not be used as a substitute for official government or standards body publications. For regulatory, legal, or compliance purposes, always verify codes against the authoritative source.

The `report_issue_url` field in API responses provides a direct link for reporting data quality concerns.

## Skeleton Systems

Some systems are included as structural placeholders where the full dataset is not freely available (e.g., SNOMED CT, CPT). These are marked with low node counts and are included to preserve the crosswalk topology. Full data requires a license from the respective standards body.
