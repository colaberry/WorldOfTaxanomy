## Medical and Health Classification Systems Compared

WorldOfTaxonomy includes major health and clinical classification systems used globally for disease coding, procedure classification, drug identification, and clinical observations. This guide compares the key systems and explains how they connect.

## System Overview

| System | Codes | Purpose | Authority |
|--------|-------|---------|-----------|
| ICD-11 MMS | 37,052 | Disease classification (latest WHO standard) | WHO |
| ICD-10-CM | 97,606 | US clinical modification for diagnoses | CMS/NCHS |
| ICD-10-PCS | 79,987 | US procedure coding system | CMS |
| LOINC | 102,751 | Laboratory and clinical observations | Regenstrief Institute |
| MeSH | 31,124 | Medical literature subject headings | NLM |
| ATC WHO 2021 | 6,440 | Drug classification by therapeutic use | WHO |
| NCI Thesaurus | 211,072 | Cancer research terminology | National Cancer Institute |
| NDC | 112,077 | National drug codes (US) | FDA |
| SNOMED CT | ~20 (skeleton) | Clinical terminology reference | SNOMED International |
| CPT | ~18 (skeleton) | Medical procedure codes (US) | AMA |

## ICD-10-CM vs ICD-11: Which to Use?

### ICD-10-CM (United States)

ICD-10-CM is the US clinical modification of the WHO's ICD-10. It is required for US healthcare billing and reporting.

- **97,606 codes** in WorldOfTaxonomy
- **Structure**: 3-7 character alphanumeric codes (e.g., E11.65 - Type 2 diabetes with hyperglycemia)
- **Required by**: CMS, US health insurers, HIPAA transactions
- **Updated**: annually (October 1 each year)
- **Best for**: US clinical documentation, billing, public health reporting

### ICD-11 MMS (Global)

ICD-11 is the latest WHO revision, adopted by the World Health Assembly in 2019.

- **37,052 codes** in WorldOfTaxonomy
- **Structure**: Alphanumeric with extension codes for detail
- **Status**: Official WHO standard since January 2022
- **Best for**: International health statistics, WHO reporting, new implementations outside the US

### When to Use Which

| Scenario | System |
|----------|--------|
| US hospital billing | ICD-10-CM (required) |
| US procedure coding | ICD-10-PCS |
| WHO mortality/morbidity reporting | ICD-11 |
| New health IT system (non-US) | ICD-11 |
| International health research | ICD-11 |
| Legacy system integration | ICD-10-CM or ICD-10 base |

## LOINC - Laboratory and Clinical Observations

LOINC (Logical Observation Identifiers Names and Codes) is the universal standard for identifying health measurements, observations, and documents.

- **102,751 codes** in WorldOfTaxonomy
- **Use cases**: lab test orders and results, clinical documents, patient surveys
- **Structure**: 5-7 digit numeric codes with check digit
- **Required by**: US federal health agencies, HL7 FHIR implementations

LOINC does not classify diseases (that is ICD's role) - it classifies what was measured or observed.

## MeSH - Medical Subject Headings

MeSH is the controlled vocabulary used for indexing biomedical literature in PubMed/MEDLINE.

- **31,124 descriptors** in WorldOfTaxonomy
- **Use cases**: literature search, research categorization, knowledge organization
- **Structure**: hierarchical tree with 16 top-level categories
- **Maintained by**: US National Library of Medicine

## ATC - Drug Classification

The Anatomical Therapeutic Chemical (ATC) classification organizes drugs by the organ system they target and their therapeutic properties.

- **6,440 codes** in WorldOfTaxonomy
- **Structure**: 7-character hierarchical (e.g., A10BA02 - metformin)
- **Levels**: Anatomical group, Therapeutic subgroup, Pharmacological subgroup, Chemical subgroup, Chemical substance
- **Maintained by**: WHO Collaborating Centre for Drug Statistics

## How Health Systems Connect

```
ICD-10-CM (diagnoses) ---> MeSH (literature)
     |
     +--> ICD-11 MMS (newer WHO version)
     |
ATC (drugs) ---> ICD (indications)
     |
LOINC (lab tests) ---> ICD (related conditions)
     |
SNOMED CT (clinical terms) ---> ICD-10-CM (billing mapping)
```

## Domain-Specific Health Vocabularies

WorldOfTaxonomy also includes domain taxonomies for healthcare specialization:

| Domain | Codes | Coverage |
|--------|-------|----------|
| Hospital Department Types | 18 | Department classification |
| Nursing Specialty Types | 17 | Nursing specializations |
| Lab Test Category Types | 17 | Laboratory categories |
| Surgical Specialty Types | 17 | Surgical specializations |
| Pharmacy Practice Types | 16 | Pharmacy settings |
| Health Care Settings | 23 | Care delivery settings |
| Health Care Payer Types | 18 | Insurance/payer categories |

## API Examples

```bash
# Search for a medical term
curl "https://worldoftaxonomy.com/api/v1/search?q=diabetes&grouped=true"

# Browse ICD-10-CM structure
curl https://worldoftaxonomy.com/api/v1/systems/icd10_cm/nodes/E11/children

# Get LOINC code detail
curl https://worldoftaxonomy.com/api/v1/systems/loinc

# Browse ATC hierarchy
curl https://worldoftaxonomy.com/api/v1/systems/atc_who_2021/nodes/A10/children
```
