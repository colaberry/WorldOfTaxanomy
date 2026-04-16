"""Ingest HL7 FHIR Resource Type Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("fhir_resources", "FHIR Resources", "HL7 FHIR Resource Type Categories", "R4", "Global", "HL7 International")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FH", "FHIR Resource Categories", 1, None),
    ("FH.01", "Foundation: Bundle, OperationOutcome", 2, 'FH'),
    ("FH.02", "Base: Patient, Practitioner, Organization", 2, 'FH'),
    ("FH.03", "Clinical: Condition, Procedure, Observation", 2, 'FH'),
    ("FH.04", "Diagnostics: DiagnosticReport, Specimen", 2, 'FH'),
    ("FH.05", "Medications: MedicationRequest, Immunization", 2, 'FH'),
    ("FH.06", "Workflow: Task, Appointment, Schedule", 2, 'FH'),
    ("FH.07", "Financial: Claim, Coverage, ExplanationOfBenefit", 2, 'FH'),
    ("FH.08", "Conformance: CapabilityStatement, StructureDefinition", 2, 'FH'),
    ("FH.09", "Terminology: CodeSystem, ValueSet, ConceptMap", 2, 'FH'),
    ("FH.10", "Security: AuditEvent, Provenance, Consent", 2, 'FH'),
    ("FH.11", "Documents: Composition, DocumentReference", 2, 'FH'),
    ("FH.12", "Exchange: MessageHeader, Subscription", 2, 'FH'),
    ("FH.13", "Public Health: MeasureReport, Library", 2, 'FH'),
    ("FH.14", "Research: ResearchStudy, ResearchSubject", 2, 'FH'),
]

async def ingest_fhir_resources(conn) -> int:
    sid, short, full, ver, region, authority = _SYSTEM_ROW
    await conn.execute(
        """INSERT INTO classification_system (id, name, full_name, version, region, authority,
                  source_url, source_date, data_provenance, license)
           VALUES ($1,$2,$3,$4,$5,$6,$7,CURRENT_DATE,$8,$9)
           ON CONFLICT (id) DO UPDATE SET name=$2,full_name=$3,version=$4,region=$5,
                  authority=$6,source_url=$7,source_date=CURRENT_DATE,
                  data_provenance=$8,license=$9""",
        sid, short, full, ver, region, authority,
        _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute("DELETE FROM classification_node WHERE system_id = $1", sid)
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1,$2,$3,$4,$5)""",
            sid, code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, sid)
    return count
