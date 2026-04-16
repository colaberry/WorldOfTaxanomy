-- Migration 001: Add data provenance columns to classification_system
-- Adds source_url, source_date, data_provenance (trust tier), and license
-- for auditable data lineage tracking.

ALTER TABLE classification_system
  ADD COLUMN IF NOT EXISTS source_url      TEXT;

ALTER TABLE classification_system
  ADD COLUMN IF NOT EXISTS source_date     DATE;

ALTER TABLE classification_system
  ADD COLUMN IF NOT EXISTS data_provenance TEXT
    CHECK (data_provenance IN (
        'official_download',
        'structural_derivation',
        'manual_transcription',
        'expert_curated'
    ));

ALTER TABLE classification_system
  ADD COLUMN IF NOT EXISTS license         TEXT;
