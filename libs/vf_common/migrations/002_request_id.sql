-- Correlation ID of the ingestion request (X-Request-ID), carried through the pipeline.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS request_id text;
