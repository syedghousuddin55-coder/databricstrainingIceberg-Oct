-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW dev.silver.sample_trips_pipeline_sample AS
SELECT
    pickup_zip,
    fare_amount
FROM samples.nyctaxi.trips;
