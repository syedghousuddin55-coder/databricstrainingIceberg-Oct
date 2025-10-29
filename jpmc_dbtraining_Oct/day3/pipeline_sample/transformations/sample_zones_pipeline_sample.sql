-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW dev.gold.sample_zones_pipeline_sample AS
SELECT
    pickup_zip,
    SUM(fare_amount) AS total_fare
FROM dev.silver.sample_trips_pipeline_sample
GROUP BY pickup_zip;
