-- =============================================================================
--  dim_customers.sql  —  DIMENSION table (the "gold" customer table)
--  Reads from the SCD2 snapshot and presents it in business-friendly form:
--  renames the dbt_valid_* columns and adds an easy is_current flag.
--  reads: customers_snapshot   -->   produces: ANALYTICS.DIM_CUSTOMERS (table)
-- =============================================================================

-- Materialize as a TABLE (a stored copy). Dimensions are queried often by BI
-- tools, so storing them physically makes those queries fast.
{{ config(materialized='table') }}

WITH latest AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        dbt_valid_from   AS effective_from,   -- when this version became valid
        dbt_valid_to     AS effective_to,     -- when it stopped (NULL = still current)
        -- Turn the NULL/NOT-NULL end date into a simple TRUE/FALSE flag so BI
        -- tools can filter "current customers" with WHERE is_current = TRUE.
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('customers_snapshot') }}      -- the SCD2 history table
)

SELECT * FROM latest
