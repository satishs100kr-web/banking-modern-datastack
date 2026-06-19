-- =============================================================================
--  dim_accounts.sql  —  DIMENSION table (the "gold" account table)
--  Reads the account SCD2 snapshot and presents business-friendly columns with
--  an is_current flag — same pattern as dim_customers.
--  reads: accounts_snapshot   -->   produces: ANALYTICS.DIM_ACCOUNTS (table)
-- =============================================================================

{{ config(materialized='table') }}   -- stored table -> fast for BI queries

WITH source_data AS (
    SELECT
        account_id,
        customer_id,
        account_type,
        balance,
        currency,
        created_at,
        dbt_valid_from   AS effective_from,   -- version valid-from date
        dbt_valid_to     AS effective_to,     -- version valid-to (NULL = current)
        -- simple active flag derived from the SCD2 end date
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('accounts_snapshot') }}       -- the SCD2 history table
)

SELECT * FROM source_data
