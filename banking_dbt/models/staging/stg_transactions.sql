-- =============================================================================
--  stg_transactions.sql  —  STAGING model (a VIEW)
--  Extract typed columns from the raw transaction events (json "variant" v).
--  NOTE: NO dedup here (no row_number). Transactions are IMMUTABLE facts —
--  they're inserted once and never updated — so there are no "newer versions"
--  to collapse. (Customers/accounts CAN change, which is why those dedup.)
--  reads: source raw.transactions  -->  produces: ANALYTICS.STG_TRANSACTIONS (view)
-- =============================================================================

{{ config(materialized='view') }}

SELECT
    v:id::string                 AS transaction_id,       -- unique transaction id
    v:account_id::string         AS account_id,            -- the account it belongs to
    v:amount::float              AS amount,                -- how much money
    v:txn_type::string           AS transaction_type,      -- DEPOSIT / WITHDRAWAL / TRANSFER
    v:related_account_id::string AS related_account_id,    -- receiver (only for transfers)
    v:status::string             AS status,                -- COMPLETED
    v:created_at::timestamp      AS transaction_time,      -- when it happened
    CURRENT_TIMESTAMP            AS load_timestamp         -- when this model ran
FROM {{ source('raw', 'transactions') }}                  -- = BANKING.RAW.TRANSACTIONS
