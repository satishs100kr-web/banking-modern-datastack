-- =============================================================================
--  fact_transactions.sql  —  FACT table (the centre of the star schema)
--  Holds the measurable events (transactions). Enriches each transaction with
--  its customer_id by joining to accounts, so you can analyse spend per customer.
--  reads: stg_transactions + stg_accounts  -->  produces: ANALYTICS.FACT_TRANSACTIONS
-- =============================================================================

-- INCREMENTAL: on the first run it builds the whole table; on later runs it
-- only INSERTS NEW rows (matched by unique_key). Essential for big fact tables —
-- you don't rebuild millions of rows every time, just append the new ones.
{{ config(materialized='incremental', unique_key='transaction_id') }}

SELECT
    t.transaction_id,        -- the unique key (used by 'incremental' to find new rows)
    t.account_id,            -- which account
    a.customer_id,           -- which customer (brought in by the join below)
    t.amount,                -- how much money
    t.related_account_id,    -- receiver, for transfers
    t.status,
    t.transaction_type,      -- DEPOSIT / WITHDRAWAL / TRANSFER
    t.transaction_time,      -- when it happened
    CURRENT_TIMESTAMP AS load_timestamp
FROM {{ ref('stg_transactions') }} t
-- LEFT JOIN to accounts so every transaction gets its owner's customer_id.
-- LEFT (not INNER) keeps the transaction even if the account isn't found.
LEFT JOIN {{ ref('stg_accounts') }} a
    ON t.account_id = a.account_id
