-- =============================================================================
--  stg_accounts.sql  —  STAGING model (a VIEW)
--  Same idea as stg_customers: extract typed columns from the raw account events
--  (the json "variant" column v) and keep only the newest version per account id.
--  reads: source raw.accounts   -->   produces: ANALYTICS.STG_ACCOUNTS (view)
-- =============================================================================

{{ config(materialized='view') }}   -- a view: light, always fresh, no stored copy

with ranked as (
    select
        v:id::string            as account_id,      -- the account's id
        v:customer_id::string   as customer_id,     -- which customer owns it (links to customers)
        v:account_type::string  as account_type,    -- SAVINGS / CHECKING
        v:balance::float        as balance,          -- money in the account
        v:currency::string      as currency,         -- USD
        v:created_at::timestamp as created_at,
        current_timestamp       as load_timestamp,
        -- newest-first numbering per account id (rn = 1 = latest version)
        row_number() over (
            partition by v:id::string
            order by v:created_at desc
        ) as rn
    from {{ source('raw', 'accounts') }}            -- = BANKING.RAW.ACCOUNTS
)

select                                              -- keep one clean row per account
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
from ranked
where rn = 1
