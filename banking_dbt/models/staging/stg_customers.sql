-- =============================================================================
--  stg_customers.sql  —  STAGING model (a VIEW)
--  Reads the raw customer events (stored as ONE json "variant" column called v)
--  and turns them into clean, typed columns. It also DE-DUPLICATES: CDC can
--  deliver many versions of the same customer, so we keep only the newest per id.
--  reads: source raw.customers   -->   produces: ANALYTICS.STG_CUSTOMERS (view)
-- =============================================================================

-- Materialize as a VIEW (a saved query, always fresh, no stored copy) — staging
-- is a light transform, so a cheap view is the right choice.
{{ config(materialized='view') }}

-- CTE (temporary named result) that extracts JSON fields and numbers each
-- customer's versions newest-first.
with ranked as (
    select
        -- v:field = read a key from the variant JSON; ::type = cast it.
        v:id::string            as customer_id,
        v:first_name::string    as first_name,
        v:last_name::string     as last_name,
        v:email::string         as email,
        v:created_at::timestamp as created_at,
        current_timestamp       as load_timestamp,    -- when THIS model ran
        -- number rows within each id, newest first; rn = 1 is the latest version.
        row_number() over (
            partition by v:id::string                 -- group rows of the same id
            order by v:created_at desc                 -- newest first
        ) as rn
    from {{ source('raw', 'customers') }}             -- = BANKING.RAW.CUSTOMERS
)

-- Keep only rn = 1 -> exactly ONE clean row per customer (dedup complete).
select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp
from ranked
where rn = 1
