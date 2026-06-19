{% snapshot accounts_snapshot %}
{#
  accounts_snapshot — SCD2 history for accounts (same idea as customers_snapshot).
  Keeps a history row whenever an account's customer/type/balance changes
  (e.g. the balance going up or down over time), instead of overwriting.
  Built by:  dbt snapshot   ->   ANALYTICS.ACCOUNTS_SNAPSHOT

  config below:
    target_schema='ANALYTICS' -> snapshot table lives in ANALYTICS
    unique_key='account_id'   -> the business key for an account
    strategy='check'          -> detect change by comparing the check_cols
    check_cols=[...]          -> if customer_id/account_type/balance change, close
                                 the old row and add a new one. dbt auto-adds
                                 dbt_valid_from / dbt_valid_to / dbt_updated_at.
#}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='account_id',
      strategy='check',
      check_cols=['customer_id', 'account_type', 'balance']
    )
}}
SELECT * FROM {{ ref('stg_accounts') }}
{% endsnapshot %}
