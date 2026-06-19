{% snapshot customers_snapshot %}
{#
  customers_snapshot — SCD2 history (the project's signature feature).
  Keeps history: when a customer's data changes, the old row is "closed" (given an
  end date) and a new row is added, instead of overwriting. Lets you answer
  "what was true at any point in time" (vital for banking/audit).
  Built by:  dbt snapshot   (NOT dbt run)   ->   ANALYTICS.CUSTOMERS_SNAPSHOT

  config below:
    target_schema='ANALYTICS' -> the snapshot table lives in ANALYTICS
    unique_key='customer_id'  -> the business key identifying a customer
    strategy='check'          -> detect change by COMPARING the check_cols
    check_cols=[...]          -> if first_name/last_name/email change for a
                                 customer_id, close the old row (dbt_valid_to=now)
                                 and insert a new one. dbt also auto-adds the
                                 dbt_valid_from / dbt_valid_to / dbt_updated_at columns.
#}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='customer_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'email']
    )
}}
SELECT * FROM {{ ref('stg_customers') }}
{% endsnapshot %}
