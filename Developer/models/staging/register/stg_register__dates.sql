
with register__dates as (
      select * from {{ source('register', 'date') }}
),
final as (
    select
        DIMDATEPOSTPK as date_post_pk,
        DATE as date,
        YEAR as year ,
        MONTH as month,
        MONTHPERIOD as month_period,
        MONTHYEAR as month_year,
        DAY as day,
        DAYNAME as day_name
    from register__dates
)
select * from final
  