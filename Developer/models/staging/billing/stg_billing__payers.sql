
with billing__payers as (
      select * from {{ source('billing', 'payer') }}
),
final as (
    select
        DIMPAYERPK as payer_id,
	    PAYERNAME as payer_name
    from billing__payers
)
select * from final
  