
with register__locations as (
      select * from {{ source('register', 'location') }}
),
final as (
    select
        dimlocationpk as location_pk,
        locationname as location_name
    from register__locations
)
select * from final
  