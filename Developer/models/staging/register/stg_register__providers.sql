with providers as (
      select * from {{ source('register', 'users') }}
),
final as (
    select
        uidpk as provider_pk, 
        uid as provider_npi, 
        ufname as first_name, 
        ulname as last_name, 
        email, 
        gender, 
        age,  
        fte, 
        specialty
    from providers where  usertype = 1
)
select * from final
  