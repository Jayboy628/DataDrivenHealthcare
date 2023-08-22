with patients as (
      select * from {{ source('register', 'users') }}
),
final as (
    select
        uidpk as patient_pk, 
        uid as patient_number, 
        ufname as first_name, 
        ulname as last_name, 
        email, 
        gender, 
        age, 
        city,
        state 
    from patients where usertype = 3
)
select * from final
  