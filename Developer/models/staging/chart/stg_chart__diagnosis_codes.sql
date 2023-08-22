
with diagnosis_codes as (
      select * from {{ source('chart', 'code') }}
),
final as (
    select
        CodePK as diagnosis_code_pk,	
        Code as diagnosis_code,	
        Description as diagnosis_code_description,	
        Groups as diagnosis_code_group
    from diagnosis_codes where CodeType = 'dc'
)
select * from final