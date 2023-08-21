
with cpt_codes as (
      select * from {{ source('chart', 'code') }}
),
final as (
    select
        CodePK as cpt_code_pk,	
        Code as cpt_code,	
        Description as cpt_desc,	
        Groups as cpt_grouping
    from cpt_codes where CodeType = 'cc'
)
select * from final
  