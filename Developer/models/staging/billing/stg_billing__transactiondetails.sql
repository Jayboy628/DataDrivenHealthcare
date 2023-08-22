
with billing__transactiondetails as (
      select * from {{ source('billing', 'transactiondetails') }}
),
final as (
    select
        TRANSIDPK as trans_pk,
        PATIENTID as patient_id,
        PHYSICIANID as physician_id,
        DATEPOSTID as date_post_id,
        DATESERVICEID as date_service_id,
        CPTCODEID as cpt_code_id,
        PAYERID as payer_id,
        TRANSACTIONID as transaction_id,
        LOCATIONID as location_id,
        PATIENTNUMBER as patient_number,
        DIAGNOSISCODEID as diagnosis_code_id,
        CPTUNITS as cpt_units,
        GROSSCHARGE as gross_charge,
        PAYMENT as payment,
        ADJUSTMENT as adjustment,
        AR as account_receivable
    from billing__transactiondetails
)
select * from final
  