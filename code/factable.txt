WITH patient AS (
    SELECT * FROM {{ ref('dim_patient') }}
),
provider AS (
    SELECT * FROM {{ ref('dim_provider') }}
),
date AS (
    SELECT * FROM {{ ref('dim_date') }}
),
cptcode AS (
    SELECT * FROM {{ ref('dim_cptcode') }}
),
diagnosiscode AS (
    SELECT * FROM {{ ref('dim_diagnosiscode') }}
),
payer AS (
    SELECT * FROM {{ ref('dim_payer') }}
),
location AS (
    SELECT * FROM {{ ref('dim_location') }}
),
transaction AS (
    SELECT * FROM {{ ref('dim_transaction') }}
),
transaction_detail AS (
    SELECT * FROM {{ ref('int_transaction_detail') }}
),
ar AS (
    SELECT 
        AR_PK,
        PATIENT_FK,
        PROVIDER_FK,
        DATE_POST_FK,
        DATE_SERVICE_FK,
        CPT_CODE_FK,
        PAYER_FK,
        LOCATION_FK,
        PATIENT_NUMBER,
        DIAGNOSIS_CODE_FK,
        TRANSACTION_FK,
        Amount AS AR
    FROM {{ ref('stg_bill__ar') }}
),
fact AS (
    SELECT
        AR_PK AS FACT_PK,
        pa.PATIENT_PK,
        prp.PROVIDER_PK,
        dt.DATE_POST_PK,
        ct.CPT_CODE_PK,
        py.PAYER_PK,
        ln.LOCATION_PK,
        ar.PATIENT_NUMBER,
        dc.DIAGNOSIS_CODE_PK,
        td.TRANSACTION_PK,
        td.CPT_UNITS,
        td.GROSS_CHARGE,
        td.PAYMENT,
        td.ADJUSTMENT,
        ar.AR
    FROM ar
        LEFT JOIN patient pa ON ar.PATIENT_FK = pa.PATIENT_PK
        LEFT JOIN provider prp ON ar.PROVIDER_FK = prp.PROVIDER_PK
        LEFT JOIN date dt ON ar.DATE_POST_FK = dt.DATE_POST_PK 
        LEFT JOIN cptcode ct ON ar.CPT_CODE_FK = ct.CPT_CODE_PK
        LEFT JOIN payer py ON ar.PAYER_FK = py.PAYER_PK
        LEFT JOIN location ln ON ar.LOCATION_FK = ln.LOCATION_PK
        LEFT JOIN diagnosiscode dc ON ar.DIAGNOSIS_CODE_FK = dc.DIAGNOSIS_CODE_PK
        LEFT JOIN transaction_detail td ON TRANSACTION_ARFK = AR_PK 
        LEFT JOIN transaction t ON ar.TRANSACTION_FK = t.TRANSACTION_PK )

SELECT * FROM fact
