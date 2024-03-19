

WITH adjustment AS (
    SELECT * FROM {{ ref('stg_bill__adjustment') }}
),
grosscharge AS (
    SELECT * FROM {{ ref('stg_bill__grosscharge') }}
),
payment AS (
    SELECT * FROM {{ ref('stg_bill__payment') }}
),
consolidated_transactions AS (
    SELECT adjustment.ADJUSTMENT_PK AS TRANSACTION_PK, adjustment.AR_FK AS TRANSACTION_ARFK FROM adjustment
    UNION ALL
    SELECT grosscharge.CHARGE_PK AS TRANSACTION_PK, grosscharge.AR_FK AS TRANSACTION_ARFK FROM grosscharge
    UNION ALL
    SELECT payment.PAYMENT_PK AS TRANSACTION_PK, payment.AR_FK AS TRANSACTION_ARFK FROM payment
),

transaction_detail AS (
    SELECT
        ct.TRANSACTION_PK, 
        ct.TRANSACTION_ARFK,
        g.cpt_units,
        {{ cent_to_dollar('g.amount') AS gross_charge,
        {{ cent_to_dollar('p.amount') }} AS payment,
        {{ cent_to_dollar('a.amount') }} AS adjustment
    FROM consolidated_transactions ct
    LEFT JOIN adjustment a ON ct.TRANSACTION_ARFK = a.AR_FK
    LEFT JOIN payment p ON ct.TRANSACTION_ARFK = p.AR_FK
    LEFT JOIN grosscharge g ON ct.TRANSACTION_ARFK = g.AR_FK
)
SELECT * FROM transaction_detail


