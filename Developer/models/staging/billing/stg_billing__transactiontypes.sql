
with billing__transactiontypes as (
      select * from {{ source('billing', 'transactiontype') }}
),
final as (
    select
        DIMTRANSACTIONPK as transaction_pk,
        TRANSACTIONTYPE as transaction_type,
        TRANSACTION as transaction,
        ADJUSTMENTREASON as adjustment_reason
    from billing__transactiontypes
)
select * from final
  