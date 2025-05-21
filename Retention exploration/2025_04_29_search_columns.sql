--- Query to search column names in Schema

SELECT table_schema, table_name, column_name
FROM cleansed.information_schema.columns
WHERE lower(table_schema) = 'zuora'
  AND lower(column_name) LIKE '%discount_type%';



SELECT table_schema, table_name, column_name
FROM cleansed.information_schema.columns
WHERE lower(table_schema) = 'zuora'
  AND lower(column_name) LIKE '%product_rate_plan_charge_tiers%';



select count(*)
from cleansed.zuora.ZUORA_PAYMENT_METHODS_BCV 


with joined as (
    select accounts.ACCOUNT_NUMBER, payment_methods.type
    from cleansed.zuora.ZUORA_ACCOUNTS_BCV accounts
    left join cleansed.zuora.ZUORA_PAYMENT_METHODS_BCV payment_methods
        on accounts.default_payment_method_id = payment_methods.id
)

select count(*) tot_obs,
sum(case when type is null then 1 else 0 end) null_payment_methods,
from joined
limit 10

