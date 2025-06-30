----------------------------------
--- Investigate if we can use Zuora to track coupon usage

-- Santi's query
select distinct
    mapping.crm_account_id,
    mapping.instance_account_id,
    listagg(distinct code_list.coupon) as promo_code,
    min(date(zuora.created_date)) as zuora_date_promo_code_applied
from functional.growth_analytics_staging.startup_promo_codes as code_list
    left join cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
        on code_list.coupon = zuora.description
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
where
    mapping.crm_account_id is not null
    and mapping.instance_account_id is not null
group by all



