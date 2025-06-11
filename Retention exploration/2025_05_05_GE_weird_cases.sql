--- Query to search weird cases



-- Accounts not appearing in C/C table
select *
from functional.growth_engine.dim_growth_engine_churn_predictions
where instance_account_id = 9239100

select *
from functional.growth_engine.dim_growth_engine_churn_predictions
where instance_account_id = 10887011


-- Account with low predicted probability
select *
from functional.growth_engine.dim_growth_engine_churn_predictions
where instance_account_id = 13760446
order by source_snapshot_date




-- https://monitor.zende.sk/accounts/20625768/billing
-- Account used other coupon ZENPAUSE-20625768

select *
from sandbox.juan_salgado.ge_dashboard_test
where account_id = 20625768

-- Net_arr_usd does not considerate temporary discounts
-- This customer redeemed SAVE50-10214376 coupon but BCV ARR does not reflect it
select
        finance.*
    from foundational.finance.fact_recurring_revenue_bcv_enriched as finance
        left join
            foundational.customer.entity_mapping_daily_snapshot_bcv
                as snapshot_bcv
            on finance.billing_account_id = snapshot_bcv.billing_account_id
where snapshot_bcv.instance_account_id = 10214376








----- Understanding Zuora table
----- 0. Why coupon id includes account id
----- 1. Differenciate queued coupon to reedemeed coupon
    --- Join subscription status and join
----- 2. Know amount of coupon
----- 3. Know if coupon had any problem
    -- Join Zuora invoice zuora_invoice_items_bcv
        -- On rate plan charge id

--- https://monitor.zende.sk/accounts/10214376/billing

select *
from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
where 
    mapping.instance_account_id in (10214376)
    and lower(zuora.description) like '%save50%'



select 
zuora.created_date, zuora.account_id, zuora.description,
zuora.EFFECTIVE_START_DATE, zuora.EFFECTIVE_END_DATE,
zuora.UP_TO_PERIODS, UP_TO_PERIODS_TYPE, zuora.is_last_segment,
zuora.rate
from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
    left join cleansed.zuora.zuora_subscriptions_bcv as subscription
        on zuora.subscription_id = subscription.id
where 
    mapping.instance_account_id in (10214376)
    and lower(zuora.description) like '%save50%'
    and subscription.status = 'Active'





---- Searching redeemed customer that contracted
---- Contracted because net_arr is including the discount, but the customer uses the same # of seats and products
select
    finance.service_date,
    finance.crm_account_id,
    snapshot.instance_account_id as zendesk_account_id,
    finance.billing_account_id,
    finance.list_price_arr_usd,
    finance.gross_arr_usd,
    finance.net_arr_usd,
    finance.temp_discount_arr_usd,
    finance.recurring_discount_arr_usd,
    finance.list_price_discount_arr_usd,
    finance.nonrecurring_discount_arr_usd
from
    foundational.finance.fact_recurring_revenue_daily_snapshot_enriched
        as finance
    left join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on
            finance.billing_account_id = snapshot.billing_account_id
            and finance.service_date = snapshot.source_snapshot_date
where
    finance.service_date >= '2025-04-15'
    and snapshot.instance_account_id = 20970182
order by 1




select service_date, net_arr_usd, contraction_arr_usd
from FUNCTIONAL.FINANCE.SFA_QTD_CRM_PRODUCT_FINANCE_ADJ_CURRENT
where crm_account_id = '001PC00000CcEZUYA3'
and service_date >= '2025-04-15'
order by service_date






---- Searching not redeemed customer that renewed subscription
select
    finance.service_date,
    finance.crm_account_id,
    snapshot.instance_account_id as zendesk_account_id,
    finance.billing_account_id,
    finance.list_price_arr_usd,
    finance.gross_arr_usd,
    finance.net_arr_usd,
    finance.temp_discount_arr_usd,
    finance.recurring_discount_arr_usd,
    finance.list_price_discount_arr_usd,
    finance.nonrecurring_discount_arr_usd
from
    foundational.finance.fact_recurring_revenue_daily_snapshot_enriched
        as finance
    left join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on
            finance.billing_account_id = snapshot.billing_account_id
            and finance.service_date = snapshot.source_snapshot_date
where
    finance.service_date >= '2025-04-15'
    and snapshot.instance_account_id = 23423406
order by 1





------------------------------------------
--- Follow-up items ML team

-- 0. Understand table to use

select *
from functional.growth_engine.dim_growth_engine_churn_predictions
where instance_account_id = 10623844
order by source_snapshot_date



select *
from dev_functional.growth_engine.dim_growth_engine_churn_predictions
where instance_account_id = 10623844
order by source_snapshot_date


select *
from functional.eda_ml_outcome.churn_score_predictions
where instance_account_id = 20814736
and source_snapshot_date >= '2025-04-15'
order by source_snapshot_date




select *
from functional.eda_ml_outcome.churn_score_predictions
where instance_account_id = 11852324
and source_snapshot_date >= '2025-04-15'
order by source_snapshot_date



select *
from functional.eda_ml_outcome.churn_score_predictions
where instance_account_id = 13330277
and source_snapshot_date >= '2025-04-15'
order by source_snapshot_date


select instance_account_id, source_snapshot_date, count(*)
from functional.eda_ml_outcome.churn_score_predictions
group by 1, 2
order by 3 desc
limit 10

--- 1. Lower probabilities

select count(*), count(distinct account_id)
from sandbox.juan_salgado.ge_growth_metrics_test
where predicted_probability < 0.5 or predicted_probability is null

select distinct account_id
from sandbox.juan_salgado.ge_growth_metrics_test
where predicted_probability < 0.5 or predicted_probability is null

select *
from sandbox.juan_salgado.ge_growth_metrics_test
where predicted_probability < 0.5 
and data_type = 'not_redeemed'


--Full list: 10887011, 13760446, 20625768, 2317209, 9239100, 24905253, 12084713, 9859964, 17166022, 10623844, 20667467, 2223774, 1868710, 12949991, 13330277, 10949684, 9809334, 23913266, 10998949, 20820854, 20848154, 9898642, 13610089, 24853211, 10751417, 20748938

--- Loaded banner May 6, probability 32% May 10
select *
from functional.growth_engine.dim_growth_engine_churn_predictions
where instance_account_id = 23913266
order by source_snapshot_date


--- Loaded banner April 22, probability Null
select *functional.growth_engine.dim_growth_engine_churn_predictions
from 
where instance_account_id = 20625768
order by source_snapshot_date


--- Not storing historical data
select instance_account_id, count(*) tot_obs
from FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
group by 1
order by 2 desc
limit 10



--- 2. Customer applied other coupon

-- coupon SAVE30ANNUAL / applied ZENPAUSE
-- https://monitor.zende.sk/accounts/20625768/billing
select *
from sandbox.juan_salgado.ge_dashboard_test
where account_id = 20625768


--- 3. Contraction showing/not showing

select *
from sandbox.juan_salgado.ge_dashboard_test
where zuora_unique_coupon_redeemed is not null

select *
from sandbox.juan_salgado.ge_growth_metrics_test
where contraction_arr > 0
and unique_count_work_modal_2_click is not null

-- Customer paid subscription before renewal date
select *
from sandbox.juan_salgado.ge_dashboard_test
where account_id = 17371620


---- Net ARR not changing
select
    finance.service_date,
    finance.crm_account_id,
    snapshot.instance_account_id as zendesk_account_id,
    finance.billing_account_id,
    finance.list_price_arr_usd,
    finance.gross_arr_usd,
    finance.net_arr_usd,
    finance.temp_discount_arr_usd,
    finance.recurring_discount_arr_usd,
    finance.list_price_discount_arr_usd,
    finance.nonrecurring_discount_arr_usd
from
    foundational.finance.fact_recurring_revenue_daily_snapshot_enriched
        as finance
    left join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on
            finance.billing_account_id = snapshot.billing_account_id
            and finance.service_date = snapshot.source_snapshot_date
where
    finance.service_date >= '2025-04-15'
    and snapshot.instance_account_id = 20814736
order by 1


---- Net ARR contracting
select
    finance.service_date,
    finance.crm_account_id,
    snapshot.instance_account_id as zendesk_account_id,
    finance.billing_account_id,
    finance.list_price_arr_usd,
    finance.gross_arr_usd,
    finance.net_arr_usd,
    finance.temp_discount_arr_usd,
    finance.recurring_discount_arr_usd,
    finance.list_price_discount_arr_usd,
    finance.nonrecurring_discount_arr_usd
from
    foundational.finance.fact_recurring_revenue_daily_snapshot_enriched
        as finance
    left join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on
            finance.billing_account_id = snapshot.billing_account_id
            and finance.service_date = snapshot.source_snapshot_date
where
    finance.service_date >= '2025-04-15'
    and snapshot.instance_account_id = 12949991
order by 1



--- 4. Customers not redeeming & not churning

select *
from sandbox.juan_salgado.ge_growth_metrics_test
where renewal_passed = 1
and data_type = 'not_redeemed'


-- List of accounts: 9809334, 24143511, 20748938, 2223774, 23913266, 23269123, 21044824, 13330277, 17699600, 20625768, 9239100, 10887011, 24244805, 12084713, 23423406

---- Net ARR contracting
select
    finance.service_date,
    finance.crm_account_id,
    snapshot.instance_account_id as zendesk_account_id,
    finance.billing_account_id,
    finance.list_price_arr_usd,
    finance.gross_arr_usd,
    finance.net_arr_usd,
    finance.temp_discount_arr_usd,
    finance.recurring_discount_arr_usd,
    finance.list_price_discount_arr_usd,
    finance.nonrecurring_discount_arr_usd
from
    foundational.finance.fact_recurring_revenue_daily_snapshot_enriched
        as finance
    left join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on
            finance.billing_account_id = snapshot.billing_account_id
            and finance.service_date = snapshot.source_snapshot_date
where
    finance.service_date >= '2025-04-15'
    and snapshot.instance_account_id = 10214376
order by 1



---- First churn case

select *
from sandbox.juan_salgado.ge_growth_metrics_test
where account_id = 20814736




select *
from sandbox.juan_salgado.ge_growth_metrics_test
where last_date_finance is null


---- Validate if table is being updated

-- Max date from segment tables
select *
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_load_1_scd2
order by original_timestamp desc
limit 10




select distinct account_category
from functional.growth_engine.dim_growth_engine_customer_accounts
