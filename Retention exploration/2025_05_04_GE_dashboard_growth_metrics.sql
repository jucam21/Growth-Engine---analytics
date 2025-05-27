--- Query to update table

----------------------------------------------------------
--- Create table with all data

select account_id, loaded_date, count(*)
from sandbox.juan_salgado.ge_dashboard_test
group by 1,2
order by 3 desc
limit 10



--- Check if redeemed accounts have multiple clicks
select account_id, loaded_date, count(*)
from sandbox.juan_salgado.ge_dashboard_test
where unique_count_work_modal_2_click is not null
group by 1,2
order by 3 desc
limit 10



---- Zuora query to extract if customer redeemed coupon & ARR associated
-- Coupon was redeemed if last_segment = True & subscription status = 'Active'


--- Curate Zuora query
--- Adjust to include new logic: measure how many discounts have passed
with test as (
    select distinct account_id from sandbox.juan_salgado.ge_dashboard_test
),

redeemed_zuora as (
    select 
        mapping.instance_account_id,
        zuora.up_to_periods,
        zuora.billing_period,
        zuora.charge_model,
        tiers.currency,
        tiers.discount_amount,
        tiers.discount_percentage,
        SPLIT_PART(zuora.description, '-', 1) as coupon_id,
        count(*) as total_records,
        total_records - 1 as direct_charges,
        case when direct_charges > 0 then 1 else 0 end as coupon_redeemed
    from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
        left join
            foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
            on zuora.account_id = mapping.billing_account_id
        left join cleansed.zuora.zuora_subscriptions_bcv as subscription
            on zuora.subscription_id = subscription.id
        left join cleansed.zuora.zuora_rate_plan_charge_tiers_bcv as tiers
            on zuora.id = tiers.rate_plan_charge_id
        --left join cleansed.zuora.zuora_invoice_items_bcv as invoice
        --    on
        --        zuora.product_rate_plan_charge_id
        --        = invoice.product_rate_plan_charge_id
        --        and zuora.account_id = invoice.account_id
        inner join
            test
            on mapping.instance_account_id = test.account_id
    where
        zuora.is_last_segment = true
        and subscription.status in ('Active', 'Expired')
        and zuora.created_date >= '2025-01-01'
        and LOWER(coupon_id) like '%save%'
    group by
        all
)

select *
from redeemed_zuora



select
    instance_account_id,
    coupon_id,
    COUNT(*),
    sum(coupon_redeemed) as coupon_redeemed,
from redeemed_zuora
group by 1, 2
order by 3 desc
limit 10

--- Query to share with Aaron

-- Redeemed accounts
select
    zuora.created_date,
    zuora.account_id,
    zuora.description,
    zuora.effective_start_date,
    zuora.effective_end_date,
    zuora.up_to_periods,
    up_to_periods_type,
    zuora.is_last_segment,
    subscription.status,
    --invoice.*,
    --invoice.charge_amount_home_currency,
    --invoice.home_currency,
    tiers_2.*
from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
    left join cleansed.zuora.zuora_subscriptions_bcv as subscription
        on zuora.subscription_id = subscription.id
    --left join cleansed.zuora.zuora_invoice_items_bcv as invoice
    --    on
    --        zuora.product_rate_plan_charge_id
    --        = invoice.product_rate_plan_charge_id
    --        and zuora.account_id = invoice.account_id
    left join cleansed.zuora.ZUORA_RATE_PLAN_CHARGE_TIERS_BCV as tiers_2
        on zuora.id = tiers_2.RATE_PLAN_CHARGE_ID
where
    mapping.instance_account_id in (24853211)
    and lower(zuora.description) like '%save20%'
    --and is_last_segment = true
    --and subscription.status = 'Active'
    --and zuora.created_date >= '2025-01-01'
order by zuora.created_date


--- Account with multiple redemptions, different logic
--- mapping.instance_account_id in (21745328)
--- and lower(zuora.description) like '%zenpause%'



--- Extracting all columns to identify fields
--- Succesfully redeemed coupon #1 all data
select
    subscription.status, 
    zuora.*,
    --invoice.*,
    --product.*,
    --product_rate_plan.*,
    --rate_plans.*,
    --tiers.*,
    tiers_2.*
from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
    left join cleansed.zuora.zuora_subscriptions_bcv as subscription
        on zuora.subscription_id = subscription.id
    left join cleansed.zuora.zuora_invoice_items_bcv as invoice
        on zuora.PRODUCT_RATE_PLAN_CHARGE_ID = invoice.PRODUCT_RATE_PLAN_CHARGE_ID
        and zuora.account_id = invoice.account_id
    left join cleansed.zuora.zuora_product_rate_plan_charges_bcv as product
        on zuora.PRODUCT_RATE_PLAN_CHARGE_ID = product.id
    left join cleansed.zuora.zuora_product_rate_plans_bcv as product_rate_plan
        on product.PRODUCT_RATE_PLAN_ID = product_rate_plan.id
    left join cleansed.zuora.ZUORA_RATE_PLANS_BCV as rate_plans
        on zuora.rate_plan_id = rate_plans.id
    left join cleansed.zuora.ZUORA_PRODUCT_RATE_PLAN_CHARGE_TIERS_BCV as tiers
        on zuora.product_rate_plan_charge_id = tiers.product_rate_plan_charge_id
        --and zuora.account_id = product.account_id
    left join cleansed.zuora.ZUORA_RATE_PLAN_CHARGE_TIERS_BCV as tiers_2
        on zuora.id = tiers_2.RATE_PLAN_CHARGE_ID
where
    mapping.instance_account_id in (22169213)
    and lower(zuora.description) like '%save20%'
    --and subscription.status = 'Active'
order by zuora.created_date



select *
from cleansed.zuora.ZUORA_PRODUCT_RATE_PLAN_CHARGE_TIERS_BCV
where product_rate_plan_charge_id = '2c92a0fe5d8298df015dbdbdf1b94eb0'



select *
from cleansed.zuora.ZUORA_RATE_PLAN_CHARGE_TIERS_BCV
where product_rate_plan_charge_id = '2c92a0fe5d8298df015dbdbdf1b94eb0'


select *
from cleansed.zuora.ZUORA_RATE_PLAN_CHARGE_TIERS_BCV
where RATE_PLAN_CHARGE_ID = '8a1298489638986601964ae055040cf5'


--- Account to be renewed
select
    subscription.status, 
    zuora.*,
    invoice.*
from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
    left join cleansed.zuora.zuora_subscriptions_bcv as subscription
        on zuora.subscription_id = subscription.id
    left join cleansed.zuora.zuora_invoice_items_bcv as invoice
        on zuora.PRODUCT_RATE_PLAN_CHARGE_ID = invoice.PRODUCT_RATE_PLAN_CHARGE_ID
        and zuora.account_id = invoice.account_id
where
    mapping.instance_account_id in (10996450)
    and lower(zuora.description) like '%save15%'
    --and subscription.status = 'Active'
order by zuora.created_date





--- Succesfully redeemed coupon #1
select 
zuora.created_date, zuora.account_id, zuora.description,
zuora.EFFECTIVE_START_DATE, zuora.EFFECTIVE_END_DATE,
zuora.UP_TO_PERIODS, UP_TO_PERIODS_TYPE, zuora.is_last_segment,
subscription.status
from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
    left join cleansed.zuora.zuora_subscriptions_bcv as subscription
        on zuora.subscription_id = subscription.id
where 
    mapping.instance_account_id in (10214376)
    and lower(zuora.description) like '%save50%'
    --and subscription.status = 'Active'
order by zuora.created_date




--- Succesfully redeemed coupon #2
select 
zuora.created_date, zuora.account_id, zuora.description,
zuora.EFFECTIVE_START_DATE, zuora.EFFECTIVE_END_DATE,
zuora.UP_TO_PERIODS, UP_TO_PERIODS_TYPE, zuora.is_last_segment,
subscription.status
from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
    left join cleansed.zuora.zuora_subscriptions_bcv as subscription
        on zuora.subscription_id = subscription.id
where 
    mapping.instance_account_id in (11082714)
    and lower(zuora.description) like '%save20%'
    --and subscription.status = 'Active'
order by zuora.created_date




--- Account to be renewed
select 
zuora.created_date, zuora.account_id, zuora.description,
zuora.EFFECTIVE_START_DATE, zuora.EFFECTIVE_END_DATE,
zuora.UP_TO_PERIODS, UP_TO_PERIODS_TYPE, zuora.is_last_segment,
subscription.status
from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
    left join cleansed.zuora.zuora_subscriptions_bcv as subscription
        on zuora.subscription_id = subscription.id
where 
    mapping.instance_account_id in (10996450)
    and lower(zuora.description) like '%save15%'
    --and subscription.status = 'Active'
order by zuora.created_date













-----------------------------------------------------------
-- Main Query

--- Selecting only 1 record per account_id
--- For redeemed accounts, we take the record with the redemption
--- For non-redeemed accounts, we take the most recent record
with redeemed as (
    select
        *,
        'redeemed' as data_type
    from sandbox.juan_salgado.ge_dashboard_test
    where unique_count_work_modal_2_click is not null
),

not_redeemed as (
    select a.*,
row_number() over (
            partition by a.account_id
            order by a.loaded_date desc
        ) as rank,
        'not_redeemed' as data_type
from sandbox.juan_salgado.ge_dashboard_test as a
left join redeemed as b
on a.account_id = b.account_id
where b.account_id is null
qualify rank = 1
),

unique_obs as (
    select
        loaded_date,
        account_id,
        offer_id,
        promo_code_id,
        total_count_prompt_loads,
        unique_count_prompt_loads,
        total_count_prompt_clicks,
        unique_count_prompt_clicks,
        total_count_prompt_dismiss,
        unique_count_prompt_dismiss,
        total_count_work_modal_1_click,
        unique_count_work_modal_1_click,
        total_count_work_modal_1_dismiss,
        unique_count_work_modal_1_dismiss,
        total_count_work_modal_2_click,
        unique_count_work_modal_2_click,
        total_count_work_modal_2_dismiss,
        unique_count_work_modal_2_dismiss,
        total_count_work_modal_2_go_back,
        unique_count_work_modal_2_go_back,
        total_count_follow_up_close,
        unique_count_follow_up_close,
        total_count_follow_up_dismiss,
        unique_count_follow_up_dismiss,
        total_count_follow_up_subscription,
        unique_count_follow_up_subscription,
        first_loaded_date,
        days_since_first_loaded,
        date_to_join,
        account_state,
        account_category,
        account_age_in_days,
        subscription_start_date,
        subscription_renewal_date,
        subscription_age_in_days,
        days_to_subscription_renewal,
        subscription_term_length,
        subscription_term_type,
        crm_account_id,
        region,
        market_segment,
        sales_model,
        employee_count_range,
        net_arr_usd_instance,
        net_arr_usd_crm,
        crm_arr_range,
        instance_arr_range,
        churn_driver,
        churn_tooltip,
        predicted_probability,
        sku_mix,
        addon_mix,
        sku_type,
        seats_capacity,
        payment_method_name,
        zuora_coupon_redeemed,
        zuora_unique_coupon_redeemed,
        zuora_charge_model,
        zuora_total_discount_amount,
        data_type
    from redeemed
    union all
    select
        loaded_date,
        account_id,
        offer_id,
        promo_code_id,
        total_count_prompt_loads,
        unique_count_prompt_loads,
        total_count_prompt_clicks,
        unique_count_prompt_clicks,
        total_count_prompt_dismiss,
        unique_count_prompt_dismiss,
        total_count_work_modal_1_click,
        unique_count_work_modal_1_click,
        total_count_work_modal_1_dismiss,
        unique_count_work_modal_1_dismiss,
        total_count_work_modal_2_click,
        unique_count_work_modal_2_click,
        total_count_work_modal_2_dismiss,
        unique_count_work_modal_2_dismiss,
        total_count_work_modal_2_go_back,
        unique_count_work_modal_2_go_back,
        total_count_follow_up_close,
        unique_count_follow_up_close,
        total_count_follow_up_dismiss,
        unique_count_follow_up_dismiss,
        total_count_follow_up_subscription,
        unique_count_follow_up_subscription,
        first_loaded_date,
        days_since_first_loaded,
        date_to_join,
        account_state,
        account_category,
        account_age_in_days,
        subscription_start_date,
        subscription_renewal_date,
        subscription_age_in_days,
        days_to_subscription_renewal,
        subscription_term_length,
        subscription_term_type,
        crm_account_id,
        region,
        market_segment,
        sales_model,
        employee_count_range,
        net_arr_usd_instance,
        net_arr_usd_crm,
        crm_arr_range,
        instance_arr_range,
        churn_driver,
        churn_tooltip,
        predicted_probability,
        sku_mix,
        addon_mix,
        sku_type,
        seats_capacity,
        payment_method_name,
        zuora_coupon_redeemed,
        zuora_unique_coupon_redeemed,
        zuora_charge_model,
        zuora_total_discount_amount,
        data_type
    from not_redeemed
    union all
    select
        loaded_date,
        account_id,
        offer_id,
        promo_code_id,
        total_count_prompt_loads,
        unique_count_prompt_loads,
        total_count_prompt_clicks,
        unique_count_prompt_clicks,
        total_count_prompt_dismiss,
        unique_count_prompt_dismiss,
        total_count_work_modal_1_click,
        unique_count_work_modal_1_click,
        total_count_work_modal_1_dismiss,
        unique_count_work_modal_1_dismiss,
        total_count_work_modal_2_click,
        unique_count_work_modal_2_click,
        total_count_work_modal_2_dismiss,
        unique_count_work_modal_2_dismiss,
        total_count_work_modal_2_go_back,
        unique_count_work_modal_2_go_back,
        total_count_follow_up_close,
        unique_count_follow_up_close,
        total_count_follow_up_dismiss,
        unique_count_follow_up_dismiss,
        total_count_follow_up_subscription,
        unique_count_follow_up_subscription,
        first_loaded_date,
        days_since_first_loaded,
        date_to_join,
        account_state,
        account_category,
        account_age_in_days,
        subscription_start_date,
        subscription_renewal_date,
        subscription_age_in_days,
        days_to_subscription_renewal,
        subscription_term_length,
        subscription_term_type,
        crm_account_id,
        region,
        market_segment,
        sales_model,
        employee_count_range,
        net_arr_usd_instance,
        net_arr_usd_crm,
        crm_arr_range,
        instance_arr_range,
        churn_driver,
        churn_tooltip,
        predicted_probability,
        sku_mix,
        addon_mix,
        sku_type,
        seats_capacity,
        payment_method_name,
        zuora_coupon_redeemed,
        zuora_unique_coupon_redeemed,
        zuora_charge_model,
        zuora_total_discount_amount,
        'baseline_1' as data_type
    from not_redeemed
),

max_date_finance as (
    select
        max(service_date) as last_date_finance
    from foundational.finance.fact_recurring_revenue_bcv_enriched
),

--- BCV ARR for renewal rate, churn rate, and expansion rate
import_finance_recurring_revenue_instance_arr as (
    select
        snapshot_bcv.instance_account_id as zendesk_account_id,
        sum(finance.net_arr_usd) as net_arr_usd
    from foundational.finance.fact_recurring_revenue_bcv_enriched as finance
        left join
            foundational.customer.entity_mapping_daily_snapshot_bcv
                as snapshot_bcv
            on finance.billing_account_id = snapshot_bcv.billing_account_id
    group by all
),

unique_obs_arr_bcv as (
    select
        a.*,
        c.last_date_finance,
        b.net_arr_usd as instance_net_arr_usd_bcv
    from unique_obs as a
        left join import_finance_recurring_revenue_instance_arr as b
            on a.account_id = b.zendesk_account_id
        cross join max_date_finance as c
),

--- Daily ARR for retention curves

daily_arr as (
    select
        finance.service_date,
        snapshot.instance_account_id as zendesk_account_id,
        sum(finance.net_arr_usd) as net_arr_usd
    from foundational.finance.fact_recurring_revenue_daily_snapshot_enriched as finance
    left join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on finance.billing_account_id = snapshot.billing_account_id
        and finance.service_date = snapshot.source_snapshot_date
    where finance.service_date >= '2025-03-01'
    group by all
),

--- Creating future dates, based on initial renewal date, for joining ARR data

unique_obs_future_dates as (
    select
        *,
        -- Adding X months or years to the renewal date based on the subscription term type
        case
            when subscription_term_type = 'monthly' then dateadd(month, 1, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 1, subscription_renewal_date)
            else null
        end as renewal_p1_date,
        case
            when subscription_term_type = 'monthly' then dateadd(month, 2, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 2, subscription_renewal_date)
            else null
        end as renewal_p2_date,
        case
            when subscription_term_type = 'monthly' then dateadd(month, 3, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 3, subscription_renewal_date)
            else null
        end as renewal_p3_date,
        case
            when subscription_term_type = 'monthly' then dateadd(month, 4, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 4, subscription_renewal_date)
            else null
        end as renewal_p4_date,
        case
            when subscription_term_type = 'monthly' then dateadd(month, 5, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 5, subscription_renewal_date)
            else null
        end as renewal_p5_date,
        -- Determining if the renewal date have passed
        -- Customers available to renew on each future date
        case when renewal_p1_date <= last_date_finance then 1 else 0 end as renewal_p1_date_passed,
        case when renewal_p2_date <= last_date_finance then 1 else 0 end as renewal_p2_date_passed,
        case when renewal_p3_date <= last_date_finance then 1 else 0 end as renewal_p3_date_passed,
        case when renewal_p4_date <= last_date_finance then 1 else 0 end as renewal_p4_date_passed,
        case when renewal_p5_date <= last_date_finance then 1 else 0 end as renewal_p5_date_passed
    from unique_obs_arr_bcv
),

--- Joining future renewal dates with daily ARR data

unique_retention_curves as (
    select
        a.*,
        -- Creating flags for dashboard
        -- ARR that was renewed on each future date
        renewal_1.net_arr_usd as renewed_p1_arr,
        renewal_2.net_arr_usd as renewed_p2_arr,
        renewal_3.net_arr_usd as renewed_p3_arr,
        renewal_4.net_arr_usd as renewed_p4_arr,
        renewal_5.net_arr_usd as renewed_p5_arr,
        --- ARR available to renew
        case when renewal_p1_date_passed = 1 then net_arr_usd_instance else null end as renewal_p1_arr_passed,
        case when renewal_p2_date_passed = 1 then net_arr_usd_instance else null end as renewal_p2_arr_passed,
        case when renewal_p3_date_passed = 1 then net_arr_usd_instance else null end as renewal_p3_arr_passed,
        case when renewal_p4_date_passed = 1 then net_arr_usd_instance else null end as renewal_p4_arr_passed,
        case when renewal_p5_date_passed = 1 then net_arr_usd_instance else null end as renewal_p5_arr_passed,
        -- Flag if customer renewed on each future date
        case when renewed_p1_arr > 0 then 1 else 0 end as renewed_p1_account,
        case when renewed_p2_arr > 0 then 1 else 0 end as renewed_p2_account,
        case when renewed_p3_arr > 0 then 1 else 0 end as renewed_p3_account,
        case when renewed_p4_arr > 0 then 1 else 0 end as renewed_p4_account,
        case when renewed_p5_arr > 0 then 1 else 0 end as renewed_p5_account
    from unique_obs_future_dates as a
        left join daily_arr as renewal_1
            on a.account_id = renewal_1.zendesk_account_id
            and a.renewal_p1_date = renewal_1.service_date
        left join daily_arr as renewal_2
            on a.account_id = renewal_2.zendesk_account_id
            and a.renewal_p2_date = renewal_2.service_date
        left join daily_arr as renewal_3
            on a.account_id = renewal_3.zendesk_account_id
            and a.renewal_p3_date = renewal_3.service_date
        left join daily_arr as renewal_4
            on a.account_id = renewal_4.zendesk_account_id
            and a.renewal_p4_date = renewal_4.service_date
        left join daily_arr as renewal_5
            on a.account_id = renewal_5.zendesk_account_id
            and a.renewal_p5_date = renewal_5.service_date
),

--- Growth metrics

growth_metrics as (
    select
        *,
        -- Renewal Rate
        case
            when subscription_renewal_date <= last_date_finance then 1 else 0
        end as renewal_passed,
        case
            when
                instance_net_arr_usd_bcv > 0 and renewal_passed = 1 then 1 else 0
        end as subscription_renewed,
        -- ARR changes
        instance_net_arr_usd_bcv - net_arr_usd_instance as arr_change,
        -- Same ARR
        case when instance_net_arr_usd_bcv = net_arr_usd_instance and net_arr_usd_instance > 0 then 1 else 0 end as no_change,
        case when instance_net_arr_usd_bcv = net_arr_usd_instance and net_arr_usd_instance > 0 then net_arr_usd_instance else null end as no_change_arr,
        -- Churn ARR
        case when (instance_net_arr_usd_bcv = 0 or instance_net_arr_usd_bcv is null) and net_arr_usd_instance > 0 then 1 else 0 end as churn,
        case when (instance_net_arr_usd_bcv = 0 or instance_net_arr_usd_bcv is null) and net_arr_usd_instance > 0 then net_arr_usd_instance else null end as churn_arr,
        -- Expansion ARR
        case when arr_change > 0 and net_arr_usd_instance > 0 then 1 else 0 end as expansion,
        case when arr_change > 0 and net_arr_usd_instance > 0 then net_arr_usd_instance else null end as expansion_arr,
        -- Contraction ARR
        case when arr_change < 0 and net_arr_usd_instance > 0 then 1 else 0 end as contraction,
        case when arr_change < 0 and net_arr_usd_instance > 0 then net_arr_usd_instance else null end as contraction_arr,

    from unique_retention_curves
)

select data_type,
count(*) as count,
count(distinct account_id) as unique_accounts,
sum(renewal_passed) renewal_passed,
sum(subscription_renewed) subscription_renewed,
sum(no_change) no_change,
sum(churn) churn,
sum(expansion) expansion,
sum(contraction) contraction,
from growth_metrics
group by 1




select *
from growth_metrics
where data_type = 'redeemed'
and renewal_passed = 1




select data_type,
count(*) as count,
count(distinct account_id) as unique_accounts,
sum(renewal_passed) renewal_passed,
sum(subscription_renewed) subscription_renewed,
sum(no_change) no_change,
sum(churn) churn,
sum(expansion) expansion,
sum(contraction) contraction,
from growth_metrics
group by 1












-----------------------------------
--- Validation queries


--- Check coupon amounts

select account_id, subscription_term_type, promo_code_id, net_arr_usd_instance,
instance_net_arr_usd_bcv, zuora_effective_charges,
zuora_total_discount_amount, zuora_effective_discount_amount,
from sandbox.juan_salgado.ge_growth_metrics_test
where zuora_effective_discount_amount > 0



select *
from sandbox.juan_salgado.ge_growth_metrics_test
where churn_arr > 0


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





---- Validate redeemed amount from Zuora
select
    renewal_passed,
    loaded_date,
    last_date_finance,
    subscription_renewal_date,
    account_id,
    zuora_coupon_redeemed,
    promo_code_id,
    subscription_term_type,
    net_arr_usd_instance,
    instance_net_arr_usd_bcv,
    zuora_total_discount_amount,
    arr_change,
    contraction_arr
from sandbox.juan_salgado.ge_growth_metrics_test
where data_type = 'redeemed'
order by renewal_passed desc, loaded_date



---- Not redeemed churn
select
    renewal_passed,
    loaded_date,
    last_date_finance,
    subscription_renewal_date,
    account_id,
    zuora_coupon_redeemed,
    promo_code_id,
    subscription_term_type,
    net_arr_usd_instance,
    instance_net_arr_usd_bcv,
    arr_change,
    contraction_arr
from sandbox.juan_salgado.ge_growth_metrics_test
where data_type = 'not_redeemed'
order by renewal_passed desc, loaded_date




--- Validate counts
select
    count(*),
    count(distinct account_id),
    count(unique_count_work_modal_2_click),
    count(distinct unique_count_work_modal_2_click),
    sum(case when last_date_finance is null then 1 else 0 end) as finance_null,
    sum(future_renewal) future_renewal,
    sum(arr_null) arr_null,
from growth_metrics





select *
from growth_metrics
where future_renewal = 1




select *
from foundational.customer.dim_instance_accounts_daily_snapshot
where instance_account_id = 24905253
order by source_snapshot_date
instance_account_id in (24905253,
24853211)




2025-04-21 00:00:00.000
2025-04-15 00:00:00.000


24905253
24853211


select account_category, COUNT(*)
from sandbox.juan_salgado.ge_dashboard_test
group by 1
where account_id = 24905253



select *
from sandbox.juan_salgado.ge_dashboard_test
where account_category is null

select distinct instance_account_derived_type
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv




select
        finance.*
    from foundational.finance.fact_recurring_revenue_bcv_enriched as finance
        left join
            foundational.customer.entity_mapping_daily_snapshot_bcv
                as snapshot_bcv
            on finance.billing_account_id = snapshot_bcv.billing_account_id
where snapshot_bcv.instance_account_id = 10214376

