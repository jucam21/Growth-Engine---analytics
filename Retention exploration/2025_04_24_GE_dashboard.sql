-- Queries to create GE dashboard



--- 1. Understand flow of events


--- Segment events flow
  -- https://zendesk.atlassian.net/browse/PUFFINS-481

-- Prompt
  -- https://zendesk.atlassian.net/browse/PUFFINS-487
cleansed.segment_central_admin.GROWTH_ENGINE_ADMINHOMEBANNER1_PROMPT_LOAD_1_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_ADMINHOMEBANNER1_PROMPT_CLAIM_OFFER_CLICK_1_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_ADMINHOMEBANNER1_PROMPT_DISMISS_OFFER_1_SCD2


-- Work modal 1
  -- https://zendesk.atlassian.net/browse/PUFFINS-488
  -- Do not have event growth_engine_couponmodal_subscription_redirect
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_CLAIM_OFFER_CLICK_2_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_DISMISS_OFFER_1_SCD2


-- Work modal 2
  -- https://zendesk.atlassian.net/browse/PUFFINS-489
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_GO_BACK_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_WORK_MODAL_2_APPLY_OFFER_CLICK_1_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_WORK_MODAL_2_DISMISS_OFFER_1_SCD2


-- Follow up modal
  -- https://zendesk.atlassian.net/browse/PUFFINS-490
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_CLOSE_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_SUBSCRIPTION_DETAILS_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_DISMISS_SCD2





select *
from cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_WORK_MODAL_2_APPLY_OFFER_CLICK_1_SCD2



24853211


10214376
24905253
11082714


10996450
10214376
11082714

select
    date_trunc('month', original_timestamp),
    count(*) as tot_obs,
    count(distinct account_id) as unique_accounts
from
    cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
group by 1
order by 1


select
    date_trunc('month', original_timestamp),
    count(*) as tot_obs,
    count(distinct account_id) as unique_accounts
from
    cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_CLOSE_SCD2
group by 1
order by 1




select
    date_trunc('month', original_timestamp),
    count(*) as tot_obs,
    count(distinct account_id) as unique_accounts
from
    cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_SUBSCRIPTION_DETAILS_SCD2
group by 1
order by 1




select
    date_trunc('month', original_timestamp),
    count(*) as tot_obs,
    count(distinct account_id) as unique_accounts
from
    cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_DISMISS_SCD2
group by 1
order by 1




with unioned as (
    select original_timestamp, account_id
    from cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_CLOSE_SCD2
    union all
    select original_timestamp, account_id
    from cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_SUBSCRIPTION_DETAILS_SCD2
    union all
    select original_timestamp, account_id
    from cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_DISMISS_SCD2
)


select
    date_trunc('month', original_timestamp),
    count(*) as tot_obs,
    count(distinct account_id) as unique_accounts
from
    unioned
group by 1
order by 1



select *
from cleansed.zuora.zuora_rate_plan_charges_bcv 





select *
from cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2











-----------------------------------------------------
--- Zuora redemption codes

    -- Santi's query to use Zuora for promo code redemption
select distinct
        listagg(distinct code_list.coupon) as promo_code,
        mapping.crm_account_id,
        mapping.instance_account_id,
        min(date(zuora.created_date)) as zuora_date_promo_code_applied
    from functional.growth_analytics_staging.startup_promo_codes code_list
    left join cleansed.zuora.zuora_rate_plan_charges_bcv zuora
        on code_list.coupon = zuora.description
    left join foundational.customer.entity_mapping_daily_snapshot_bcv mapping
        on zuora.account_id = mapping.billing_account_id
    where mapping.crm_account_id is not null
        and mapping.instance_account_id is not null
    group by all


-- List of accounts that have been redeemed
select *
from cleansed.segment_central_admin.GROWTH_ENGINE_ADMINHOMEBANNER1_PROMPT_LOAD_1_SCD2
where account_id in (
    11082714,
    24853211,
    10996450,
    10214376,
    24905253
)

-- Searching redeemed accounts in Zuora
select *
from cleansed.zuora.zuora_rate_plan_charges_bcv zuora
left join foundational.customer.entity_mapping_daily_snapshot_bcv mapping
        on zuora.account_id = mapping.billing_account_id
where mapping.instance_account_id in (
    11082714,
    24853211,
    10996450,
    10214376,
    24905253
)





select *
from cleansed.zuora.zuora_rate_plan_charges_bcv zuora
left join foundational.customer.entity_mapping_daily_snapshot_bcv mapping
        on zuora.account_id = mapping.billing_account_id
where mapping.instance_account_id in (
    11082714
)
--and zuora.CREATED_DATE >= '2025-01-01'
and lower(zuora.description) like '%save20%'


SAVE20SUITEGROWTH


-- Search for accounts in Zuora

select *
from cleansed.zuora.zuora_rate_plan_charges_bcv zuora
left join foundational.customer.entity_mapping_daily_snapshot_bcv mapping
        on zuora.account_id = mapping.billing_account_id
where mapping.instance_account_id in (
    10214376
)
and lower(zuora.description) like '%save50%'

select *
from sandbox.juan_salgado.ge_dashboard_test
where account_id = 10214376






-- Aaron suggested to add this filter to look only at charges from promo codes
select *
from cleansed.zuora.zuora_rate_plan_charges_bcv zuora
where product_rate_plan_charge_id IN ('2c92a0fe5d8298df015dbdbdf1b94eb0', '2c92a0fe5d8298df015dbdbdf1d24eba')



select *
from cleansed.segment_central_admin.GROWTH_ENGINE_ADMINHOMEBANNER1_PROMPT_LOAD_1_SCD2
where account_id = 11082714





select distinct
        zuora.*
    from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
    inner join (
        select distinct account_id
        from cleansed.segment_central_admin.GROWTH_ENGINE_ADMINHOMEBANNER1_PROMPT_LOAD_1_SCD2
    ) as accounts
    on mapping.instance_account_id = accounts.account_id
    where zuora.effective_start_date >= '2025-04-01'





    where zuora.description in (
        'SAVE30GROWTH',
'SAVE20GROWTH',
'SAVE30TEAMANNUAL',
'SAVE15SUITE',
'SAVE15PROFESSIONAL',
'SAVE20SUITEGROWTH',
'SAVE15PROFESSIONAL2',
'SAVE30ANNUAL',
'SAVE20GROWTHANNUAL',
'SAVE50'
    )







--- 2. Main GE dashboard query

---- Joining segment events


-- Prompt
  -- https://zendesk.atlassian.net/browse/PUFFINS-487
cleansed.segment_central_admin.GROWTH_ENGINE_ADMINHOMEBANNER1_PROMPT_LOAD_1_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_ADMINHOMEBANNER1_PROMPT_CLAIM_OFFER_CLICK_1_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_ADMINHOMEBANNER1_PROMPT_DISMISS_OFFER_1_SCD2


-- Work modal 1
  -- https://zendesk.atlassian.net/browse/PUFFINS-488
  -- Do not have event growth_engine_couponmodal_subscription_redirect
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_CLAIM_OFFER_CLICK_2_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_DISMISS_OFFER_1_SCD2


-- Work modal 2
  -- https://zendesk.atlassian.net/browse/PUFFINS-489
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_GO_BACK_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_WORK_MODAL_2_APPLY_OFFER_CLICK_1_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_WORK_MODAL_2_DISMISS_OFFER_1_SCD2


-- Follow up modal
  -- https://zendesk.atlassian.net/browse/PUFFINS-490
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_CLOSE_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_SUBSCRIPTION_DETAILS_SCD2
cleansed.segment_central_admin.GROWTH_ENGINE_COUPONMODAL_SUBSCRIPTION_SUBMITTED_DISMISS_SCD2




-- Searching accounts in prompt
select *
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_load_1_scd2
where account_id = 24905253
order by original_timestamp 


select *
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2
where account_id = 24905253
order by original_timestamp 


select *
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_dismiss_offer_1_scd2
where account_id = 24905253
order by original_timestamp 








select count( account_id)
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2


select count( account_id)
from cleansed.segment_central_admin.growth_engine_couponmodal_claim_offer_click_2_scd2


select count( account_id)
from cleansed.segment_central_admin.growth_engine_couponmodal_dismiss_offer_1_scd2



select count(distinct account_id)
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2


select count(distinct account_id)
from cleansed.segment_central_admin.growth_engine_couponmodal_claim_offer_click_2_scd2


select count(distinct account_id)
from cleansed.segment_central_admin.growth_engine_couponmodal_dismiss_offer_1_scd2


select distinct account_id
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2


select distinct account_id
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2


select distinct account_id
from cleansed.segment_central_admin.growth_engine_couponmodal_dismiss_offer_1_scd2








select distinct account_id
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_load_1_scd2


select distinct account_id
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2


select distinct account_id
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_dismiss_offer_1_scd2



select *
from sandbox.juan_salgado.ge_dashboard_test
where account_id = 13760446



prompt_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2
    group by all
),


-- Work modal 1 events
work_modal_1_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_claim_offer_click_2_scd2
    group by all
),

work_modal_1_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_dismiss_offer_1_scd2
    group by all
),







--- Main query to join segment events

-- First, counting how many events are triggered daily per account_id, offer_id and promo_code_id
-- Unique count: uses account_id and will use function count distinct in Tableau
-- Total count: counts total interactions per day and will use function sum in Tableau
-- Prompt events
with prompt_load as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_load_1_scd2
    group by all
),

prompt_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2
    group by all
),

prompt_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_dismiss_offer_1_scd2
    group by all
),

-- Work modal 1 events
work_modal_1_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_claim_offer_click_2_scd2
    group by all
),

work_modal_1_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_dismiss_offer_1_scd2
    group by all
),

-- Work modal 2 events
work_modal_2_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
    group by all
),

work_modal_2_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_dismiss_offer_1_scd2
    group by all
),

work_modal_2_go_back as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_go_back_scd2
    group by all
),

-- Follow up modal events
follow_up_close as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_close_scd2
    group by all
),

follow_up_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_dismiss_scd2
    group by all
),

follow_up_subscription as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_subscription_details_scd2
    group by all
),

-- Joining all events
-- Joining using date (day), account_id, offer_id and promo_code_id as keys and
-- left joining all events against prompt_load, since is the starting point of the flow
main as (
    select
        prompt_load_.date as loaded_date,
        prompt_load_.account_id,
        prompt_load_.offer_id,
        prompt_load_.promo_code_id,
        prompt_load_.total_count as total_count_prompt_loads,
        prompt_load_.unique_count as unique_count_prompt_loads,
        prompt_click_.total_count as total_count_prompt_clicks,
        prompt_click_.unique_count as unique_count_prompt_clicks,
        prompt_dismiss_.total_count as total_count_prompt_dismiss,
        prompt_dismiss_.unique_count as unique_count_prompt_dismiss,
        work_modal_1_click_.total_count as total_count_work_modal_1_click,
        work_modal_1_click_.unique_count as unique_count_work_modal_1_click,
        work_modal_1_dismiss_.total_count as total_count_work_modal_1_dismiss,
        work_modal_1_dismiss_.unique_count as unique_count_work_modal_1_dismiss,
        work_modal_2_click_.total_count as total_count_work_modal_2_click,
        work_modal_2_click_.unique_count as unique_count_work_modal_2_click,
        work_modal_2_dismiss_.total_count as total_count_work_modal_2_dismiss,
        work_modal_2_dismiss_.unique_count as unique_count_work_modal_2_dismiss,
        work_modal_2_go_back_.total_count as total_count_work_modal_2_go_back,
        work_modal_2_go_back_.unique_count as unique_count_work_modal_2_go_back,
        follow_up_close_.total_count as total_count_follow_up_close,
        follow_up_close_.unique_count as unique_count_follow_up_close,
        follow_up_dismiss_.total_count as total_count_follow_up_dismiss,
        follow_up_dismiss_.unique_count as unique_count_follow_up_dismiss,
        follow_up_subscription_.total_count
            as total_count_follow_up_subscription,
        follow_up_subscription_.unique_count
            as unique_count_follow_up_subscription
    from prompt_load as prompt_load_
        left join prompt_click as prompt_click_
            on
                prompt_load_.account_id = prompt_click_.account_id
                and prompt_load_.date = prompt_click_.date
                and prompt_load_.offer_id = prompt_click_.offer_id
                and prompt_load_.promo_code_id = prompt_click_.promo_code_id
        left join prompt_dismiss as prompt_dismiss_
            on
                prompt_load_.account_id = prompt_dismiss_.account_id
                and prompt_load_.date = prompt_dismiss_.date
                and prompt_load_.offer_id = prompt_dismiss_.offer_id
                and prompt_load_.promo_code_id = prompt_dismiss_.promo_code_id
        left join work_modal_1_click as work_modal_1_click_
            on
                prompt_load_.account_id = work_modal_1_click_.account_id
                and prompt_load_.date = work_modal_1_click_.date
                and prompt_load_.offer_id = work_modal_1_click_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_1_click_.promo_code_id
        left join work_modal_1_dismiss as work_modal_1_dismiss_
            on
                prompt_load_.account_id = work_modal_1_dismiss_.account_id
                and prompt_load_.date = work_modal_1_dismiss_.date
                and prompt_load_.offer_id = work_modal_1_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_1_dismiss_.promo_code_id
        left join work_modal_2_click as work_modal_2_click_
            on
                prompt_load_.account_id = work_modal_2_click_.account_id
                and prompt_load_.date = work_modal_2_click_.date
                and prompt_load_.offer_id = work_modal_2_click_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_click_.promo_code_id
        left join work_modal_2_dismiss as work_modal_2_dismiss_
            on
                prompt_load_.account_id = work_modal_2_dismiss_.account_id
                and prompt_load_.date = work_modal_2_dismiss_.date
                and prompt_load_.offer_id = work_modal_2_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_dismiss_.promo_code_id
        left join work_modal_2_go_back as work_modal_2_go_back_
            on
                prompt_load_.account_id = work_modal_2_go_back_.account_id
                and prompt_load_.date = work_modal_2_go_back_.date
                and prompt_load_.offer_id = work_modal_2_go_back_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_go_back_.promo_code_id
        left join follow_up_close as follow_up_close_
            on
                prompt_load_.account_id = follow_up_close_.account_id
                and prompt_load_.date = follow_up_close_.date
                and prompt_load_.offer_id = follow_up_close_.offer_id
                and prompt_load_.promo_code_id = follow_up_close_.promo_code_id
        left join follow_up_dismiss as follow_up_dismiss_
            on
                prompt_load_.account_id = follow_up_dismiss_.account_id
                and prompt_load_.date = follow_up_dismiss_.date
                and prompt_load_.offer_id = follow_up_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = follow_up_dismiss_.promo_code_id
        left join follow_up_subscription as follow_up_subscription_
            on
                prompt_load_.account_id = follow_up_subscription_.account_id
                and prompt_load_.date = follow_up_subscription_.date
                and prompt_load_.offer_id = follow_up_subscription_.offer_id
                and prompt_load_.promo_code_id
                = follow_up_subscription_.promo_code_id
)

select
    sum(total_count_prompt_loads) as total_count_prompt_loads,
    sum(total_count_prompt_clicks) as total_count_prompt_clicks,
    sum(total_count_prompt_dismiss) as total_count_prompt_dismiss,
    sum(total_count_work_modal_1_click) as total_count_work_modal_1_click,
    sum(total_count_work_modal_1_dismiss) as total_count_work_modal_1_dismiss,
    sum(total_count_work_modal_2_click) as total_count_work_modal_2_click,
    sum(total_count_work_modal_2_dismiss) as total_count_work_modal_2_dismiss,
    sum(total_count_work_modal_2_go_back) as total_count_work_modal_2_go_back,
    sum(total_count_follow_up_close) as total_count_follow_up_close,
    sum(total_count_follow_up_dismiss) as total_count_follow_up_dismiss,
    sum(total_count_follow_up_subscription)
        as total_count_follow_up_subscription
from main;



-- Some accounts have more total interactions in later parts of the funnel,
-- mainly from clicks
select *
from main
where total_count_prompt_clicks > total_count_prompt_loads






-- Main query to extract account info

/* 
   start-end date
   Offer ids 
   action type: coupon
   Values 
   unique or total
   day/week/month/quarter
account age               ------- OK
region                    ------- OK
Existing plan type        ------- OK
Addon in subscription     ------- OK
employee seat count       ------- OK
days to renewal           ------- OK
market segment            ------- OK
Number of support seats   ------- OK
   Payment type 
churn driver              ------- OK
------------
-- Added variables
CRM ARR range             ------- OK
Instance ARR range        ------- OK
Suite seats               ------- OK
*/

-- 0 Churn driver
-- Has to be adjusted to use the closest date to the segment event loaded date.
-- This is an aproximation to the real date where C/C data was pulled

with cc_model as (
    select 
        instance_account_id,
        driver_1 as churn_driver,
        tool_tip_1 as churn_tooltip,
        row_number() over (
            partition by instance_account_id
            order by source_snapshot_date desc
        ) as rank
    from dev_functional.growth_engine.dim_growth_engine_churn_predictions
    qualify rank = 1
)

select *
from cc_model
limit 10


--- 1 Extracting data from GE accounts table
with ge_vars as (
    select
        zendesk_account_id,
        account_age_in_days,
        region,
        employee_count_range,
        days_to_subscription_renewal,
        market_segment,
        net_arr_usd_crm,
        net_arr_usd_instance,
        case
            when net_arr_usd_crm is null
                then '0. NULL'
            when net_arr_usd_crm < 5000 
                then '1. < 5K'
            when
                net_arr_usd_crm >= 5000 and net_arr_usd_crm < 10000
                then '2. 5K-10K'
            when
                net_arr_usd_crm >= 10000 and net_arr_usd_crm < 25000
                then '3. 10K-25K'
            when
                net_arr_usd_crm >= 25000 and net_arr_usd_crm < 50000
                then '4. 25K-50K'
            when
                net_arr_usd_crm >= 50000 and net_arr_usd_crm < 100000
                then '5. 50K-100K'
            when
                net_arr_usd_crm >= 100000 
                then '6. >100K'
            else '7. other'
        end as crm_arr_range,
        case
            when net_arr_usd_instance is null
                then '0. NULL'
            when net_arr_usd_instance < 5000 
                then '1. < 5K'
            when
                net_arr_usd_instance >= 5000 and net_arr_usd_instance < 10000
                then '2. 5K-10K'
            when
                net_arr_usd_instance >= 10000 and net_arr_usd_instance < 25000
                then '3. 10K-25K'
            when
                net_arr_usd_instance >= 25000 and net_arr_usd_instance < 50000
                then '4. 25K-50K'
            when
                net_arr_usd_instance >= 50000 and net_arr_usd_instance < 100000
                then '5. 50K-100K'
            when
                net_arr_usd_instance >= 100000 
                then '6. >100K'
            else '7. other'
        end as instance_arr_range
    from functional.growth_engine.dim_growth_engine_customer_accounts
)

select
    instance_arr_range,
    count(*)
from ge_vars
group by 1
order by 1


--- 2. Extracting SKUs, addons & suite/support seats
with import_skus_with_parsed_settings as (
    select
        account_id,
        name,
        state,
        boost,
        parse_json(plan_settings) as parsed_plan_settings,
        parsed_plan_settings:plan:value::string as plan,
        parsed_plan_settings:maxAgents:value::string as max_agents,
        row_number() over (
            partition by account_id, name, state -- Adding or removing state change results
            order by updated_at desc
        ) as rank
    from propagated_formatted.accountsdb.skus
    qualify rank = 1
),

-- Product SKUs have a plan associated
product_skus as (
    select
        account_id,
        listagg(concat(name, '_', plan), ', ') within GROUP (ORDER BY concat(name, '_', plan)) AS sku_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is not null
    group by account_id

),

-- Addon SKUs do not have a plan associated
addons_skus as (
    select
        account_id,
        listagg(name, ', ') within GROUP (ORDER BY name) AS addon_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is null
    group by account_id
),

support_seats as (
    select
        account_id,
        max(max_agents) as max_support_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%support%'
    group by account_id

),

suite_seats as (
    select
        account_id,
        max(max_agents) as max_suite_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%suite%'
    group by account_id

),

main_skus as (
    select
        product_.*,
        addon_.addon_mix,
        support_.max_support_seats,
        suite_.max_suite_seats
    from product_skus as product_
        left join addons_skus as addon_
            on product_.account_id = addon_.account_id
        left join support_seats as support_
            on product_.account_id = support_.account_id
        left join suite_seats as suite_
            on product_.account_id = suite_.account_id
)

select *
from main_skus
where account_id = 526508

-- 3 Payment method

with joined as (
    select distinct TRY_CAST(accounts.zendesk_account_id_c as INTEGER) instance_account_id, payment_methods.type as payment_method_name
    from cleansed.zuora.ZUORA_ACCOUNTS_BCV accounts
    left join cleansed.zuora.ZUORA_PAYMENT_METHODS_BCV payment_methods
        on accounts.default_payment_method_id = payment_methods.id
),

payment_mix as (
    select
        instance_account_id,
        listagg(payment_method_name, ', ') within GROUP (ORDER BY payment_method_name) as payment_methods
    from joined
    group by instance_account_id
)

select instance_account_id, count(*) tot_obs
from payment_mix
group by 1
having count(*) > 1


select *
from joined
where instance_account_id = 2268887




select count(*) tot_obs,
sum(case when payment_method_name is null then 1 else 0 end) null_payment_methods
from joined







----------------------------------------------------------
--- Create table with all data

create or replace table sandbox.juan_salgado.ge_dashboard_test as

-- Step 1: Segment events

-- First, counting how many events are triggered daily per account_id, offer_id and promo_code_id
-- Unique count: uses account_id and will use function count distinct in Tableau
-- Total count: counts total interactions per day and will use function sum in Tableau
-- Prompt events
with prompt_load as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_load_1_scd2
    group by all
),

prompt_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2
    group by all
),

prompt_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_dismiss_offer_1_scd2
    group by all
),

-- Work modal 1 events
work_modal_1_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_claim_offer_click_2_scd2
    group by all
),

work_modal_1_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_dismiss_offer_1_scd2
    group by all
),

-- Work modal 2 events
work_modal_2_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
    group by all
),

work_modal_2_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_dismiss_offer_1_scd2
    group by all
),

work_modal_2_go_back as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_go_back_scd2
    group by all
),

-- Follow up modal events
follow_up_close as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_close_scd2
    group by all
),

follow_up_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_dismiss_scd2
    group by all
),

follow_up_subscription as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_subscription_details_scd2
    group by all
),

-- Joining all events
-- Joining using date (day), account_id, offer_id and promo_code_id as keys and
-- left joining all events against prompt_load, since is the starting point of the flow
segment_events_all as (
    select
        prompt_load_.date as loaded_date,
        prompt_load_.account_id,
        prompt_load_.offer_id,
        prompt_load_.promo_code_id,
        prompt_load_.total_count as total_count_prompt_loads,
        prompt_load_.unique_count as unique_count_prompt_loads,
        prompt_click_.total_count as total_count_prompt_clicks,
        prompt_click_.unique_count as unique_count_prompt_clicks,
        prompt_dismiss_.total_count as total_count_prompt_dismiss,
        prompt_dismiss_.unique_count as unique_count_prompt_dismiss,
        work_modal_1_click_.total_count as total_count_work_modal_1_click,
        work_modal_1_click_.unique_count as unique_count_work_modal_1_click,
        work_modal_1_dismiss_.total_count as total_count_work_modal_1_dismiss,
        work_modal_1_dismiss_.unique_count as unique_count_work_modal_1_dismiss,
        work_modal_2_click_.total_count as total_count_work_modal_2_click,
        work_modal_2_click_.unique_count as unique_count_work_modal_2_click,
        work_modal_2_dismiss_.total_count as total_count_work_modal_2_dismiss,
        work_modal_2_dismiss_.unique_count as unique_count_work_modal_2_dismiss,
        work_modal_2_go_back_.total_count as total_count_work_modal_2_go_back,
        work_modal_2_go_back_.unique_count as unique_count_work_modal_2_go_back,
        follow_up_close_.total_count as total_count_follow_up_close,
        follow_up_close_.unique_count as unique_count_follow_up_close,
        follow_up_dismiss_.total_count as total_count_follow_up_dismiss,
        follow_up_dismiss_.unique_count as unique_count_follow_up_dismiss,
        follow_up_subscription_.total_count
            as total_count_follow_up_subscription,
        follow_up_subscription_.unique_count
            as unique_count_follow_up_subscription
    from prompt_load as prompt_load_
        left join prompt_click as prompt_click_
            on
                prompt_load_.account_id = prompt_click_.account_id
                and prompt_load_.date = prompt_click_.date
                and prompt_load_.offer_id = prompt_click_.offer_id
                and prompt_load_.promo_code_id = prompt_click_.promo_code_id
        left join prompt_dismiss as prompt_dismiss_
            on
                prompt_load_.account_id = prompt_dismiss_.account_id
                and prompt_load_.date = prompt_dismiss_.date
                and prompt_load_.offer_id = prompt_dismiss_.offer_id
                and prompt_load_.promo_code_id = prompt_dismiss_.promo_code_id
        left join work_modal_1_click as work_modal_1_click_
            on
                prompt_load_.account_id = work_modal_1_click_.account_id
                and prompt_load_.date = work_modal_1_click_.date
                and prompt_load_.offer_id = work_modal_1_click_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_1_click_.promo_code_id
        left join work_modal_1_dismiss as work_modal_1_dismiss_
            on
                prompt_load_.account_id = work_modal_1_dismiss_.account_id
                and prompt_load_.date = work_modal_1_dismiss_.date
                and prompt_load_.offer_id = work_modal_1_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_1_dismiss_.promo_code_id
        left join work_modal_2_click as work_modal_2_click_
            on
                prompt_load_.account_id = work_modal_2_click_.account_id
                and prompt_load_.date = work_modal_2_click_.date
                and prompt_load_.offer_id = work_modal_2_click_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_click_.promo_code_id
        left join work_modal_2_dismiss as work_modal_2_dismiss_
            on
                prompt_load_.account_id = work_modal_2_dismiss_.account_id
                and prompt_load_.date = work_modal_2_dismiss_.date
                and prompt_load_.offer_id = work_modal_2_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_dismiss_.promo_code_id
        left join work_modal_2_go_back as work_modal_2_go_back_
            on
                prompt_load_.account_id = work_modal_2_go_back_.account_id
                and prompt_load_.date = work_modal_2_go_back_.date
                and prompt_load_.offer_id = work_modal_2_go_back_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_go_back_.promo_code_id
        left join follow_up_close as follow_up_close_
            on
                prompt_load_.account_id = follow_up_close_.account_id
                and prompt_load_.date = follow_up_close_.date
                and prompt_load_.offer_id = follow_up_close_.offer_id
                and prompt_load_.promo_code_id = follow_up_close_.promo_code_id
        left join follow_up_dismiss as follow_up_dismiss_
            on
                prompt_load_.account_id = follow_up_dismiss_.account_id
                and prompt_load_.date = follow_up_dismiss_.date
                and prompt_load_.offer_id = follow_up_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = follow_up_dismiss_.promo_code_id
        left join follow_up_subscription as follow_up_subscription_
            on
                prompt_load_.account_id = follow_up_subscription_.account_id
                and prompt_load_.date = follow_up_subscription_.date
                and prompt_load_.offer_id = follow_up_subscription_.offer_id
                and prompt_load_.promo_code_id
                = follow_up_subscription_.promo_code_id
),

-- Step 2: adding additional variables

-- Step 2.1 Churn driver
-- Has to be adjusted to use the closest date to the segment event loaded date.
-- This is an aproximation to the real date where C/C data was pulled

cc_model as (
    select 
        instance_account_id,
        driver_1 as churn_driver,
        tool_tip_1 as churn_tooltip,
        row_number() over (
            partition by instance_account_id
            order by source_snapshot_date desc
        ) as rank
    from dev_functional.growth_engine.dim_growth_engine_churn_predictions
    qualify rank = 1
),

--- Step 2.2 Extracting data from GE accounts table
ge_table_vars as (
    select
        zendesk_account_id,
        account_age_in_days,
        region,
        employee_count_range,
        days_to_subscription_renewal,
        market_segment,
        net_arr_usd_crm,
        net_arr_usd_instance,
        case
            when net_arr_usd_crm is null
                then '0. NULL'
            when net_arr_usd_crm < 5000 
                then '1. < 5K'
            when
                net_arr_usd_crm >= 5000 and net_arr_usd_crm < 10000
                then '2. 5K-10K'
            when
                net_arr_usd_crm >= 10000 and net_arr_usd_crm < 25000
                then '3. 10K-25K'
            when
                net_arr_usd_crm >= 25000 and net_arr_usd_crm < 50000
                then '4. 25K-50K'
            when
                net_arr_usd_crm >= 50000 and net_arr_usd_crm < 100000
                then '5. 50K-100K'
            when
                net_arr_usd_crm >= 100000 
                then '6. >100K'
            else '7. other'
        end as crm_arr_range,
        case
            when net_arr_usd_instance is null
                then '0. NULL'
            when net_arr_usd_instance < 5000 
                then '1. < 5K'
            when
                net_arr_usd_instance >= 5000 and net_arr_usd_instance < 10000
                then '2. 5K-10K'
            when
                net_arr_usd_instance >= 10000 and net_arr_usd_instance < 25000
                then '3. 10K-25K'
            when
                net_arr_usd_instance >= 25000 and net_arr_usd_instance < 50000
                then '4. 25K-50K'
            when
                net_arr_usd_instance >= 50000 and net_arr_usd_instance < 100000
                then '5. 50K-100K'
            when
                net_arr_usd_instance >= 100000 
                then '6. >100K'
            else '7. other'
        end as instance_arr_range
    from functional.growth_engine.dim_growth_engine_customer_accounts
),

--- Step 2.3 Extracting SKUs, addons & suite/support seats
import_skus_with_parsed_settings as (
    select
        account_id,
        name,
        state,
        boost,
        parse_json(plan_settings) as parsed_plan_settings,
        parsed_plan_settings:plan:value::string as plan,
        parsed_plan_settings:maxAgents:value::string as max_agents,
        row_number() over (
            partition by account_id, name, state -- Adding or removing state change results
            order by updated_at desc
        ) as rank
    from propagated_formatted.accountsdb.skus
    qualify rank = 1
),

-- Product SKUs have a plan associated
product_skus as (
    select
        account_id,
        listagg(concat(name, '_', plan), ', ') within GROUP (ORDER BY concat(name, '_', plan)) AS sku_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is not null
    group by account_id

),

-- Addon SKUs do not have a plan associated
addons_skus as (
    select
        account_id,
        listagg(name, ', ') within GROUP (ORDER BY name) AS addon_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is null
    group by account_id
),

support_seats as (
    select
        account_id,
        max(max_agents) as max_support_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%support%'
    group by account_id

),

suite_seats as (
    select
        account_id,
        max(max_agents) as max_suite_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%suite%'
    group by account_id

),

main_skus as (
    select
        product_.*,
        addon_.addon_mix,
        support_.max_support_seats,
        suite_.max_suite_seats
    from product_skus as product_
        left join addons_skus as addon_
            on product_.account_id = addon_.account_id
        left join support_seats as support_
            on product_.account_id = support_.account_id
        left join suite_seats as suite_
            on product_.account_id = suite_.account_id
),

-- Step 2.4 Payment method

payment_method as (
    select 
        TRY_CAST(accounts.zendesk_account_id_c as INTEGER)
            as instance_account_id,
        payment_methods.type as payment_method_name
    from cleansed.zuora.zuora_accounts_bcv accounts
        left join cleansed.zuora.ZUORA_PAYMENT_METHODS_BCV payment_methods
            on accounts.default_payment_method_id = payment_methods.id
),

-- Step 3: Join all data

-- Step 3.1: Join additional variables data

joined_additional_vars as (
    select *
    from ge_table_vars ge_table_vars_
        left join cc_model cc_model_
            on ge_table_vars_.zendesk_account_id = cc_model_.instance_account_id
        left join main_skus main_skus_
            on ge_table_vars_.zendesk_account_id = main_skus_.account_id
        left join payment_method payment_method_
            on ge_table_vars_.zendesk_account_id = payment_method_.instance_account_id
),

-- Step 3.2: Join all data

main as (
    select *
    from segment_events_all segment_events_all_
    left join joined_additional_vars joined_additional_vars_
        on segment_events_all_.account_id = joined_additional_vars_.zendesk_account_id
)

select *
from main;




















----------------------------------------------------------
--- Adjust query to measure variables at the time of redemption

--create or replace table sandbox.juan_salgado.ge_dashboard_test as

-- Step 1: Segment events

-- First, counting how many events are triggered daily per account_id, offer_id and promo_code_id
-- Unique count: uses account_id and will use function count distinct in Tableau
-- Total count: counts total interactions per day and will use function sum in Tableau
-- Prompt events
with prompt_load as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_load_1_scd2
    group by all
),
prompt_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2
    group by all
),
prompt_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_dismiss_offer_1_scd2
    group by all
),
work_modal_1_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_claim_offer_click_2_scd2
    group by all
),
work_modal_1_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_dismiss_offer_1_scd2
    group by all
),
work_modal_2_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
    group by all
),
work_modal_2_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_dismiss_offer_1_scd2
    group by all
),
work_modal_2_go_back as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_go_back_scd2
    group by all
),
follow_up_close as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_close_scd2
    group by all
),
follow_up_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_dismiss_scd2
    group by all
),
follow_up_subscription as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_subscription_details_scd2
    group by all
),
segment_events_all_tmp as (
    select
        prompt_load_.date as loaded_date,
        prompt_load_.account_id,
        prompt_load_.offer_id,
        prompt_load_.promo_code_id,
        prompt_load_.total_count as total_count_prompt_loads,
        prompt_load_.unique_count as unique_count_prompt_loads,
        prompt_click_.total_count as total_count_prompt_clicks,
        prompt_click_.unique_count as unique_count_prompt_clicks,
        prompt_dismiss_.total_count as total_count_prompt_dismiss,
        prompt_dismiss_.unique_count as unique_count_prompt_dismiss,
        work_modal_1_click_.total_count as total_count_work_modal_1_click,
        work_modal_1_click_.unique_count as unique_count_work_modal_1_click,
        work_modal_1_dismiss_.total_count as total_count_work_modal_1_dismiss,
        work_modal_1_dismiss_.unique_count as unique_count_work_modal_1_dismiss,
        work_modal_2_click_.total_count as total_count_work_modal_2_click,
        work_modal_2_click_.unique_count as unique_count_work_modal_2_click,
        work_modal_2_dismiss_.total_count as total_count_work_modal_2_dismiss,
        work_modal_2_dismiss_.unique_count as unique_count_work_modal_2_dismiss,
        work_modal_2_go_back_.total_count as total_count_work_modal_2_go_back,
        work_modal_2_go_back_.unique_count as unique_count_work_modal_2_go_back,
        follow_up_close_.total_count as total_count_follow_up_close,
        follow_up_close_.unique_count as unique_count_follow_up_close,
        follow_up_dismiss_.total_count as total_count_follow_up_dismiss,
        follow_up_dismiss_.unique_count as unique_count_follow_up_dismiss,
        follow_up_subscription_.total_count
            as total_count_follow_up_subscription,
        follow_up_subscription_.unique_count
            as unique_count_follow_up_subscription
    from prompt_load as prompt_load_
        left join prompt_click as prompt_click_
            on
                prompt_load_.account_id = prompt_click_.account_id
                and prompt_load_.date = prompt_click_.date
                and prompt_load_.offer_id = prompt_click_.offer_id
                and prompt_load_.promo_code_id = prompt_click_.promo_code_id
        left join prompt_dismiss as prompt_dismiss_
            on
                prompt_load_.account_id = prompt_dismiss_.account_id
                and prompt_load_.date = prompt_dismiss_.date
                and prompt_load_.offer_id = prompt_dismiss_.offer_id
                and prompt_load_.promo_code_id = prompt_dismiss_.promo_code_id
        left join work_modal_1_click as work_modal_1_click_
            on
                prompt_load_.account_id = work_modal_1_click_.account_id
                and prompt_load_.date = work_modal_1_click_.date
                and prompt_load_.offer_id = work_modal_1_click_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_1_click_.promo_code_id
        left join work_modal_1_dismiss as work_modal_1_dismiss_
            on
                prompt_load_.account_id = work_modal_1_dismiss_.account_id
                and prompt_load_.date = work_modal_1_dismiss_.date
                and prompt_load_.offer_id = work_modal_1_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_1_dismiss_.promo_code_id
        left join work_modal_2_click as work_modal_2_click_
            on
                prompt_load_.account_id = work_modal_2_click_.account_id
                and prompt_load_.date = work_modal_2_click_.date
                and prompt_load_.offer_id = work_modal_2_click_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_click_.promo_code_id
        left join work_modal_2_dismiss as work_modal_2_dismiss_
            on
                prompt_load_.account_id = work_modal_2_dismiss_.account_id
                and prompt_load_.date = work_modal_2_dismiss_.date
                and prompt_load_.offer_id = work_modal_2_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_dismiss_.promo_code_id
        left join work_modal_2_go_back as work_modal_2_go_back_
            on
                prompt_load_.account_id = work_modal_2_go_back_.account_id
                and prompt_load_.date = work_modal_2_go_back_.date
                and prompt_load_.offer_id = work_modal_2_go_back_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_go_back_.promo_code_id
        left join follow_up_close as follow_up_close_
            on
                prompt_load_.account_id = follow_up_close_.account_id
                and prompt_load_.date = follow_up_close_.date
                and prompt_load_.offer_id = follow_up_close_.offer_id
                and prompt_load_.promo_code_id = follow_up_close_.promo_code_id
        left join follow_up_dismiss as follow_up_dismiss_
            on
                prompt_load_.account_id = follow_up_dismiss_.account_id
                and prompt_load_.date = follow_up_dismiss_.date
                and prompt_load_.offer_id = follow_up_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = follow_up_dismiss_.promo_code_id
        left join follow_up_subscription as follow_up_subscription_
            on
                prompt_load_.account_id = follow_up_subscription_.account_id
                and prompt_load_.date = follow_up_subscription_.date
                and prompt_load_.offer_id = follow_up_subscription_.offer_id
                and prompt_load_.promo_code_id
                = follow_up_subscription_.promo_code_id
),
first_loaded as (
    select
        account_id,
        min(loaded_date) as first_loaded_date
    from segment_events_all_tmp
    group by 1
),
segment_events_all as (
    select
        a.*,
        b.first_loaded_date,
        datediff('day', b.first_loaded_date, a.loaded_date)
            as days_since_first_loaded,
        date(a.loaded_date) - 1 as date_to_join
    from segment_events_all_tmp as a
        left join first_loaded as b
            on a.account_id = b.account_id
),

-- Step 2: Join additional variables from the previous day of event load

---- State, category and account age
segment_events_all_states as (
    select
        a.*,
        b.instance_account_state as account_state,
        b.instance_account_derived_type as account_category,
        datediff(
            day, b.instance_account_created_timestamp::date, a.date_to_join
        ) as account_age_in_days
    from segment_events_all as a
        left join
            foundational.customer.dim_instance_accounts_daily_snapshot as b
            on
                a.account_id = b.instance_account_id
                and a.date_to_join = b.source_snapshot_date
),

---- Renewal date and subscription age
import_finance_subscriptions as (
    select
        distinct 
        finance.service_date,
        snapshot.instance_account_id as zendesk_account_id,
        finance.subscription_status,
        finance.subscription_term_start_date as subscription_start_date,
        finance.subscription_term_end_date as subscription_renewal_date,
        datediff(day, finance.subscription_term_start_date, finance.service_date)
            as subscription_age_in_days,
        datediff(day, finance.service_date, finance.subscription_term_end_date)
            as days_to_subscription_renewal,
        row_number() over (
            partition by finance.service_date, zendesk_account_id
            order by finance.subscription_term_end_date asc
        ) as rank
    from
        foundational.finance.fact_recurring_revenue_daily_snapshot_enriched
            as finance
        inner join
            foundational.customer.entity_mapping_daily_snapshot as snapshot
            on
                finance.billing_account_id = snapshot.billing_account_id
                and finance.service_date = snapshot.source_snapshot_date
    where finance.service_date >= '2025-01-01'
        --finance.subscription_status = 'Active'
        -- Have to validate this filter
        -- Account 10214376 had subscription_status = 'Expired' before April 18, the day it redeemed the offer
        and (
            finance.subscription_kind = 'Primary'
            or finance.subscription_kind is null
        )
        and finance.service_date >= '2025-01-01'
    qualify rank = 1
),

segment_events_all_renewal_dates as (
    select
        a.*,
        b.subscription_start_date,
        b.subscription_renewal_date,
        b.subscription_age_in_days,
        b.days_to_subscription_renewal,
        datediff(day, b.subscription_start_date, b.subscription_renewal_date)
            as subscription_term_length,
        case
            when
                subscription_term_length >= 28
                and subscription_term_length <= 32
                then 'monthly'
            when
                subscription_term_length >= 360
                and subscription_term_length <= 370
                then 'annual'
            else 'other'
        end as subscription_term_type
    from segment_events_all_states as a
        left join
            import_finance_subscriptions as b
            on
                a.account_id = b.zendesk_account_id
                and a.date_to_join = b.service_date
),

---- Region, market segment and sales model
snapshot as (
    select distinct
        source_snapshot_date,
        crm_account_id,
        instance_account_id
    from foundational.customer.entity_mapping_daily_snapshot
    where source_snapshot_date >= '2025-01-01'
),

segment_events_all_region_mkt_segment as (
    select
        a.*,
        snapshot.crm_account_id,
        b.crm_region as region,
        b.pro_forma_market_segment as market_segment,
        b.crm_sales_model as sales_model,
        b.crm_employee_range as employee_count_range
    from segment_events_all_renewal_dates as a
        left join snapshot
            on
                a.account_id = snapshot.instance_account_id
                and a.date_to_join = snapshot.source_snapshot_date
        left join
            foundational.customer.dim_crm_accounts_daily_snapshot as b
            on
                snapshot.crm_account_id = b.crm_account_id
                and a.date_to_join = b.source_snapshot_date
),


---- Instance & CRM ARR
import_finance_recurring_revenue_instance_arr as (
    select
        finance.service_date,
        snapshot.instance_account_id as zendesk_account_id,
        sum(finance.net_arr_usd) as net_arr_usd
    from foundational.finance.fact_recurring_revenue_daily_snapshot_enriched as finance
    left join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on finance.billing_account_id = snapshot.billing_account_id
        and finance.service_date = snapshot.source_snapshot_date
    where finance.service_date >= '2025-01-01'
    group by all
),

import_finance_recurring_revenue_crm_arr as (
    select
        service_date,
        crm_account_id,
        sum(net_arr_usd) as net_arr_usd
    from foundational.finance.FACT_RECURRING_REVENUE_DAILY_SNAPSHOT_ENRICHED
    where service_date >= '2025-01-01'
    group by crm_account_id, service_date
),

segment_events_all_arr as (
    select
        a.*,
        instance_arr.net_arr_usd as net_arr_usd_instance,
        crm_arr.net_arr_usd as net_arr_usd_crm,
        case
            when net_arr_usd_crm is null
                then '0. NULL'
            when net_arr_usd_crm < 5000 
                then '1. < 5K'
            when
                net_arr_usd_crm >= 5000 and net_arr_usd_crm < 10000
                then '2. 5K-10K'
            when
                net_arr_usd_crm >= 10000 and net_arr_usd_crm < 25000
                then '3. 10K-25K'
            when
                net_arr_usd_crm >= 25000 and net_arr_usd_crm < 50000
                then '4. 25K-50K'
            when
                net_arr_usd_crm >= 50000 and net_arr_usd_crm < 100000
                then '5. 50K-100K'
            when
                net_arr_usd_crm >= 100000 
                then '6. >100K'
            else '7. other'
        end as crm_arr_range,
        case
            when net_arr_usd_instance is null
                then '0. NULL'
            when net_arr_usd_instance < 5000 
                then '1. < 5K'
            when
                net_arr_usd_instance >= 5000 and net_arr_usd_instance < 10000
                then '2. 5K-10K'
            when
                net_arr_usd_instance >= 10000 and net_arr_usd_instance < 25000
                then '3. 10K-25K'
            when
                net_arr_usd_instance >= 25000 and net_arr_usd_instance < 50000
                then '4. 25K-50K'
            when
                net_arr_usd_instance >= 50000 and net_arr_usd_instance < 100000
                then '5. 50K-100K'
            when
                net_arr_usd_instance >= 100000 
                then '6. >100K'
            else '7. other'
        end as instance_arr_range
    from segment_events_all_region_mkt_segment as a
        left join import_finance_recurring_revenue_instance_arr instance_arr
            on
                a.account_id = instance_arr.zendesk_account_id
                and a.date_to_join = instance_arr.service_date
        left join import_finance_recurring_revenue_crm_arr crm_arr
            on
                a.crm_account_id = crm_arr.crm_account_id
                and a.date_to_join = crm_arr.service_date
),

-- Step 2.2 Churn driver
-- Has to be adjusted to use the closest date to the segment event loaded date.
-- This is an aproximation to the real date where C/C data was pulled

/*
Cannot use C/C values at redemption since the table does not have this info.
Ask the ML team about it
segment_events_all_cc as (
    select
        a.*,
        b.driver_1 as churn_driver,
        b.tool_tip_1 as churn_tooltip,
        b.source_snapshot_date
    from segment_events_all as a
        left join
            functional.growth_engine.dim_growth_engine_churn_predictions as b
            on
                a.account_id = b.instance_account_id
                and a.date_to_join - 1 = b.source_snapshot_date
)


select instance_account_id, count(*) tot_obs
from FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
group by 1
order by 2 desc

cc_model as (
    select 
        instance_account_id,
        driver_1 as churn_driver,
        tool_tip_1 as churn_tooltip,
        row_number() over (
            partition by instance_account_id
            order by source_snapshot_date desc
        ) as rank
    from dev_functional.growth_engine.dim_growth_engine_churn_predictions
    qualify rank = 1
),

 */

segment_events_all_cc as (
    select
        a.*,
        b.driver_1 as churn_driver,
        b.tool_tip_1 as churn_tooltip,
        b.predicted_probability
    from segment_events_all_arr as a
        left join
            functional.eda_ml_outcome.churn_score_predictions as b
            on
                a.account_id = b.instance_account_id
                and a.date_to_join = b.source_snapshot_date
),

--- Step 2.3 Extracting SKUs, addons & suite/support seats
import_skus_with_parsed_settings as (
    select
        account_id,
        name,
        state,
        boost,
        parse_json(plan_settings) as parsed_plan_settings,
        parsed_plan_settings:plan:value::string as plan,
        parsed_plan_settings:maxAgents:value::string as max_agents,
        row_number() over (
            partition by account_id, name, state -- Adding or removing state change results
            order by updated_at desc
        ) as rank
    from propagated_formatted.accountsdb.skus
    qualify rank = 1
),

-- Product SKUs have a plan associated
product_skus as (
    select
        account_id,
        listagg(concat(name, '_', plan), ', ') within GROUP (ORDER BY concat(name, '_', plan)) AS sku_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is not null
    group by account_id

),

-- Addon SKUs do not have a plan associated
addons_skus as (
    select
        account_id,
        listagg(name, ', ') within GROUP (ORDER BY name) AS addon_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is null
    group by account_id
),

support_seats as (
    select
        account_id,
        max(max_agents) as max_support_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%support%'
    group by account_id

),

suite_seats as (
    select
        account_id,
        max(max_agents) as max_suite_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%suite%'
    group by account_id

),

main_skus as (
    select
        product_.*,
        addon_.addon_mix,
        support_.max_support_seats,
        suite_.max_suite_seats
    from product_skus as product_
        left join addons_skus as addon_
            on product_.account_id = addon_.account_id
        left join support_seats as support_
            on product_.account_id = support_.account_id
        left join suite_seats as suite_
            on product_.account_id = suite_.account_id
),

-- Step 2.4 Payment method

payment_method as (
    select 
        TRY_CAST(accounts.zendesk_account_id_c as INTEGER)
            as instance_account_id,
        payment_methods.type as payment_method_name
    from cleansed.zuora.zuora_accounts_bcv accounts
        left join cleansed.zuora.ZUORA_PAYMENT_METHODS_BCV payment_methods
            on accounts.default_payment_method_id = payment_methods.id
),

-- Step 2.5 Zuora data

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
        inner join
            (select distinct account_id from segment_events_all_arr) accounts
            on mapping.instance_account_id = accounts.account_id
    where
        zuora.is_last_segment = true
        and subscription.status in ('Active', 'Expired')
        and zuora.created_date >= '2025-01-01'
        and LOWER(coupon_id) like '%save%'
    group by
        all
),


-- Step 3: Join all data

-- Step 3.1: Join additional variables data

main as (
    select 
        segment_events_all_.*,
        -- C/C model
        --cc_model_.churn_driver,
        --cc_model_.churn_tooltip,
        --cc_model_.predicted_probability,
        -- SKU data
        main_skus_.sku_mix,
        main_skus_.addon_mix,
        main_skus_.max_support_seats,
        main_skus_.max_suite_seats,
        -- Payment method
        payment_method_.payment_method_name,
        -- Zuora data
        redeemed_zuora_.up_to_periods zuora_up_to_periods,
        redeemed_zuora_.billing_period zuora_billing_period,
        redeemed_zuora_.charge_model zuora_charge_model,
        redeemed_zuora_.currency zuora_currency,
        redeemed_zuora_.discount_amount zuora_discount_amount,
        redeemed_zuora_.discount_percentage zuora_discount_percentage,
        redeemed_zuora_.total_records as zuora_total_records,
        redeemed_zuora_.direct_charges as zuora_effective_charges,
        redeemed_zuora_.coupon_redeemed as zuora_coupon_redeemed,
        case 
            when 
                redeemed_zuora_.coupon_redeemed = 1 
                then segment_events_all_.account_id 
                else null 
        end as zuora_unique_coupon_redeemed,
        ---- Adjust for fixed amount coupons. Currently all are percentage
        case 
            when 
                zuora_billing_period = 'Annual' 
                then zuora_up_to_periods * (zuora_discount_percentage / 100) * segment_events_all_.net_arr_usd_instance
            when
                zuora_billing_period = 'Month'
                then (zuora_up_to_periods) * (zuora_discount_percentage / 100) * (segment_events_all_.net_arr_usd_instance / 12)
            else null
        end as zuora_total_discount_amount,
        case 
            when 
                zuora_billing_period = 'Annual' 
                then zuora_effective_charges * (zuora_discount_percentage / 100) * segment_events_all_.net_arr_usd_instance
            when
                zuora_billing_period = 'Month'
                then (zuora_effective_charges) * (zuora_discount_percentage / 100) * (segment_events_all_.net_arr_usd_instance / 12)
            else null
        end as zuora_effective_discount_amount
    from segment_events_all_cc segment_events_all_
    --left join cc_model cc_model_
      --      on segment_events_all_.account_id = cc_model_.instance_account_id
        left join main_skus main_skus_
            on segment_events_all_.account_id = main_skus_.account_id
        left join payment_method payment_method_
            on segment_events_all_.account_id = payment_method_.instance_account_id
        left join redeemed_zuora redeemed_zuora_
            on segment_events_all_.account_id = redeemed_zuora_.instance_account_id
            and lower(segment_events_all_.promo_code_id) = lower(redeemed_zuora_.coupon_id)
)


-- Query to validate the data
select
    count(*) as total_count,
    count(distinct account_id) as total_count_distinct,
    sum(total_count_prompt_loads) as total_count_prompt_loads,
    count(distinct unique_count_prompt_loads) as unique_count_prompt_loads,
    sum(total_count_work_modal_2_click) as total_count_work_modal_2_click,
    count(distinct unique_count_work_modal_2_click)
        as unique_count_work_modal_2_click,
    sum(zuora_coupon_redeemed) as total_count_zuora_coupon_redeemed,
    count(distinct zuora_unique_coupon_redeemed)
        as unique_count_zuora_coupon_redeemed,
    sum(case when predicted_probability is null then 1 else 0 end) as null_count_predicted_probability,
    count(distinct case when predicted_probability is null then account_id else null end) as unique_null_count_predicted_probability,
from main;


select *
from main;








----- Adjust SKUs query

with import_skus_with_parsed_settings as (
    select
        account_id,
        name,
        state,
        boost,
        parse_json(plan_settings) as parsed_plan_settings,
        parsed_plan_settings:plan:value::string as plan,
        parsed_plan_settings:maxAgents:value::string as max_agents,
        row_number() over (
            partition by account_id, name, state -- Adding or removing state change results
            order by updated_at desc
        ) as rank
    from propagated_formatted.accountsdb.skus
    qualify rank = 1
),

-- Product SKUs have a plan associated
product_skus as (
    select
        account_id,
        listagg(concat(name, '_', plan), ', ') within GROUP (ORDER BY concat(name, '_', plan)) AS sku_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is not null
    group by account_id

),

-- Addon SKUs do not have a plan associated
addons_skus as (
    select
        account_id,
        listagg(name, ', ') within GROUP (ORDER BY name) AS addon_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is null
    group by account_id
),

support_seats as (
    select
        account_id,
        max(max_agents) as max_support_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%support%'
    group by account_id

),

suite_seats as (
    select
        account_id,
        max(max_agents) as max_suite_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%suite%'
    group by account_id

),

main_skus as (
    select
        product_.*,
        addon_.addon_mix,
        support_.max_support_seats,
        suite_.max_suite_seats
    from product_skus as product_
        left join addons_skus as addon_
            on product_.account_id = addon_.account_id
        left join support_seats as support_
            on product_.account_id = support_.account_id
        left join suite_seats as suite_
            on product_.account_id = suite_.account_id
),

joined as (select distinct
    a.account_id,
    a.sku_mix,
    a.addon_mix,
    a.max_support_seats,
    a.max_suite_seats,
    b.sku_mix sku_mix_b,
    b.addon_mix addon_mix_b,
    b.max_support_seats max_support_seats_b,
    b.max_suite_seats max_suite_seats_b,
    case 
        when 
            b.sku_mix like '%support%' 
            and b.sku_mix not like '%suite%' 
            then 'support'
        when 
            b.sku_mix like '%suite%' 
            then 'suite'
        else 'other'
    end as sku_type,
    case 
        when sku_type = 'support' then max_support_seats 
        when sku_type = 'suite' then max_suite_seats 
        else null
    end as seats_capacity
from sandbox.juan_salgado.ge_dashboard_test a
left join main_skus b
    on a.account_id = b.account_id)

select sku_type, count(*)
from joined
group by 1




select distinct
a.account_id,
a.sku_mix,
a.addon_mix,
a.max_support_seats,
a.max_suite_seats
from sandbox.juan_salgado.ge_dashboard_test a
where a.max_support_seats = 1
    



select distinct
a.account_id,
a.sku_mix,
a.addon_mix,
a.max_support_seats,
a.max_suite_seats
from sandbox.juan_salgado.ge_dashboard_test a
where a.max_support_seats is not null
limit 10



select a.*,
b.seats_capacity,
c.seats_occupied
from functional.product_analytics.product_mix_daily_snapshot a
left join functional.product_analytics.seats_capacity_daily_snapshot b
on a.instance_account_id = b.instance_account_id
and a.source_snapshot_date = b.source_snapshot_date
left join functional.product_analytics.seats_occupied_daily_snapshot c
on a.instance_account_id = c.instance_account_id
and a.source_snapshot_date = c.source_snapshot_date
where a.instance_account_id = 11106182
and a.source_snapshot_date >= '2025-04-01'
order by a.source_snapshot_date;


with joined as(select distinct
a.account_id,
a.sku_mix,
a.addon_mix,
a.max_support_seats,
a.max_suite_seats,
b.core_base_plan,
case when lower(b.core_base_plan) like '%support%' then 1 else null end as support_seats_flag,
case when lower(b.core_base_plan) not like '%support%' then 1 else null end as suite_seats_flag,
case when lower(b.core_base_plan) like '%support%' then c.seats_capacity else null end as support_seats_capacity,
case when lower(b.core_base_plan) not like '%support%' then c.seats_capacity else null end as suite_seats_capacity,
from sandbox.juan_salgado.ge_dashboard_test a
left join functional.product_analytics.product_mix_daily_snapshot b
    on a.account_id = b.instance_account_id
    and a.first_loaded_date = b.source_snapshot_date
left join functional.product_analytics.seats_capacity_daily_snapshot c
    on a.account_id = c.instance_account_id
    and a.first_loaded_date = c.source_snapshot_date
where lower(a.account_category) != 'internal instance')


select support_seats_capacity, count(*),
from joined
where support_seats_flag = 1
group by 1
order by 1



select count(*),
count(distinct account_id), 
count(distinct case when core_base_plan is null then account_id else null end) as null_count_product_line,
sum(support_seats_flag) as support_seats_flag_count,
sum(suite_seats_flag) as suite_seats_flag_count
from joined



where lower(core_base_plan) like '%support%';






select support_seats_capacity, count(*),
from joined
where lower(core_base_plan) like '%support%'
group by 1
order by 1


select count(*),
count(distinct account_id), 
count(distinct case when core_base_plan is null then account_id else null end) as null_count_product_line
from joined
where lower(core_base_plan) like '%support%';




select 
a.account_id,
a.sku_mix,
a.addon_mix,
a.max_support_seats,
a.max_suite_seats
from sandbox.juan_salgado.ge_dashboard_test a
left join functional.product_analytics.product_mix_daily_snapshot b

where a.max_suite_seats is null 
    









----- Validate data in table


-- Query to validate the data
-- 3 customers not found in probability table
select
    count(*) as total_count,
    count(distinct account_id) as total_count_distinct,
    sum(total_count_prompt_loads) as total_count_prompt_loads,
    count(distinct unique_count_prompt_loads) as unique_count_prompt_loads,
    sum(total_count_work_modal_2_click) as total_count_work_modal_2_click,
    count(distinct unique_count_work_modal_2_click)
        as unique_count_work_modal_2_click,
    sum(zuora_coupon_redeemed) as total_count_zuora_coupon_redeemed,
    count(distinct zuora_unique_coupon_redeemed)
        as unique_count_zuora_coupon_redeemed,
    sum(case when predicted_probability is null then 1 else 0 end) as null_count_predicted_probability,
    count(distinct case when predicted_probability is null then account_id else null end) as unique_null_count_predicted_probability
from sandbox.juan_salgado.ge_dashboard_test;


select distinct account_id, first_loaded_date
from sandbox.juan_salgado.ge_dashboard_test
where predicted_probability is null
limit 10


select *
from sandbox.juan_salgado.ge_dashboard_test




SELECT 
    'SUM(CASE WHEN ' || column_name || ' IS NULL then account_id else null END) AS null_count_' || column_name
FROM sandbox.information_schema.columns
WHERE lower(table_schema) = 'juan_salgado'
  AND lower(table_name) LIKE '%ge_dashboard_test%';



SELECT table_schema, table_name, column_name
FROM sandbox.information_schema.columns
WHERE lower(table_schema) = 'juan_salgado'
  AND lower(table_name) LIKE '%ge_dashboard_test%';



select *
from sandbox.juan_salgado.ge_dashboard_test
where DAYS_TO_SUBSCRIPTION_RENEWAL is null


select *
from sandbox.juan_salgado.ge_dashboard_test
where UNIQUE_COUNT_WORK_MODAL_2_GO_BACK is not null

select *
from sandbox.juan_salgado.ge_dashboard_test
where account_id = 24905253





select count(distinct account_id) as total_count,
count(distinct CASE WHEN PROMO_CODE_ID IS NULL then account_id else null END) AS null_count_PROMO_CODE_ID,
count(distinct CASE WHEN UNIQUE_COUNT_WORK_MODAL_2_GO_BACK IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_WORK_MODAL_2_GO_BACK,
count(distinct CASE WHEN TOTAL_COUNT_FOLLOW_UP_CLOSE IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_FOLLOW_UP_CLOSE,
count(distinct CASE WHEN PAYMENT_METHOD_NAME IS NULL then account_id else null END) AS null_count_PAYMENT_METHOD_NAME,
count(distinct CASE WHEN CHURN_TOOLTIP IS NULL then account_id else null END) AS null_count_CHURN_TOOLTIP,
count(distinct CASE WHEN SUBSCRIPTION_TERM_LENGTH IS NULL then account_id else null END) AS null_count_SUBSCRIPTION_TERM_LENGTH,
count(distinct CASE WHEN ACCOUNT_ID IS NULL then account_id else null END) AS null_count_ACCOUNT_ID,
count(distinct CASE WHEN TOTAL_COUNT_WORK_MODAL_1_CLICK IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_WORK_MODAL_1_CLICK,
count(distinct CASE WHEN TOTAL_COUNT_WORK_MODAL_2_DISMISS IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_WORK_MODAL_2_DISMISS,
count(distinct CASE WHEN SUBSCRIPTION_TERM_TYPE IS NULL then account_id else null END) AS null_count_SUBSCRIPTION_TERM_TYPE,
count(distinct CASE WHEN DATE_TO_JOIN IS NULL then account_id else null END) AS null_count_DATE_TO_JOIN,
count(distinct CASE WHEN CRM_ACCOUNT_ID IS NULL then account_id else null END) AS null_count_CRM_ACCOUNT_ID,
count(distinct CASE WHEN NET_ARR_USD_INSTANCE IS NULL then account_id else null END) AS null_count_NET_ARR_USD_INSTANCE,
count(distinct CASE WHEN UNIQUE_COUNT_FOLLOW_UP_SUBSCRIPTION IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_FOLLOW_UP_SUBSCRIPTION,
count(distinct CASE WHEN UNIQUE_COUNT_WORK_MODAL_2_DISMISS IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_WORK_MODAL_2_DISMISS,
count(distinct CASE WHEN MAX_SUPPORT_SEATS IS NULL then account_id else null END) AS null_count_MAX_SUPPORT_SEATS,
count(distinct CASE WHEN SALES_MODEL IS NULL then account_id else null END) AS null_count_SALES_MODEL,
count(distinct CASE WHEN TOTAL_COUNT_PROMPT_LOADS IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_PROMPT_LOADS,
count(distinct CASE WHEN ACCOUNT_CATEGORY IS NULL then account_id else null END) AS null_count_ACCOUNT_CATEGORY,
count(distinct CASE WHEN ACCOUNT_STATE IS NULL then account_id else null END) AS null_count_ACCOUNT_STATE,
count(distinct CASE WHEN UNIQUE_COUNT_PROMPT_LOADS IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_PROMPT_LOADS,
count(distinct CASE WHEN TOTAL_COUNT_FOLLOW_UP_DISMISS IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_FOLLOW_UP_DISMISS,
count(distinct CASE WHEN CHURN_DRIVER IS NULL then account_id else null END) AS null_count_CHURN_DRIVER,
count(distinct CASE WHEN OFFER_ID IS NULL then account_id else null END) AS null_count_OFFER_ID,
count(distinct CASE WHEN TOTAL_COUNT_WORK_MODAL_2_CLICK IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_WORK_MODAL_2_CLICK,
count(distinct CASE WHEN TOTAL_COUNT_PROMPT_CLICKS IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_PROMPT_CLICKS,
count(distinct CASE WHEN SUBSCRIPTION_AGE_IN_DAYS IS NULL then account_id else null END) AS null_count_SUBSCRIPTION_AGE_IN_DAYS,
count(distinct CASE WHEN LOADED_DATE IS NULL then account_id else null END) AS null_count_LOADED_DATE,
count(distinct CASE WHEN TOTAL_COUNT_PROMPT_DISMISS IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_PROMPT_DISMISS,
count(distinct CASE WHEN UNIQUE_COUNT_PROMPT_CLICKS IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_PROMPT_CLICKS,
count(distinct CASE WHEN TOTAL_COUNT_FOLLOW_UP_SUBSCRIPTION IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_FOLLOW_UP_SUBSCRIPTION,
count(distinct CASE WHEN DAYS_TO_SUBSCRIPTION_RENEWAL IS NULL then account_id else null END) AS null_count_DAYS_TO_SUBSCRIPTION_RENEWAL,
count(distinct CASE WHEN UNIQUE_COUNT_WORK_MODAL_1_DISMISS IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_WORK_MODAL_1_DISMISS,
count(distinct CASE WHEN EMPLOYEE_COUNT_RANGE IS NULL then account_id else null END) AS null_count_EMPLOYEE_COUNT_RANGE,
count(distinct CASE WHEN SKU_MIX IS NULL then account_id else null END) AS null_count_SKU_MIX,
count(distinct CASE WHEN TOTAL_COUNT_WORK_MODAL_1_DISMISS IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_WORK_MODAL_1_DISMISS,
count(distinct CASE WHEN UNIQUE_COUNT_PROMPT_DISMISS IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_PROMPT_DISMISS,
count(distinct CASE WHEN NET_ARR_USD_CRM IS NULL then account_id else null END) AS null_count_NET_ARR_USD_CRM,
count(distinct CASE WHEN SUBSCRIPTION_RENEWAL_DATE IS NULL then account_id else null END) AS null_count_SUBSCRIPTION_RENEWAL_DATE,
count(distinct CASE WHEN UNIQUE_COUNT_FOLLOW_UP_DISMISS IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_FOLLOW_UP_DISMISS,
count(distinct CASE WHEN DAYS_SINCE_FIRST_LOADED IS NULL then account_id else null END) AS null_count_DAYS_SINCE_FIRST_LOADED,
count(distinct CASE WHEN INSTANCE_ARR_RANGE IS NULL then account_id else null END) AS null_count_INSTANCE_ARR_RANGE,
count(distinct CASE WHEN UNIQUE_COUNT_WORK_MODAL_2_CLICK IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_WORK_MODAL_2_CLICK,
count(distinct CASE WHEN ACCOUNT_AGE_IN_DAYS IS NULL then account_id else null END) AS null_count_ACCOUNT_AGE_IN_DAYS,
count(distinct CASE WHEN UNIQUE_COUNT_WORK_MODAL_1_CLICK IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_WORK_MODAL_1_CLICK,
count(distinct CASE WHEN ADDON_MIX IS NULL then account_id else null END) AS null_count_ADDON_MIX,
count(distinct CASE WHEN REGION IS NULL then account_id else null END) AS null_count_REGION,
count(distinct CASE WHEN TOTAL_COUNT_WORK_MODAL_2_GO_BACK IS NULL then account_id else null END) AS null_count_TOTAL_COUNT_WORK_MODAL_2_GO_BACK,
count(distinct CASE WHEN SUBSCRIPTION_START_DATE IS NULL then account_id else null END) AS null_count_SUBSCRIPTION_START_DATE,
count(distinct CASE WHEN CRM_ARR_RANGE IS NULL then account_id else null END) AS null_count_CRM_ARR_RANGE,
count(distinct CASE WHEN MARKET_SEGMENT IS NULL then account_id else null END) AS null_count_MARKET_SEGMENT,
count(distinct CASE WHEN MAX_SUITE_SEATS IS NULL then account_id else null END) AS null_count_MAX_SUITE_SEATS,
count(distinct CASE WHEN FIRST_LOADED_DATE IS NULL then account_id else null END) AS null_count_FIRST_LOADED_DATE,
count(distinct CASE WHEN UNIQUE_COUNT_FOLLOW_UP_CLOSE IS NULL then account_id else null END) AS null_count_UNIQUE_COUNT_FOLLOW_UP_CLOSE
from sandbox.juan_salgado.ge_dashboard_test














with import_finance_subscriptions as (
    select
        snapshot_bcv.instance_account_id as zendesk_account_id,
        finance.billing_account_id,
        finance.subscription_term_start_date,
        finance.subscription_term_end_date,
        finance.subscription_status,
        finance.subscription_kind,
        row_number() over (
            partition by finance.billing_account_id
            order by finance.subscription_term_end_date asc
        ) as rank
    from foundational.finance.fact_recurring_revenue_bcv_enriched as finance
    inner join foundational.customer.entity_mapping_daily_snapshot_bcv as snapshot_bcv
        on finance.billing_account_id = snapshot_bcv.billing_account_id
    where
        finance.subscription_status = 'Active'
        and (
            finance.subscription_kind = 'Primary'
            or finance.subscription_kind is null
        )
    qualify rank = 1
)

select *
from import_finance_subscriptions
where zendesk_account_id = 10214376





24853211
24905253


