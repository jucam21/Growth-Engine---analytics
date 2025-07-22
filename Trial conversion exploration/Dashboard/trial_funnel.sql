--- Query to summarize usage of trial recommendation modals

--- Step 1: segment events


--- Listing events here
--- Full list here:
-- https://zendesk.atlassian.net/wiki/spaces/GM/pages/6802898946/PRD+Growth+engine+-+Trial+conversion#Instrumentation-Plan


CLEANSED.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1_AGENT_DECREASE_BCV
CLEANSED.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1_AGENT_INCREASE_BCV
CLEANSED.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1_BCV
CLEANSED.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1_BILLING_CYCLE_CHANGE_BCV
CLEANSED.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1_BUY_NOW_BCV
CLEANSED.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1_DISMISS_OFFER_BCV
CLEANSED.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1_MODAL_LOAD_BCV
CLEANSED.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1_SEE_ALL_PLANS_BCV



cleansed.segment_support.growth_engine_trial_cta_1_agent_decrease_scd2
cleansed.segment_support.growth_engine_trial_cta_1_agent_increase_scd2
cleansed.segment_support.growth_engine_trial_cta_1_scd2
cleansed.segment_support.growth_engine_trial_cta_1_billing_cycle_change_scd2
cleansed.segment_support.growth_engine_trial_cta_1_buy_now_bcscd2
cleansed.segment_support.growth_engine_trial_cta_1_dismiss_offer_scd2
cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2





select 
    min(original_timestamp) as first_event_time,
    count(*) as count,
    count(distinct account_id) as unique_users
from cleansed.segment_support.growth_engine_trial_cta_1_agent_decrease_bcv

FIRST_EVENT_TIME	COUNT	UNIQUE_USERS
2025-07-08 15:40:06.227	34	11

select 
    min(original_timestamp) as first_event_time,
    count(*) as count,
    count(distinct account_id) as unique_users
from cleansed.segment_support.growth_engine_trial_cta_1_agent_increase_bcv

FIRST_EVENT_TIME	COUNT	UNIQUE_USERS
2025-07-08 15:40:04.505	97	19



select 
    min(original_timestamp) as first_event_time,
    count(*) as count,
    count(distinct account_id) as unique_users
from cleansed.segment_support.growth_engine_trial_cta_1_bcv

FIRST_EVENT_TIME	COUNT	UNIQUE_USERS
2025-07-08 15:39:16.234	226	153



select 
    min(original_timestamp) as first_event_time,
    count(*) as count,
    count(distinct account_id) as unique_users
from cleansed.segment_support.growth_engine_trial_cta_1_billing_cycle_change_bcv

FIRST_EVENT_TIME	COUNT	UNIQUE_USERS
2025-07-08 15:40:09.311	92	38



select 
    min(original_timestamp) as first_event_time,
    count(*) as count,
    count(distinct account_id) as unique_users
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_bcv


FIRST_EVENT_TIME	COUNT	UNIQUE_USERS
2025-07-08 15:40:23.524	63	28




select 
    min(original_timestamp) as first_event_time,
    count(*) as count,
    count(distinct account_id) as unique_users
from cleansed.segment_support.growth_engine_trial_cta_1_dismiss_offer_bcv

FIRST_EVENT_TIME	COUNT	UNIQUE_USERS
2025-07-08 15:43:33.780	160	112



select 
    min(original_timestamp) as first_event_time,
    count(*) as count,
    count(distinct account_id) as unique_users
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_bcv

FIRST_EVENT_TIME	COUNT	UNIQUE_USERS
2025-07-08 15:39:16.944	700	166


select 
    min(original_timestamp) as first_event_time,
    count(*) as count,
    count(distinct account_id) as unique_users
from cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_bcv

FIRST_EVENT_TIME	COUNT	UNIQUE_USERS
2025-07-08 15:40:16.244	91	64





---------------------------------------------
--- Instrumenting the funnel



cleansed.segment_support.growth_engine_trial_cta_1_agent_decrease_scd2
cleansed.segment_support.growth_engine_trial_cta_1_agent_increase_scd2
cleansed.segment_support.growth_engine_trial_cta_1_scd2
cleansed.segment_support.growth_engine_trial_cta_1_billing_cycle_change_scd2
cleansed.segment_support.growth_engine_trial_cta_1_buy_now_bcscd2
cleansed.segment_support.growth_engine_trial_cta_1_dismiss_offer_scd2
cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2





with prompt_load as (
    select
        account_id,
        trial_type,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_scd2
    group by all
),

modal_load as (
    select
        account_id,
        offer_id,
        plan_name,
        preview_state,
        source,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
    group by all
),

modal_dismiss as (
    select
        account_id,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_dismiss_offer_scd2
    group by all
),

modal_buy_now as (
    select
        account_id,
        agent_count,
        billing_cycle,
        offer_id,
        plan,
        plan_name,
        product,
        promo_code,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
    group by all
),

modal_agent_increase as (
    select
        account_id,
        agent_count,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_agent_increase_scd2
    group by all
),

modal_agent_decrease as (
    select
        account_id,
        agent_count,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_agent_decrease_scd2
    group by all
),

modal_billing_cycle as (
    select
        account_id,
        billing_cycle,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_billing_cycle_change_scd2
    group by all
),

modal_see_all_plans as (
    select
        account_id,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
    group by all
)

select 
    date,
    sum(total_count) as total_count_prompt_load,
    count(distinct unique_count) as unique_count_prompt_load
from modal_load
group by date
order by date;


select 
    date,
    sum(total_count) as total_count_prompt_load,
    count(distinct unique_count) as unique_count_prompt_load
from prompt_load
group by date
order by date;



select distinct SOURCE
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2







with prompt_load as (
    select
        account_id,
        trial_type,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_scd2
    group by all
),

modal_load as (
    select
        account_id,
        offer_id,
        plan_name,
        preview_state,
        source,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
    group by all
),

modal_buy as (
    select
        account_id,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
    group by all
),

modal_see_all as (
    select
        account_id,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
    group by all
),

segment_events_all_tmp as (
    select
        modal_load_.date as loaded_date,
        modal_load_.account_id,
        modal_load_.offer_id,
        modal_load_.total_count as total_count_modal_loads,
        modal_load_.unique_count as unique_count_modal_loads,
        modal_buy_.total_count as total_count_modal_buy_now,
        modal_buy_.unique_count as unique_count_modal_buy_now,
        modal_see_all_.total_count as total_count_see_all_plans,
        modal_see_all_.unique_count as unique_count_see_all_plans,
    from modal_load as modal_load_
        left join modal_buy as modal_buy_
            on
                modal_load_.account_id = modal_buy_.account_id
                and modal_load_.date = modal_buy_.date
                and modal_load_.offer_id = modal_buy_.offer_id
                and modal_load_.plan_name = modal_buy_.plan_name
        left join modal_see_all as modal_see_all_
            on
                modal_load_.account_id = modal_see_all_.account_id
                and modal_load_.date = modal_see_all_.date
                and modal_load_.offer_id = modal_see_all_.offer_id
                and modal_load_.plan_name = modal_see_all_.plan_name
)

select
    loaded_date,
    sum(total_count_modal_loads) as total_count_modal_loads,
    count(distinct unique_count_modal_loads) as unique_count_modal_loads,
    sum(total_count_modal_buy_now) as total_count_modal_buy_now,
    count(distinct unique_count_modal_buy_now) as unique_count_modal_buy_now,
    sum(total_count_see_all_plans) as total_count_see_all_plans,
    count(distinct unique_count_see_all_plans) as unique_count_see_all_plans
from segment_events_all_tmp
group by loaded_date
order by loaded_date 




select date_trunc('day', original_timestamp) as date,
count(*) tot_obs,
count(distinct account_id) tot_accounts,
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
group by 1
order by 1



--------------------
--- C/C sub 5K

select 
    service_date,
    count(*),
    count(distinct crm_account_id) as unique_accounts
from FUNCTIONAL.FINANCE.SFA_QTD_CRM_PRODUCT_FINANCE_ADJ_CURRENT
where 
    NET_ARR_USD_PRIOR_QUARTER_END <= 5000
    and service_date in ('2025-01-20', '2025-04-20', '2025-07-20')
group by 1
order by 1





with cc_numbers as (
    select 
        service_date,
        crm_account_id,
        CRM_SALES_MODEL,
        sum(NET_ARR_USD_PRIOR_QUARTER_END) as net_arr,
        sum(CHURN_CONTRACTION_ARR_USD) cc_arr,
        case 
            when sum(NET_ARR_USD_PRIOR_QUARTER_END) <= 5000 then '1. sub_5k'
            when sum(NET_ARR_USD_PRIOR_QUARTER_END) <= 12000 then '2. 5k - 12k'
            when sum(NET_ARR_USD_PRIOR_QUARTER_END) <= 25000 then '3. 12k - 25k'
            when sum(NET_ARR_USD_PRIOR_QUARTER_END) <= 50000 then '4. 25k - 50k'
            when sum(NET_ARR_USD_PRIOR_QUARTER_END) <= 100000 then '5. 50k - 100k'
            when sum(NET_ARR_USD_PRIOR_QUARTER_END) > 100000 then '6. above_100k'
            else 'unknown'
        end as arr_band,
        case when cc_arr < 0 then crm_account_id else null end as churned_account_id
    from FUNCTIONAL.FINANCE.SFA_QTD_CRM_PRODUCT_FINANCE_ADJ_CURRENT
    where 
        service_date in ('2025-01-20', '2025-04-20', '2025-07-20')
    group by all 
)

select 
    service_date,
    arr_band,
    CRM_SALES_MODEL,
    count(*),
    count(distinct crm_account_id) as unique_accounts,
    count(distinct churned_account_id) as unique_churned_accounts,
    sum(net_arr) as total_net_arr,
    sum(cc_arr) as total_cc_arr
from cc_numbers
group by all
order by 1, 2, 3


