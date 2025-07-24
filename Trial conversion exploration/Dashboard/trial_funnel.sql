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





----------------------------------------------------
--- Creating funnel and measuring if duplicates
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
),

--- Counting duplicates

prompt_load_duplicates as (
    select 
        date,
        account_id,
        'prompt_load' as source,
        count(*) as tot_obs
    from prompt_load
    group by all
    having count(*) > 1
),

modal_load_duplicates as (
    select 
        date,
        account_id,
        'modal_load' as source,
        count(*) as tot_obs
    from modal_load
    group by all
    having count(*) > 1
),

modal_dismiss_duplicates as (
    select 
        date,
        account_id,
        'modal_dismiss' as source,
        count(*) as tot_obs
    from modal_dismiss
    group by all
    having count(*) > 1
),

modal_buy_now_duplicates as (
    select 
        date,
        account_id,
        'modal_buy_now' as source,
        count(*) as tot_obs
    from modal_buy_now
    group by all
    having count(*) > 1
),

modal_agent_increase_duplicates as (
    select 
        date,
        account_id,
        'modal_agent_increase' as source,
        count(*) as tot_obs
    from modal_agent_increase
    group by all
    having count(*) > 1
),

modal_agent_decrease_duplicates as (
    select 
        date,
        account_id,
        'modal_agent_decrease' as source,
        count(*) as tot_obs
    from modal_agent_decrease
    group by all
    having count(*) > 1
),

modal_billing_cycle_duplicates as (
    select 
        date,
        account_id,
        'modal_billing_cycle' as source,
        count(*) as tot_obs
    from modal_billing_cycle
    group by all
    having count(*) > 1
),

modal_see_all_plans_duplicates as (
    select 
        date,
        account_id,
        'modal_see_all_plans' as source,
        count(*) as tot_obs
    from modal_see_all_plans
    group by all
    having count(*) > 1
),

--- Joining all duplicates tables

dups_join as (
    select *
    from prompt_load_duplicates
    union all
    select *
    from modal_load_duplicates
    union all
    select *
    from modal_dismiss_duplicates
    union all
    select *
    from modal_buy_now_duplicates
    union all
    select *
    from modal_agent_increase_duplicates
    union all
    select *
    from modal_agent_decrease_duplicates
    union all
    select *
    from modal_billing_cycle_duplicates
    union all
    select *
    from modal_see_all_plans_duplicates
)

--- Exploring duplicates

select *
from modal_buy_now
where 
    account_id = 25485348
    and date = '2025-07-08'::date




select *
from dups_join


--- Counting total duplicates

select 
    source,
    sum(tot_obs) as total_duplicates
from dups_join
group by source
order by total_duplicates desc;








----------------------------------------------------
--- Funnel query

--- Step 1: count interactions with each modal step
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
),

--- Step 2: Join relevant events
--- Decided to not use agent increase/decrease & billing cycle change events,
--- since they have duplicates and require additional logic

segment_events_all_tmp as (
    select
        modal_load.date as loaded_date,
        modal_load.account_id,
        modal_load.offer_id,
        modal_load.plan_name,
        modal_load.preview_state,
        modal_load.source,
        --- Fields relevant to buy now modal
        modal_buy_now.agent_count as buy_now_agent_count,
        modal_buy_now.billing_cycle as buy_now_billing_cycle,
        modal_buy_now.offer_id as buy_now_offer_id,
        modal_buy_now.plan as buy_now_plan,
        modal_buy_now.plan_name as buy_now_plan_name,
        modal_buy_now.product as buy_now_product,
        modal_buy_now.promo_code as buy_now_promo_code,
        --- Counts per each modal
        prompt_load.total_count as total_count_prompt_load,
        prompt_load.unique_count as unique_count_prompt_load,
        modal_load.total_count as total_count_modal_loads,
        modal_load.unique_count as unique_count_modal_loads,
        modal_dismiss.total_count as total_count_modal_dismiss,
        modal_dismiss.unique_count as unique_count_modal_dismiss,
        modal_buy_now.total_count as total_count_modal_buy_now,
        modal_buy_now.unique_count as unique_count_modal_buy_now,
        modal_see_all_plans.total_count as total_count_modal_see_all_plans,
        modal_see_all_plans.unique_count as unique_count_modal_see_all_plans
    from modal_load
    left join modal_dismiss
        on modal_load.account_id = modal_dismiss.account_id
        and modal_load.date = modal_dismiss.date
        and modal_load.offer_id = modal_dismiss.offer_id
        and modal_load.plan_name = modal_dismiss.plan_name
    left join modal_buy_now
        on modal_load.account_id = modal_buy_now.account_id
        and modal_load.date = modal_buy_now.date
        and modal_load.offer_id = modal_buy_now.offer_id
        and modal_load.plan_name = modal_buy_now.plan_name
    left join prompt_load
        on modal_load.account_id = prompt_load.account_id
        and modal_load.date = prompt_load.date
    left join modal_see_all_plans
        on modal_load.account_id = modal_see_all_plans.account_id
        and modal_load.date = modal_see_all_plans.date
        and modal_load.offer_id = modal_see_all_plans.offer_id
        and modal_load.plan_name = modal_see_all_plans.plan_name
),

--- Step 3: Join win date data

sub_term as (
    select distinct
        finance.service_date,
        snapshot.instance_account_id,
        finance.subscription_term_start_date,
        finance.subscription_term_end_date
    from foundational.finance.fact_recurring_revenue_daily_snapshot_enriched as finance
    inner join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on finance.billing_account_id = snapshot.billing_account_id
        and finance.service_date = snapshot.source_snapshot_date
    where finance.service_date >= '2025-06-01'
),

wins as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.win_date,
        trial_accounts.core_base_plan_at_win,
        datediff(day, sub_term_.subscription_term_start_date::date, sub_term_.subscription_term_end_date::date) as subscription_term_days,
        case 
            when subscription_term_days >= 0 and subscription_term_days <= 40 then 'monthly'
            when subscription_term_days >= 360 and subscription_term_days <= 370 then 'annually'
            else 'other'
        end as billing_cycle,
        trial_accounts.instance_account_arr_usd_at_win
    from presentation.growth_analytics.trial_accounts trial_accounts 
    left join sub_term sub_term_
        on trial_accounts.instance_account_id = sub_term_.instance_account_id
        and trial_accounts.win_date = sub_term_.service_date
    where 
        trial_accounts.win_date is not null
        and trial_accounts.sales_model_at_win <> 'Assisted'
        and trial_accounts.is_direct_buy = FALSE  
        and trial_accounts.win_date >= '2025-06-01'
),

segment_events_all as (
    select
        segment.*,
        wins.win_date,
        wins.instance_account_arr_usd_at_win,
        datediff(day, segment.loaded_date::date, wins.win_date::date) as days_to_win,
        case when wins.win_date is not null then 1 else null end as is_won_all,
        case when wins.win_date is not null then segment.account_id else null end as is_won_unique,
        wins.core_base_plan_at_win,
        wins.subscription_term_days,
        wins.billing_cycle
    from segment_events_all_tmp segment
    left join wins
        on segment.account_id = wins.instance_account_id
)

select *
from segment_events_all
where loaded_date = '2025-07-21'::date

--- Validate results


select 
    billing_cycle,
    count(*) as total_obs,
    count(distinct instance_account_id) as unique_accounts
from wins
group by billing_cycle

select 
    instance_account_id,
    count(*) as total_obs
from wins
group by 1
having count(*) > 1
limit 10



select 
    count(*) as total_count,
    count(distinct account_id) as unique_count,
    sum(case when offer_id is null then 1 end) as nulls_offers,
    count(distinct case when offer_id is null then account_id end) as nulls_offers_unique
from segment_events_all




select *
from segment_events_all
limit 10;


select
    date_trunc('day', original_timestamp) as date,
    count(*) as tot_obs,
    count(distinct account_id) as tot_accounts
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
group by 1
order by 1;


select
    date_trunc('day', original_timestamp) as date,
    count(distinct account_id) as tot_accounts
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
group by 1
order by 1;



select
    date_trunc('day', original_timestamp) as date,
    count(distinct account_id) as tot_accounts
from cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
group by 1
order by 1;







select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id = 25535459

select *
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
where account_id = 25535459


select *
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where account_id = 25535459


















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



with modal_buy_now as (
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
)


select 
    date,
    account_id,
    count(*) tot_obs
from modal_buy_now
group by all
having count(*) > 1
limit 10



select *
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
where account_id = 25479660
and date_trunc('day', original_timestamp) = '2025-07-18'::date


select *
from cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
where account_id = 25479660
and date_trunc('day', original_timestamp) = '2025-07-18'::date



select *
from CLEANSED.SEGMENT_BILLING.SEGMENT_BILLING_PAYMENT_LOADED_SCD2
where account_id = 25479660
and date_trunc('day', original_timestamp) = '2025-07-18'::date




select distinct source
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2




select *
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
limit 10





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





-----------------------------------------
--- Checking win rate



select *
from presentation.growth_analytics.trial_accounts
where 
    win_date is not null
    and win_date = '2025-07-20'
    and sales_model_at_win <> 'Assisted'
    and is_direct_buy = FALSE  


select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id = 25501076



select *
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
where account_id = 25501076



select *
from cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
where account_id = 25501076




select 
    offer_id,
    count(*) as total_count
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
group by offer_id
order by 2 desc




-----------------------------------------
--- Multiple plans



select 
    account_id,
    count(distinct product) as product,
    count(distinct plan_name) as plan_name,
    count(distinct plan) as plan
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
group by 1
order by 4 desc
limit 10



select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id = 25432342



select *
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
where account_id = 25432342



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


