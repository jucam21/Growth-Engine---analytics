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
--- Uncohorted funnel

--- Wins

select
    trial_accounts.win_date,
    count(*) as total_wins,
    sum(instance_account_arr_usd_at_win) as total_wins_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
    where 
        trial_accounts.win_date >= '2025-05-01'
        and trial_accounts.win_date is not null 
        and trial_accounts.sales_model_at_win <> 'Assisted'
        and trial_accounts.is_direct_buy = FALSE
group by 1
order by 1



--- Check max win date

select max(win_date) as max_win_date
from presentation.growth_analytics.trial_accounts trial_accounts 
where 
    win_date is not null 
    and sales_model_at_win <> 'Assisted'
    and is_direct_buy = FALSE











--- Da 5
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
    where 
        trial_accounts.instance_account_created_date >= '2025-06-01'
),



--- Da 8
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
    where 
        trial_accounts.win_date >= '2025-06-01'
),



wins_daily as (
    select
        win_date,
        count(*) as total_wins,
        count(distinct instance_account_id) as unique_wins,
        sum(is_won_arr) as total_wins_arr
    from accounts
    where is_won = 1
    group by 1
)

select *
from wins_daily
order by 1





select *
from accounts
where 
    win_date is not null
    and win_date = '2025-06-03'




select *
from presentation.growth_analytics.trial_accounts trial_accounts 
where instance_account_id = 25527365


CTA




-----------------------------------------------
--- Check wins vs buy now/see all plans clicks

--- Adjusted wins to measure if a user ever clicked on "buy now" or "see all plans" modals
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
),

modal_buy_now as (
    select
        account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 buy_now
    inner join accounts 
        on 
            accounts.instance_account_id = buy_now.account_id
            and date(buy_now.original_timestamp) <= accounts.win_date
    group by all
),

modal_see_all_plans as (
    select
        account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2 see_all_plans
    inner join accounts 
        on 
            accounts.instance_account_id = see_all_plans.account_id
            and date(see_all_plans.original_timestamp) <= accounts.win_date
    group by all
),

--- Join wins vs segment funnel
wins_daily as (
    select
        accounts_.win_date,
        count(*) as total_wins,
        count(distinct accounts_.instance_account_id) as unique_wins,
        sum(is_won_arr) as total_wins_arr,
        --- Wins count
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is null 
                then accounts_.instance_account_id else null 
        end) as wins_just_buy_now,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is not null 
                then accounts_.instance_account_id else null 
        end) as wins_just_see_all_plans,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is not null 
                then accounts_.instance_account_id else null 
        end) as wins_both,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is null 
                then accounts_.instance_account_id else null 
        end) as wins_none,
        --- Wins arr
        sum(case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is null 
                then accounts_.is_won_arr else null 
        end) as wins_just_buy_now_arr,
        sum(case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is not null 
                then accounts_.is_won_arr else null 
        end) as wins_just_see_all_plans_arr,
        sum(case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is not null 
                then accounts_.is_won_arr else null 
        end) as wins_both_arr,
        sum(case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is null 
                then accounts_.is_won_arr else null 
        end) as wins_none_arr,
    from accounts accounts_
    left join modal_buy_now modal_buy_now_ 
        on accounts_.instance_account_id = modal_buy_now_.account_id
    left join modal_see_all_plans modal_see_all_plans_ 
        on accounts_.instance_account_id = modal_see_all_plans_.account_id
    where accounts_.is_won = 1
    group by 1
),


--- Check admin center loads & billing cart loaded event


admin_center as (
    select
        account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_billing.segment_billing_subscription_viewed_bcv admin_center
    inner join accounts 
        on 
            accounts.instance_account_id = admin_center.account_id
            -- Admin center logins last 15 days before win date
            and date(admin_center.original_timestamp) <= accounts.win_date
            and date(admin_center.original_timestamp) >= dateadd('day', -15, accounts.win_date)
    group by all
),

billing_cart_loaded as (
    select
        account_id,
        cart_screen,
        cart_step,
        cart_version,
        origin,
        cart_type,
        date(original_timestamp) as max_date
    from 
        cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing_cart_loaded
    inner join accounts 
        on 
            accounts.instance_account_id = billing_cart_loaded.account_id
            -- Admin center logins last 15 days before win date
            and date(billing_cart_loaded.original_timestamp) <= accounts.win_date
            and date(billing_cart_loaded.original_timestamp) >= dateadd('day', -15, accounts.win_date)
    where paid_customer = FALSE
    qualify row_number() over (partition by account_id order by original_timestamp desc) = 1
)

select 
    accounts_.*,
    case 
        when modal_buy_now_.account_id is not null or modal_see_all_plans_.account_id is not null 
        then 'buy_now/see_all_plans_clicked' else 'no_cta_clicked'
    end as modal_clicked,
    modal_buy_now_.max_date as max_date_modal_buy_now,
    modal_see_all_plans_.max_date as max_date_modal_see_all_plans,
    admin_center_.max_date as max_date_admin_center,
    billing_cart_loaded_.* exclude (account_id),
        case when billing_cart_loaded_.account_id is null then 1 else 0 end as cart_null
from accounts accounts_
left join modal_buy_now modal_buy_now_ 
    on accounts_.instance_account_id = modal_buy_now_.account_id
left join modal_see_all_plans modal_see_all_plans_ 
    on accounts_.instance_account_id = modal_see_all_plans_.account_id
left join admin_center admin_center_ 
    on accounts_.instance_account_id = admin_center_.account_id
left join billing_cart_loaded billing_cart_loaded_ 
    on accounts_.instance_account_id = billing_cart_loaded_.account_id
where 
    accounts_.is_won = 1
    and win_date >= '2025-05-01'
order by win_date



--- Check results
select *
from wins_daily
where win_date >= '2025-05-01'
order by 1




select *
from cleansed.segment_billing.segment_billing_cart_loaded_scd2 
where account_id = 24029360
order by original_timestamp desc
limit 100



select *
from 
where account_id = 23973893


select
*
from functional.product_analytics.basic_tools_and_rules_instance_daily_snapshot
where 
    source_snapshot_date >= '2025-07-01'
    and instance_account_id = 23973893



--- Weird account
select *
from cleansed.segment_billing.segment_billing_cart_loaded_scd2 
where account_id = 25499722
order by original_timestamp desc
limit 100








--- Manually reviewing all customers
select *
from accounts accounts_
left join modal_buy_now modal_buy_now_ 
    on accounts_.instance_account_id = modal_buy_now_.account_id
left join modal_see_all_plans modal_see_all_plans_ 
    on accounts_.instance_account_id = modal_see_all_plans_.account_id
where 
    accounts_.is_won = 1
    and win_date >= '2025-05-01'
    and modal_buy_now_.max_date is null
    and modal_see_all_plans_.max_date is null







---- Only 1 customer loaded but not clicked buy now or see all plans. The rest do not exist
select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id in (
23891561,24095397,25549782,23966682,25499722,25087774,25511168,25382124,24029360,25512364,25513912,25535900,25503826,25510562,25109227,25501335,23790407,24096193,25515012,25048600,25531422,24079522,25434039,25464533,25438581,25464976,25463182,25558786
)





---- Checking CTA field in ZDP

select 
    cta,
    count(*) as total_obs,
    min(original_timestamp) as first_event_time,
    max(original_timestamp) as last_event_time
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
group by cta



select 
    cta,
    count(*) as total_obs,
    min(original_timestamp) as first_event_time,
    max(original_timestamp) as last_event_time
from RAW.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1
group by cta



--- Segment funnel
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
    --where 
      --  trial_accounts.instance_account_created_date >= '2025-05-01'
        --- Filters in Agus dashboard. Do not include them here
        --and cast(trial_accounts.help_desk_size_grouped as string) in ('1-9', '10-49')
        --and trial_accounts.first_verified_date is not null
),
--- Step 1: count interactions with each modal step
prompt_load as (
    select
        prompt_click.account_id,
        prompt_click.trial_type,
        prompt_click.account_id as unique_count,
        date_trunc('day', prompt_click.original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_scd2 prompt_click
    inner join accounts a
        on prompt_click.account_id = a.instance_account_id
    group by all
),

modal_load as (
    select
        load.account_id,
        load.offer_id,
        load.plan_name,
        load.preview_state,
        load.source,
        load.account_id as unique_count,
        date_trunc('day', load.original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
    inner join accounts a
        on load.account_id = a.instance_account_id
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
    select distinct
        prompt_load.date as loaded_date,
        prompt_load.account_id,
        modal_load.offer_id,
        modal_load.plan_name,
        modal_load.preview_state,
        modal_load.source,
        --- Fields relevant to buy now modal
        --modal_buy_now.agent_count as buy_now_agent_count,
        modal_buy_now.billing_cycle as buy_now_billing_cycle,
        modal_buy_now.offer_id as buy_now_offer_id,
        modal_buy_now.plan as buy_now_plan,
        modal_buy_now.plan_name as buy_now_plan_name,
        modal_buy_now.product as buy_now_product,
        modal_buy_now.promo_code as buy_now_promo_code,
        --- Counts per each modal
        --prompt_load.total_count as total_count_prompt_load,
        prompt_load.unique_count as unique_count_prompt_load,
        --modal_load.total_count as total_count_modal_loads,
        modal_load.unique_count as unique_count_modal_loads,
        --modal_dismiss.total_count as total_count_modal_dismiss,
        modal_dismiss.unique_count as unique_count_modal_dismiss,
        --modal_buy_now.total_count as total_count_modal_buy_now,
        modal_buy_now.unique_count as unique_count_modal_buy_now,
        --modal_see_all_plans.total_count as total_count_modal_see_all_plans,
        modal_see_all_plans.unique_count as unique_count_modal_see_all_plans
    from prompt_load 
    left join modal_dismiss
        on prompt_load.account_id = modal_dismiss.account_id
        and prompt_load.date = modal_dismiss.date
    left join modal_buy_now
        on prompt_load.account_id = modal_buy_now.account_id
        and prompt_load.date = modal_buy_now.date
    left join modal_load
        on prompt_load.account_id = modal_load.account_id
        and prompt_load.date = modal_load.date
    left join modal_see_all_plans
        on prompt_load.account_id = modal_see_all_plans.account_id
        and prompt_load.date = modal_see_all_plans.date
),

--- Join wins vs segment funnel
wins_daily as (
    select
        accounts_.win_date,
        count(*) as total_wins,
        count(distinct accounts_.instance_account_id) as unique_wins,
        sum(is_won_arr) as total_wins_arr,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and unique_count_modal_buy_now is not null 
                and unique_count_modal_see_all_plans is null 
                then accounts_.instance_account_id else null 
        end) as wins_just_buy_now,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and unique_count_modal_buy_now is null 
                and unique_count_modal_see_all_plans is not null 
                then accounts_.instance_account_id else null 
        end) as wins_just_see_all_plans,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and unique_count_modal_buy_now is not null 
                and unique_count_modal_see_all_plans is not null 
                then accounts_.instance_account_id else null 
        end) as wins_both,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and unique_count_modal_buy_now is null 
                and unique_count_modal_see_all_plans is null 
                then accounts_.instance_account_id else null 
        end) as wins_none,
    from accounts accounts_
    left join segment_events_all_tmp funnel
        on 
            accounts_.instance_account_id = funnel.account_id
            and accounts_.win_date = funnel.loaded_date

    where accounts_.is_won = 1
    group by 1
),

wins_all as (
    select *
    from accounts accounts_
    left join segment_events_all_tmp funnel
        on 
            accounts_.instance_account_id = funnel.account_id
            and accounts_.win_date = funnel.loaded_date
    where 
        accounts_.win_date >= '2025-05-01'
        and accounts_.is_won = 1
)

select *
from wins_all
order by win_date





select *
from presentation.growth_analytics.trial_shopping_cart_funnel
where account_id = 25548550



select *
from wins_daily 
where win_date >= '2025-05-01'
order by win_date





select *
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where account_id = 25515012





select *
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where account_id = 24079522



select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id = 25499722














--- Trial accounts as 

with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
    --where 
        --trial_accounts.instance_account_created_date >= '2025-05-01'
        --- Filters in Agus dashboard. Do not include them here
        --and cast(trial_accounts.help_desk_size_grouped as string) in ('1-9', '10-49')
        --and trial_accounts.first_verified_date is not null
),

buy_now_clicks as (
    select *
    from cleansed.segment_support.growth_engine_trial_cta_1_scd2 a
    inner join accounts b
        on a.account_id = b.instance_account_id
    where date_trunc('day', a.original_timestamp) >= '2025-05-01'::date
),

cart_loaded as (
    select *
    from cleansed.segment_billing.segment_billing_cart_loaded_scd2 a
    inner join accounts b
        on a.account_id = b.instance_account_id
    where 
        --- Cart visits from trials created 15 days before cart load date
        datediff(day, b.instance_account_created_date::date, date(a.original_timestamp)) <= 15
        and date(a.original_timestamp) >= b.instance_account_created_date
        --origin = 'trial_welcome_screen'
        --cart_screen in ('preset_trial_plan', 'buy_your_trial', 'buy_trial_plan')
        --and cart_type = 'spp_self_service'
        and paid_customer = False
        and date_trunc('day', original_timestamp) >= '2025-05-01'::date
),

--select origin, count(*) as total_obs
--from cleansed.segment_billing.segment_billing_cart_loaded_scd2 a
--group by 1
--order by 2 desc

wins_daily as (
    select
        accounts_.win_date,
        count(*) as total_wins,
        count(distinct accounts_.instance_account_id) as unique_wins,
        sum(is_won_arr) as total_wins_arr
    from accounts accounts_
    where accounts_.is_won = 1
    group by 1
),

--- Used "distinct" clause to remove duplicates in cart_loaded
wins_daily_cohorted as (
    select
        date_trunc('day', original_timestamp) as date,
        count(*) as total_wins_cohorted,
        sum(is_won_arr) as total_wins_arr_cohorted
    from (
        select distinct
            account_id,
            date(original_timestamp) as original_timestamp,
            is_won_arr
        from cart_loaded
        where 
            datediff(day, date(original_timestamp), win_date) <= 2
            and win_date >= date(original_timestamp)
            and is_won = 1
            ) as cart_loaded_wins
    group by 1
),

buy_now_daily as (
    select
        date_trunc('day', original_timestamp) as date,
        count(*) as total_buy_now_clicks,
        count(distinct account_id) as unique_buy_now_clicks
    from buy_now_clicks
    group by 1
),

cart_loaded_daily as (
    select
        date_trunc('day', original_timestamp) as date,
        count(*) as total_cart_loaded,
        count(distinct account_id) as unique_cart_loaded
    from cart_loaded
    group by 1
),

--- Merging all data

all_data as (
    select
        wins_daily_.win_date as date,
        cart_loaded_daily_.total_cart_loaded,
        cart_loaded_daily_.unique_cart_loaded,
        buy_now_daily_.total_buy_now_clicks,
        buy_now_daily_.unique_buy_now_clicks,
        wins_daily_.total_wins,
        wins_daily_.unique_wins,
        wins_daily_.total_wins_arr,
        wins_daily_cohorted_.total_wins_cohorted,
        wins_daily_cohorted_.total_wins_arr_cohorted
    from wins_daily wins_daily_
    left join buy_now_daily buy_now_daily_
        on wins_daily_.win_date = buy_now_daily_.date
    left join cart_loaded_daily cart_loaded_daily_
        on wins_daily_.win_date = cart_loaded_daily_.date
    left join wins_daily_cohorted wins_daily_cohorted_
        on wins_daily_.win_date = wins_daily_cohorted_.date
    where wins_daily_.win_date >= '2025-05-01'
    order by wins_daily_.win_date
)

select *
from all_data






select distinct account_id
from cleansed.segment_support.growth_engine_trial_cta_1_scd2 
where date_trunc('day', original_timestamp) = '2025-07-23'::date




select *
from cleansed.segment_billing.segment_billing_cart_loaded_scd2 
where account_id in (25543865,25544579,25525311,25523718,25542929,25541720,25524563,25514622,25542919,25493908,25439485,25496380,25544325,25515067,25543609,25541927,25540153,25544719,25537024,25472172,24380815,25544658,25543212,25544865,25544289,25543278,25542651,24674345,25543643,25544942,25543039,25541885,25512126,25539279,25544730,25518460,25543376,25537218,25495340,25541596,25544654,25526966,25544192,25544703,25544515,25545125,25532582,25532600,25544132,25543420,25543369,25542495,25507070,25542653,25441221,25542422,25543700,25545358,25537709,25484577,25544452,25542719,25544980,25496713)
and date_trunc('day', original_timestamp) = '2025-07-23'::date









select 
    offer_id, 
    min(original_timestamp) min_timestamp,
    max(original_timestamp) max_timestamp,
    count(*) as total_obs
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where lower(offer_id) like '%01jy%'
group by offer_id
order by total_obs desc





select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where offer_id = '01JYH0M68BAKCX4ZVHVMM7Y34S'




select *
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where account_id = 25485341



select 
    max()

select *
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25485341


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




select 
    current_date,
    '2025-12-12'::date d1,
    datediff(day, current_date, '2025-12-12'::date),
    datediff(day, current_date, '2024-12-12'::date)

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

--- Step 0: filtering by trial accounts

with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
    where 
        trial_accounts.instance_account_created_date >= '2025-05-01'
        --- Filters in Agus dashboard. Do not include them here
        --and cast(trial_accounts.help_desk_size_grouped as string) in ('1-9', '10-49')
        --and trial_accounts.first_verified_date is not null
),
--- Step 1: count interactions with each modal step
prompt_load as (
    select
        prompt_click.account_id,
        prompt_click.trial_type,
        prompt_click.account_id as unique_count,
        date_trunc('day', prompt_click.original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_scd2 prompt_click
    inner join accounts a
        on prompt_click.account_id = a.instance_account_id
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
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
    inner join accounts a
        on load.account_id = a.instance_account_id
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
        prompt_load.date as loaded_date,
        prompt_load.account_id,
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
        case when modal_load.source = 'CTA' then unique_count_modal_loads end as unique_count_modal_loads_cta,
        case when modal_load.source = 'auto_trigger' then unique_count_modal_loads end as unique_count_modal_loads_auto_trigger,
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
    left join modal_buy_now
        on modal_load.account_id = modal_buy_now.account_id
        and modal_load.date = modal_buy_now.date
    left join prompt_load
        on modal_load.account_id = prompt_load.account_id
        and modal_load.date = prompt_load.date
    left join modal_see_all_plans
        on modal_load.account_id = modal_see_all_plans.account_id
        and modal_load.date = modal_see_all_plans.date
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
        case 
            when 
                wins.win_date is not null 
                and unique_count_modal_buy_now is not null 
                and unique_count_modal_see_all_plans is null 
                then segment.account_id else null 
        end as wins_just_buy_now,
        case 
            when 
                wins.win_date is not null 
                and unique_count_modal_buy_now is null 
                and unique_count_modal_see_all_plans is not null 
                then segment.account_id else null 
        end as wins_just_see_all_plans,
        case 
            when 
                wins.win_date is not null 
                and unique_count_modal_buy_now is not null 
                and unique_count_modal_see_all_plans is not null 
                then segment.account_id else null 
        end as wins_both,
        case 
            when 
                wins.win_date is not null 
                and unique_count_modal_buy_now is null 
                and unique_count_modal_see_all_plans is null 
                then segment.account_id else null 
        end as wins_none,
        wins.core_base_plan_at_win,
        wins.subscription_term_days,
        wins.billing_cycle
    from segment_events_all_tmp segment
    left join wins
        on segment.account_id = wins.instance_account_id
        and date(segment.loaded_date) = date(wins.win_date)
)

select *
from segment_events_all
where wins_none is not null





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




----- Client that purchased, no event triggered

select *
from presentation.growth_analytics.trial_accounts trial_accounts
where instance_account_id = 25493908


select *
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
where account_id = 25493908


select *
from cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
where account_id = 25493908




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






------------------------------------------
--- Checking data accuracy

--- 1. Wins vs prompt/modal loads


with sub_term as (
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
        trial_accounts.instance_account_created_date,
        case when modal_load.account_id is not null then 1 else null end as is_modal_loaded,
        modal_load.max_loaded,
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
    left join (
        select account_id, date(max(original_timestamp)) max_loaded
        from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 
        group by 1
    ) as modal_load
        on trial_accounts.instance_account_id = modal_load.account_id
    where 
        trial_accounts.win_date is not null
        and trial_accounts.sales_model_at_win <> 'Assisted'
        and trial_accounts.is_direct_buy = FALSE  
        and trial_accounts.win_date >= '2025-06-01'
)

select 
    win_date, 
    count(*) as total_wins,
    sum(is_modal_loaded) is_modal_loaded,
    sum(case when instance_account_created_date >= '2025-07-17' then 1 else 0 end) as wins_after_july_17,
    sum(case when instance_account_created_date >= '2025-07-17' then is_modal_loaded else 0 end) as loads_after_july_17
from wins
group by win_date
order by win_date;




------------------------------------------
--- New funnels



--- Logins
with logins as (
    select  
        instance_account_id,
        case when count(distinct agent_last_login_timestamp) = 1 then max(instance_account_id) else null end as login_1,
        case when count(distinct agent_last_login_timestamp) >= 2 then max(instance_account_id) else null end as login_2,
        date(max(agent_last_login_timestamp)) as last_login_date
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    where
        agent_role in ('Admin', 'Billing Admin')
        or agent_is_owner = True
    group by instance_account_id
)

select
    trial_accounts.instance_account_created_date,
    count(*) as total_accounts,
    count(distinct trial_accounts.instance_account_id) as unique_accounts,
    count(distinct case when trial_accounts.first_verified_date is not null then trial_accounts.instance_account_id else null end) as unique_verified_accounts,
    
    --------------------------
    --- Logins:
    count(distinct case when logins_.last_login_date is null and trial_accounts.first_verified_date is not null then trial_accounts.instance_account_id end) as unique_login_null,
    count(distinct case when trial_accounts.first_verified_date is not null then logins_.login_1 end) as unique_login_1,
    count(distinct case when trial_accounts.first_verified_date is not null then logins_.login_2 end) as unique_login_2,

    --------------------------
    --- First modal loads:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    count(distinct case when first_modal.account_id is not null then first_modal.id end) as total_first_modal,
    count(distinct case when first_modal.account_id is not null then trial_accounts.instance_account_id end) as unique_first_modal,
    count(distinct case when first_modal.account_id is not null and trial_accounts.first_verified_date is not null then first_modal.id end) as total_first_modal_verified,
    count(distinct case when first_modal.account_id is not null and trial_accounts.first_verified_date is not null then trial_accounts.instance_account_id end) as unique_first_modal_verified,
    count(distinct case
            when
                first_modal.account_id is not null
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then first_modal.id
        end) as total_first_modal_timeboxed,
    count(distinct case
            when
                first_modal.account_id is not null
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_first_modal_timeboxed,
   
    --- Total/unique by auto trigger
    count(distinct case when modal_load.source = 'auto_trigger' and first_modal.account_id is not null then first_modal.id end) as total_first_modal_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and first_modal.account_id is not null then trial_accounts.instance_account_id end) as unique_first_modal_auto_trigger,
    count(distinct case
            when
                first_modal.account_id is not null
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then first_modal.id
        end) as total_first_modal_timeboxed_auto_trigger,
    count(distinct case
            when
                first_modal.account_id is not null
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_first_modal_timeboxed_auto_trigger,
   
    --- Total/unique by CTA click
    count(distinct case when modal_load.source = 'CTA' and first_modal.account_id is not null then first_modal.id end) as total_first_modal_cta,
    count(distinct case when modal_load.source = 'CTA' and first_modal.account_id is not null then trial_accounts.instance_account_id end) as unique_first_modal_cta,
    count(distinct case
            when
                first_modal.account_id is not null
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then first_modal.id
        end) as total_first_modal_timeboxed_cta,
    count(distinct case
            when
                first_modal.account_id is not null
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_first_modal_timeboxed_cta,

    --------------------------
    --- Modal loads:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    count(distinct case when modal_load.account_id is not null then modal_load.id end) as total_modal_loads,
    count(distinct case when modal_load.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_loads,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then modal_load.id
        end) as total_modal_loads_timeboxed,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_loads_timeboxed,
    
    --- Total/unique by auto trigger
    count(distinct case when modal_load.source = 'auto_trigger' and modal_load.account_id is not null then modal_load.id end) as total_modal_loads_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and modal_load.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_loads_auto_trigger,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then modal_load.id
        end) as total_modal_loads_timeboxed_auto_trigger,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_loads_timeboxed_auto_trigger,
    
    --- Total/unique by CTA click
    count(distinct case when modal_load.source = 'CTA' and modal_load.account_id is not null then modal_load.id end) as total_modal_loads_cta,
    count(distinct case when modal_load.source = 'CTA' and modal_load.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_loads_cta,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then modal_load.id
        end) as total_modal_loads_timeboxed_cta,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_loads_timeboxed_cta,

    --------------------------
    --- Modal buy now:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    count(distinct case when modal_buy_now.account_id is not null then modal_buy_now.id end) as total_modal_buy_now,
    count(distinct case when modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_buy_now,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then modal_buy_now.id
        end) as total_modal_buy_now_timeboxed,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_buy_now_timeboxed,
    
    --- Total/unique by auto trigger
    count(distinct case when modal_load.source = 'auto_trigger' and modal_buy_now.account_id is not null then modal_buy_now.id end) as total_modal_buy_now_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_buy_now_auto_trigger,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then modal_buy_now.id
        end) as total_modal_buy_now_timeboxed_auto_trigger,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_buy_now_timeboxed_auto_trigger,
    
    --- Total/unique by CTA click
    count(distinct case when modal_load.source = 'CTA' and modal_buy_now.account_id is not null then modal_buy_now.id end) as total_modal_buy_now_cta,
    count(distinct case when modal_load.source = 'CTA' and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_buy_now_cta,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then modal_buy_now.id
        end) as total_modal_buy_now_timeboxed_cta,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_buy_now_timeboxed_cta,

    --------------------------
    --- Cart loaded:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    count(distinct case when cart_load.account_id is not null then cart_load.id end) as total_cart_loaded_all,
    count(distinct case when cart_load.account_id is not null then trial_accounts.instance_account_id end) as unique_cart_loaded_all,
    count(distinct case when cart_load.account_id is not null and modal_buy_now.account_id is not null then cart_load.id end) as total_cart_loaded_buy_now,
    count(distinct case when cart_load.account_id is not null and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_cart_loaded_buy_now,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then cart_load.id
        end) as total_cart_loaded_timeboxed,
    count(distinct case 
            when 
                cart_load.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_cart_loaded_timeboxed_all,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_cart_loaded_timeboxed_buy_now,
    
    --- Total/unique by auto trigger
    count(distinct case when modal_load.source = 'auto_trigger' and cart_load.account_id is not null and modal_buy_now.account_id is not null then cart_load.id end) as total_cart_loaded_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and cart_load.account_id is not null and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_cart_loaded_auto_trigger,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then cart_load.id
        end) as total_cart_loaded_timeboxed_auto_trigger,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_cart_loaded_timeboxed_auto_trigger,
    
    --- Total/unique by CTA click
    count(distinct case when modal_load.source = 'CTA' and cart_load.account_id is not null and modal_buy_now.account_id is not null then cart_load.id end) as total_cart_loaded_cta,
    count(distinct case when modal_load.source = 'CTA' and cart_load.account_id is not null and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_cart_loaded_cta,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then cart_load.id
        end) as total_cart_loaded_timeboxed_cta,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_cart_loaded_timeboxed_cta,


    --------------------------
    --- Wins:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    sum(case when trial_accounts.win_date is not null then 1 else 0 end) as total_wins,
    count(distinct case when trial_accounts.win_date is not null then trial_accounts.instance_account_id end) as unique_wins,
    sum(case when trial_accounts.win_date is not null and first_modal.account_id is not null then 1 else 0 end) as total_wins_first_modal,
    count(distinct case when trial_accounts.win_date is not null and first_modal.account_id is not null then trial_accounts.instance_account_id end) as unique_wins_first_modal,
    sum(case when trial_accounts.win_date is not null and modal_buy_now.account_id is not null then 1 else 0 end) as total_wins_buy_now,
    count(distinct case when trial_accounts.win_date is not null and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_wins_buy_now,
    sum(case when trial_accounts.win_date is not null and cart_load.account_id is not null then 1 else 0 end) as total_wins_cart_loaded,
    count(distinct case when trial_accounts.win_date is not null and cart_load.account_id is not null then trial_accounts.instance_account_id end) as unique_wins_cart_loaded,
    sum(case 
            when 
                trial_accounts.win_date is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then 1
            else 0
        end) as total_wins_timeboxed,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_timeboxed,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and first_modal.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_first_modal_timeboxed,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_modal_buy_now_timeboxed,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and cart_load.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_cart_load_timeboxed,
    
    --- Total/unique by auto trigger
    sum(case when modal_load.source = 'auto_trigger' and trial_accounts.win_date is not null then 1 else 0 end) as total_wins_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and trial_accounts.win_date is not null then trial_accounts.instance_account_id end) as unique_wins_auto_trigger,
    sum(case 
            when 
                trial_accounts.win_date is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then 1
            else 0
        end) as total_wins_timeboxed_auto_trigger,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_timeboxed_auto_trigger,
    
    --- Total/unique by CTA click
    sum(case when modal_load.source = 'CTA' and trial_accounts.win_date is not null then 1 else 0 end) as total_wins_cta,
    count(distinct case when modal_load.source = 'CTA' and trial_accounts.win_date is not null then trial_accounts.instance_account_id end) as unique_wins_cta,
    sum(case 
            when 
                trial_accounts.win_date is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then 1
            else 0
        end) as total_wins_timeboxed_cta,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_timeboxed_cta,

from presentation.growth_analytics.trial_accounts trial_accounts 
left join cleansed.segment_support.growth_engine_trial_cta_1_scd2 first_modal
    on trial_accounts.instance_account_id = first_modal.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal_load
    on trial_accounts.instance_account_id = modal_load.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 modal_buy_now
    on trial_accounts.instance_account_id = modal_buy_now.account_id
left join logins logins_
    on trial_accounts.instance_account_id = logins_.instance_account_id
left join ( --- Count only cart loads from buy trial CTAs
    select *
    from cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where 
        cart_screen in ('preset_trial_plan', 'buy_your_trial', 'buy_trial_plan')
        and cart_type = 'spp_self_service'
        and paid_customer = False
    ) cart_load
    on trial_accounts.instance_account_id = cart_load.account_id
where 
    trial_accounts.instance_account_created_date >= '2025-07-17'
group by all
order by 1






-------------------
--- Additional checks
with logins as (
    select  
        instance_account_id,
        case when count(distinct agent_last_login_timestamp) = 1 then max(instance_account_id) else null end as login_1,
        case when count(distinct agent_last_login_timestamp) >= 2 then max(instance_account_id) else null end as login_2,
        date(max(agent_last_login_timestamp)) as last_login_date
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    where
        agent_role in ('Admin', 'Billing Admin')
        or agent_is_owner = True
    group by instance_account_id
)


select
    distinct
    trial_accounts.instance_account_created_date,
    trial_accounts.instance_account_id,
    logins_.last_login_date,
    logins_.login_1,
    logins_.login_2,
    first_modal.account_id as first_modal_id,
    modal_load.account_id as modal_load_id,
    modal_buy_now.account_id as modal_buy_now_id
    
from presentation.growth_analytics.trial_accounts trial_accounts 
left join cleansed.segment_support.growth_engine_trial_cta_1_scd2 first_modal
    on trial_accounts.instance_account_id = first_modal.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal_load
    on trial_accounts.instance_account_id = modal_load.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 modal_buy_now
    on trial_accounts.instance_account_id = modal_buy_now.account_id
left join logins logins_
    on trial_accounts.instance_account_id = logins_.instance_account_id
where 
    trial_accounts.instance_account_id = 25538897
group by all
order by 1







--- Logins
with logins as (
    select  
        instance_account_id,
        case when count(distinct agent_last_login_timestamp) = 1 then max(instance_account_id) else null end as login_1,
        case when count(distinct agent_last_login_timestamp) >= 2 then max(instance_account_id) else null end as login_2,
        date(max(agent_last_login_timestamp)) as last_login_date
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    where
        agent_role in ('Admin', 'Billing Admin')
        or agent_is_owner = True
    group by instance_account_id
)


select
    distinct
    trial_accounts.instance_account_created_date,
    trial_accounts.instance_account_id,
    logins_.last_login_date,
    logins_.login_1,
    logins_.login_2
    
from presentation.growth_analytics.trial_accounts trial_accounts 
left join cleansed.segment_support.growth_engine_trial_cta_1_scd2 first_modal
    on trial_accounts.instance_account_id = first_modal.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal_load
    on trial_accounts.instance_account_id = modal_load.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 modal_buy_now
    on trial_accounts.instance_account_id = modal_buy_now.account_id
left join logins logins_
    on trial_accounts.instance_account_id = logins_.instance_account_id
where 
    trial_accounts.instance_account_created_date = '2025-07-22'
    and first_verified_date is not null
group by all
order by 1










--- Logins
with logins as (
    select  
        instance_account_id,
        case when count(distinct agent_last_login_timestamp) = 1 then max(instance_account_id) else null end as login_1,
        case when count(distinct agent_last_login_timestamp) >= 2 then max(instance_account_id) else null end as login_2,
        date(max(agent_last_login_timestamp)) as last_login_date
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    where
        agent_role in ('Admin', 'Billing Admin')
        or agent_is_owner = True
    group by instance_account_id
)


select
    distinct
    trial_accounts.instance_account_created_date,
    trial_accounts.instance_account_id,
    trial_accounts.first_verified_date,
    logins_.last_login_date,
    first_modal.account_id as first_modal_id,
    modal_load.account_id as modal_load_id,
    modal_buy_now.account_id as modal_buy_now_id,
    modal_see_all_plans.account_id as modal_see_all_plans_id,
    cart_load.account_id as cart_loaded_id,
    trial_accounts.instance_account_arr_usd_at_win
    
from presentation.growth_analytics.trial_accounts trial_accounts 
left join cleansed.segment_support.growth_engine_trial_cta_1_scd2 first_modal
    on trial_accounts.instance_account_id = first_modal.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal_load
    on trial_accounts.instance_account_id = modal_load.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 modal_buy_now
    on trial_accounts.instance_account_id = modal_buy_now.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2 modal_see_all_plans
    on trial_accounts.instance_account_id = modal_see_all_plans.account_id
left join logins logins_
    on trial_accounts.instance_account_id = logins_.instance_account_id
left join ( --- Count only cart loads from buy trial CTAs
    select *
    from cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where 
        cart_screen in ('preset_trial_plan', 'buy_your_trial', 'buy_trial_plan')
        and cart_type = 'spp_self_service'
    ) cart_load
    on trial_accounts.instance_account_id = cart_load.account_id
where 
    trial_accounts.instance_account_created_date = '2025-07-22'
    and trial_accounts.win_date is not null
group by all
order by 1









select
    distinct
    trial_accounts.instance_account_created_date,
    trial_accounts.instance_account_id,
    first_modal.account_id as first_modal_id,
    modal_load.account_id as modal_load_id
from presentation.growth_analytics.trial_accounts trial_accounts 
left join cleansed.segment_support.growth_engine_trial_cta_1_scd2 first_modal
    on trial_accounts.instance_account_id = first_modal.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal_load
    on trial_accounts.instance_account_id = modal_load.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 modal_buy_now
    on trial_accounts.instance_account_id = modal_buy_now.account_id
where 
    trial_accounts.instance_account_created_date = '2025-07-22'
    and (first_modal.account_id is not null or modal_load.account_id is not null)




select *
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where account_id = 25538897



select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id = 25538897


select distinct account_id
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where source = 'auto_trigger'



--- Logins
with logins as (
    select  
        instance_account_id,
        case when count(distinct agent_last_login_timestamp) >= 1 then max(instance_account_id) else null end as login_1,
        case when count(distinct agent_last_login_timestamp) >= 2 then max(instance_account_id) else null end as login_2,
        date(max(agent_last_login_timestamp)) as last_login_date
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    where
        agent_role in ('Admin', 'Billing Admin')
        or agent_is_owner = True
    group by instance_account_id
)

select *
from logins
limit 10





---- Check data diff prompt vs modal loads

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

segment_events_all_tmp as (
    select
        modal_load.date as loaded_date,
        modal_load.account_id,
        modal_load.offer_id,
        modal_load.plan_name,
        modal_load.preview_state,
        modal_load.source,
        --- Counts per each modal
        prompt_load.total_count as total_count_prompt_load,
        prompt_load.unique_count as unique_count_prompt_load,
    from modal_load
    left join prompt_load
        on modal_load.account_id = prompt_load.account_id
        and modal_load.date = prompt_load.date
)

select *
from segment_events_all_tmp
where 
    loaded_date = '2025-07-21'::date
    and unique_count_prompt_load is null






select *
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where account_id = 25207849



select
    count(distinct loads.account_id) as unique_accounts,
    count(distinct case when trial_accounts.instance_account_id is not null then loads.account_id end) as unique_trial_accounts,
    count(distinct case when trial_accounts.instance_account_id is null then loads.account_id end) as unique_trial_accounts_created_today,
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 loads
left join presentation.growth_analytics.trial_accounts trial_accounts 
on loads.account_id = trial_accounts.instance_account_id
where date(date_trunc('day', loads.original_timestamp) ) = '2025-07-19'::date



select *
from foundational.customer.entity_mapping_daily_snapshot_bcv
where instance_account_id in (
    25207850,25207852,25207849,25207841,25537324,25207848,25207846,25207851,25536593,25207842,25207844,25207843
)







select
    count(*) as total_obs,
    count(distinct id) as unique_accounts,
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2












-------------------------------------
--- Legacy



select source, count(*) as total_obs
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
group by source



select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where source = 'auto_trigger'



select *
from presentation.growth_analytics.trial_accounts
where instance_account_id in (
25485348,25485348,25485352,25485348,25485348,11,25485348,25485348
)


select *
from presentation.growth_analytics.trial_accounts trial_accounts 
left join cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal_load
    on trial_accounts.instance_account_id = modal_load.account_id
    and date_trunc('day', modal_load.original_timestamp) = date_trunc('day', trial_accounts.instance_account_created_date)
where 
    trial_accounts.instance_account_created_date = '2025-07-17'
    and modal_load.account_id is not null



select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id = 25521005






select 
    date(date_trunc('day', original_timestamp)) date,
    count(distinct account_id) as unique_accounts,
    count(distinct case when preview_state = 'success' then account_id end) as unique_success_accounts,
    count(distinct case when preview_state = 'error' then account_id end) as unique_error_accounts,
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
group by 1
order by 1






select 
    date(date_trunc('day', original_timestamp)) date,
    count(distinct account_id) as unique_accounts,
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
group by 1
order by 1







select preview_state, count(*) as total_obs
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
group by preview_state





