--- Test new wins recommendatation






--- Step 0: filtering by trial accounts
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        trial_accounts.crm_account_id,
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
        a.crm_account_id,
        offer_id,
        plan_name,
        preview_state,
        source,
        account_id as unique_count,
        original_timestamp as loaded_timestamp,
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
        modal_load.loaded_timestamp,
        modal_load.date as loaded_date,
        modal_load.account_id,
        modal_load.crm_account_id,
        case when modal_load.offer_id is null then 'No Offer' else modal_load.offer_id end as offer_id,
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
        case when modal_load.source = 'CTA' then total_count_modal_loads end as total_count_modal_loads_cta,
        case when modal_load.source = 'auto_trigger' then total_count_modal_loads end as total_count_modal_loads_auto_trigger,
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

--- Including wins here but they will be cohorted by cart load.
--- The correct # of uncohorted wins will be measured in a different query.

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
        --and trial_accounts.sales_model_at_win <> 'Assisted'
        --and trial_accounts.is_direct_buy = FALSE  
        and trial_accounts.win_date >= '2025-06-01'
),

segment_events_all as (
    select
        segment.*,
        crms.crm_account_name,
        crms.pro_forma_market_segment,
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
        --and date(segment.loaded_date) = date(wins.win_date)
    left join foundational.customer.dim_crm_accounts_daily_snapshot crms
        on 
            segment.crm_account_id = crms.crm_account_id
            and date(segment.loaded_date) = crms.source_snapshot_date

),

emails as (
    select distinct instance_account_id, agent_name, agent_email
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    where 
        agent_is_active = True
        and agent_is_owner = True
        and agent_role = 'Admin'
)

select distinct
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) as updated_at,
    segments_.loaded_timestamp,
    date(segments_.loaded_date) loaded_day,
    segments_.crm_account_id,
    segments_.crm_account_name,
    segments_.account_id as instance_account_id,
    e.agent_name as agent_admin_name,
    e.agent_email as agent_email,
    segments_.pro_forma_market_segment, 
    ta.help_desk_size_grouped,
    segments_.offer_id, 
    segments_.plan_name, 
    segments_.preview_state, 
    segments_.source,
    segments_.BUY_NOW_AGENT_COUNT, segments_.BUY_NOW_BILLING_CYCLE, segments_.BUY_NOW_OFFER_ID, segments_.BUY_NOW_PLAN, segments_.BUY_NOW_PLAN_NAME, segments_.BUY_NOW_PRODUCT, segments_.BUY_NOW_PROMO_CODE, 
    segments_.UNIQUE_COUNT_PROMPT_LOAD as is_cta_loaded,
    segments_.UNIQUE_COUNT_MODAL_LOADS as is_modal_loaded,
    segments_.UNIQUE_COUNT_MODAL_LOADS_CTA as is_modal_loaded_cta,
    segments_.UNIQUE_COUNT_MODAL_LOADS_AUTO_TRIGGER as is_modal_loaded_auto_trigger,
    segments_.UNIQUE_COUNT_MODAL_DISMISS as is_modal_dismissed,
    segments_.UNIQUE_COUNT_MODAL_BUY_NOW as is_modal_buy_now_clicked,
    segments_.UNIQUE_COUNT_MODAL_SEE_ALL_PLANS as is_modal_see_all_plans_clicked,
    segments_.win_date,
    segments_.INSTANCE_ACCOUNT_ARR_USD_AT_WIN,
from segment_events_all segments_
left join presentation.growth_analytics.trial_accounts ta
    on segments_.account_id = ta.instance_account_id
left join emails e
    on segments_.account_id = e.instance_account_id
--where pro_forma_market_segment in ('SMB', 'Enterprise', 'Commercial', 'Strategic')
order by segments_.crm_account_id, segments_.loaded_timestamp







with emails as (
    select distinct instance_account_id, agent_name, agent_email
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    where 
        agent_is_active = True
        and agent_is_owner = True
        and agent_role = 'Admin'
)




select distinct instance_account_id, agent_name, agent_email
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    where 
        agent_is_active = True
        and agent_is_owner = True
        and agent_role = 'Admin'
        and instance_account_id = 25353644




select agents.instance_account_id, simple_setup_scd2.*
from cleansed.segment_support.simple_setup_scd2 as simple_setup_scd2
left join propagated_foundational.product_agent_info.dim_agent_emails_bcv agents
    on simple_setup_scd2.user_id = agents.agent_id
where 
    action = 'click'
    and feature = 'macros'
    and name = 'activate-action'
    and convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-08'::date
order by timestamp
