-----------------------------------------------------------
--- Query to populate 
--- https://docs.google.com/spreadsheets/d/1lel00vDXHO7P8uIrtqCEZ8y5r7hR3vAlufkwDxPY2PE/edit?gid=686707884#gid=686707884




----------------------------------------------------
--- 1.0: Uncohorted funnel - clicks



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
        modal_see_all_plans.unique_count as unique_count_modal_see_all_plans,
        case 
            when modal_buy_now.unique_count is not null or modal_see_all_plans.unique_count is not null 
            then modal_load.account_id else null 
        end as unique_cta_clicks
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

-----------------------------------------------------
--- Modal win attribution
--- Extract maximum modal load & CTA click (buy now, see all plans)
--- For trialists before their win date, or in case win date is
--- null, the last modal load or click.

wins_attribution as (
    with modal_load_atr as (
        select
            load.account_id,
            load.offer_id,
            load.source,
            --- Trial win data
            trial_accounts.win_date,
            trial_accounts.core_base_plan_at_win,
            case when trial_accounts.win_date is not null then 1 else null end as is_won,
            case 
                when 
                    trial_accounts.win_date is not null 
                    and trial_accounts.sales_model_at_win <> 'Assisted'
                    and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
            end as is_won_ss,
            case when trial_accounts.win_date is not null then instance_account_arr_usd_at_win else null end as is_won_arr,
            case 
                when 
                    trial_accounts.win_date is not null 
                    and trial_accounts.sales_model_at_win <> 'Assisted'
                    and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
            end as is_won_ss_arr,
            original_timestamp as max_date_load,
            --- Trials extra info
            trial_accounts.region,
            trial_accounts.help_desk_size_grouped,
            trial_accounts.instance_account_created_date,
            trial_accounts.seats_capacity_band_at_win,
        from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
        --- Remove testing accounts
        inner join presentation.growth_analytics.trial_accounts trial_accounts
            on
                trial_accounts.instance_account_id = load.account_id
                --- Loads before win, or all loads in case not won
                and (trial_accounts.win_date is null or date(load.original_timestamp) <= trial_accounts.win_date)
        qualify row_number() over (partition by load.account_id order by original_timestamp desc) = 1
    ),

    modal_buy_now_atr as (
        select
            buy_now.account_id,
            max(original_timestamp) as max_date_buy_now
        from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 buy_now
        --- Remove testing accounts
        inner join presentation.growth_analytics.trial_accounts trial_accounts  
            on 
                trial_accounts.instance_account_id = buy_now.account_id
                and (trial_accounts.win_date is null or date(buy_now.original_timestamp) <= trial_accounts.win_date)
        group by all
    ),

    modal_see_all_plans_atr as (
        select
            see_all_plans.account_id,
            max(original_timestamp) as max_date_see_all_plans
        from cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2 see_all_plans
        --- Remove testing accounts
        inner join presentation.growth_analytics.trial_accounts trial_accounts
            on
                trial_accounts.instance_account_id = see_all_plans.account_id
                and (trial_accounts.win_date is null or date(see_all_plans.original_timestamp) <= trial_accounts.win_date)
        group by all
    ),

    modal_funnel as (
        select
            modal_load_.*,
            modal_buy_now_.max_date_buy_now,
            modal_see_all_plans_.max_date_see_all_plans,
            --- Click attribution
            case
                when 
                    modal_buy_now_.max_date_buy_now is null and modal_see_all_plans_.max_date_see_all_plans is null
                then 'No modal click'
                when 
                    --- Clicked buy now last before win
                    --- Modal buy now click after modal load
                    (
                        modal_buy_now_.max_date_buy_now > modal_see_all_plans_.max_date_see_all_plans
                        and modal_buy_now_.max_date_buy_now > modal_load_.max_date_load
                    )
                    or modal_see_all_plans_.max_date_see_all_plans is null
                then 'Buy now click'
                when 
                    --- Clicked see all plans last before win
                    --- Modal see all plans click after modal load
                    (
                        modal_see_all_plans_.max_date_see_all_plans > modal_buy_now_.max_date_buy_now
                        and modal_see_all_plans_.max_date_see_all_plans > modal_load_.max_date_load
                    )
                    or modal_buy_now_.max_date_buy_now is null
                then 'See all plans click'
                else 'Other'
            end as win_cta_attribution
        from modal_load_atr modal_load_
        left join modal_buy_now_atr modal_buy_now_
            on modal_load_.account_id = modal_buy_now_.account_id
        left join modal_see_all_plans_atr modal_see_all_plans_
            on modal_load_.account_id = modal_see_all_plans_.account_id
    )

    select *
    from modal_funnel
),

segment_events_all as (
    select
        segment.*,
        crms.crm_account_name,
        crms.pro_forma_market_segment,
        --- Using win attribution table
        wins.win_date,
        wins.is_won_arr,
        datediff(day, segment.loaded_date::date, wins.win_date::date) as days_to_win,
        --- Win flags
        case when wins.win_date is not null then 1 else null end as is_won_all,
        case when wins.win_date is not null then segment.account_id else null end as is_won_unique,
        case when wins.is_won_ss is not null then segment.account_id else null end as is_won_unique_ss,
        --- CTAs flags
        
        case 
            when win_cta_attribution in ('Buy now click', 'See all plans click')
            then is_won_unique_ss
            else null
        end as is_won_cta_unique,

        case 
            when win_cta_attribution in ('Buy now click')
            then is_won_unique_ss
            else null
        end as wins_just_buy_now,
        case 
            when win_cta_attribution in ('See all plans click')
            then is_won_unique_ss
            else null
        end as wins_just_see_all_plans,
        --- Duplicating it to not break the dashboards
        is_won_cta_unique as wins_both,
        case 
            when win_cta_attribution in ('No modal click', 'Other')
            then is_won_unique_ss
            else null
        end as is_won_no_cta,
        --- Base plan categorization to order by price
        case
            when wins.core_base_plan_at_win = 'Support Team' then '1. Support Team'
            when wins.core_base_plan_at_win = 'Support Professional' then '2. Support Professional'
            when wins.core_base_plan_at_win = 'Support Enterprise' then '3. Support Enterprise'
            when wins.core_base_plan_at_win = 'Zendesk Suite Team' then '4. Zendesk Suite Team'
            when wins.core_base_plan_at_win = 'Zendesk Suite Growth' then '5. Zendesk Suite Growth'
            when wins.core_base_plan_at_win = 'Zendesk Suite Professional' then '6. Zendesk Suite Professional'
            when wins.core_base_plan_at_win = 'Zendesk Suite Enterprise' then '7. Zendesk Suite Enterprise'
            when wins.core_base_plan_at_win is null then '8 No Win'
            else '9, Other plan won'
        end as core_base_plan_at_win,
        --- Trials extra info
        wins.region,
        wins.help_desk_size_grouped,
        wins.instance_account_created_date,
        wins.seats_capacity_band_at_win,
    from segment_events_all_tmp segment
    left join wins_attribution wins
        on 
            segment.account_id = wins.account_id
            and segment.offer_id = wins.offer_id
            and segment.source = wins.source
            and date(segment.loaded_date) = date(wins.max_date_load)
    left join foundational.customer.dim_crm_accounts_daily_snapshot crms
        on 
            segment.crm_account_id = crms.crm_account_id
            and date(segment.loaded_date) = crms.source_snapshot_date

)

select *, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) as updated_at
from segment_events_all;







select *
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25762581
