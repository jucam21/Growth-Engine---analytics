
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
            else '9, Other'
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

select 
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    sum(is_won_all) as total_wins,
    count(distinct is_won_unique) as unique_wins,
    count(distinct is_won_unique_ss) as is_won_unique_ss,
    --count(distinct wins_none) as wins_none,
    count(distinct case when wins_just_buy_now is not null or wins_just_see_all_plans is not null then account_id else null end) as wins_with_cta,
    --count(distinct is_won_cta_unique_non_ss) is_won_cta_unique_non_ss
from segment_events_all

























-----------------------------------------------------
--- Agent home CTA funnels
--- Measure compare plans - buy your trials funnels
--- Attribution of wins to last clicked CTA before win
--- Modified shopping cart funnel to do it in a sequential way


--- Step 0: Import relevant fields from trial accounts
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.crm_account_id,
        --- Overall Win data
        trial_accounts.win_date,
        case when trial_accounts.win_date is not null then 1 else null end as is_won,
        case when trial_accounts.win_date is not null then instance_account_arr_usd_at_win else null end as is_won_arr,
        --- SS wins
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won_ss,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_ss_arr,
        --- Trials extra info
        trial_accounts.region,
        trial_accounts.help_desk_size_grouped,
        trial_accounts.instance_account_created_date,
        trial_accounts.seats_capacity_band_at_win,
    from presentation.growth_analytics.trial_accounts trial_accounts 
),

--- Step 1: count interactions with each modal step
--- Inner join to trial accounts to remove testing accounts

cta_click as (
    select distinct
        cta_click.original_timestamp,
        convert_timezone('UTC', 'America/Los_Angeles', cta_click.original_timestamp) original_timestamp_pt,
        cta_click.account_id,
        cta_click.trial_type,
        cta_click.cta,
    from
        cleansed.segment_support.growth_engine_trial_cta_1_scd2 cta_click
    inner join accounts a
        on cta_click.account_id = a.instance_account_id
),

modal_load as (
    select distinct
        load.original_timestamp,
        convert_timezone('UTC', 'America/Los_Angeles', load.original_timestamp) original_timestamp_pt,
        account_id,
        offer_id,
        plan_name,
        preview_state,
        source
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
    inner join accounts a
        on load.account_id = a.instance_account_id
),

modal_buy_now as (
    select distinct
        original_timestamp,
        convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) original_timestamp_pt,
        account_id,
        promo_code
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
),

modal_see_all_plans as (
    select distinct
        original_timestamp,
        convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) original_timestamp_pt,
        account_id,
        plan_name,
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
),


------------------------------------------------------------------------
--- Step 2: Plan lineup (see all plans), payment page visits & submits

---- Plan lineup
plan_lineup as (
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        product,
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        and cart_screen = 'preset_all_plans'
        and cart_step = 'multi_step_plan'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
),      
        
---- Payment page visits
payment_page_from_support as (
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Support - All Plans' as product_cta
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product = 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Support - All Plans' as product_cta
    from
        cleansed.segment_billing.segment_billing_payment_loaded_scd2
    where
        not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product = 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Support - All Plans' as product_cta
    from
        cleansed.bq_archive.billing_payment_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product = 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Support - All Plans' as product_cta
    from
        cleansed.bq_archive.billing_cart_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product = 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
),

payment_page_from_suite as (
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Suite - All Plans' as product_cta
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product <> 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Suite - All Plans' as product_cta
    from
        cleansed.segment_billing.segment_billing_payment_loaded_scd2
    where
        not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product <> 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Suite - All Plans' as product_cta
    from
        cleansed.bq_archive.billing_payment_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product <> 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Suite - All Plans' as product_cta
    from
        cleansed.bq_archive.billing_cart_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product <> 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
),

payment_page_from_trial_buy as (
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Buy Trial Plan' as cta_name,
        'Suite - Buy Trial' as product_cta
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Buy Trial Plan' as cta_name,
        'Suite - Buy Trial' as product_cta
    from
        cleansed.segment_billing.segment_billing_payment_loaded_scd2
    where
        not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Buy Trial Plan' as cta_name,
        'Suite - Buy Trial' as product_cta
    from
        cleansed.bq_archive.billing_payment_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        --- Adjusted Agus's query. Was compare plans before
        'Buy Trial Plan' as cta_name,
        'Suite - Buy Trial' as product_cta
    from
        cleansed.bq_archive.billing_cart_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
),

payment_page_visits as (
    select * from payment_page_from_support
    union all
    select * from payment_page_from_suite
    union all
    select * from payment_page_from_trial_buy
),

--- Payment submissions

payment_submit_all_plans as (
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        'Compare plans' as cta_name,
        --- Extracting plan sumbitted in the event
        parse_json(new_products)[0]:product::string as product_cta,
    from
        cleansed.segment_billing.segment_billing_update_submit_scd2
    where
        not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        'Compare plans' as cta_name,
        parse_json(new_products)[0]:product::string as product_cta,
    from
        cleansed.bq_archive.billing_update_submit
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_screen <> 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
),

payment_submit_buy_trial as (
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        'Buy Trial Plan' as cta_name,
        'zendesk_suite' as product_cta
    from
        cleansed.segment_billing.segment_billing_update_submit_scd2
    where
        not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        'Buy Trial Plan' as cta_name,
        'zendesk_suite' as product_cta
    from
        cleansed.bq_archive.billing_update_submit
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
),

payment_submission as (
    select * from payment_submit_all_plans
    union all
    select * from payment_submit_buy_trial
),


-------------------------------------------------------------------
--- Step 3: Construct each CTA funnel

--- Buy your trial CTA funnel
--- Determining closest next event in the funnel sequence
--- Limiting to 120 minutes between events

buy_your_trial_payment_page as (
    select
        cta_click_.*,
        ppv.timestamp as ppv_timestamp,
    from cta_click cta_click_
    left join payment_page_visits ppv
        on cta_click_.account_id = ppv.account_id
        and ppv.cta_name = 'Buy Trial Plan'
        and ppv.timestamp >= cta_click_.original_timestamp
        and datediff(minute, cta_click_.original_timestamp, ppv.timestamp) <= 120
    where cta_click_.cta = 'purchase'
    qualify row_number() over (partition by cta_click_.account_id, cta_click_.original_timestamp order by ppv.timestamp) = 1
),

buy_your_trial_payment_submit as (
    select
        buy_your_trial_payment_page_.*,
        pps.timestamp as pps_timestamp
    from buy_your_trial_payment_page buy_your_trial_payment_page_
    left join payment_submission pps
        on buy_your_trial_payment_page_.account_id = pps.account_id
        and pps.cta_name = 'Buy Trial Plan'
        and pps.timestamp >= buy_your_trial_payment_page_.ppv_timestamp
        and datediff(minute, buy_your_trial_payment_page_.ppv_timestamp, pps.timestamp) <= 120
    qualify row_number() over (partition by buy_your_trial_payment_page_.account_id, buy_your_trial_payment_page_.original_timestamp order by pps.timestamp) = 1
),

--- Compare plans CTA funnel
--- Merging compare plans clicks
--- Same 120 minutes limit between events


--- Compare plans join
--- Some modal loads from CTA do not have a preceding compare plans click
--- This happened until Sep17
--- Unioning all compare plans & load accounts with all
--- modal load without preceding compare plans click 

compare_plans_into_modal_load as (
    --- Accounts with compare plans clicks & modal load
    select
        cta_click_.original_timestamp as cta_click_timestamp,
        cta_click_.original_timestamp_pt as cta_click_timestamp_pt,
        cta_click_.account_id as cta_click_account_id,
        cta_click_.trial_type as cta_click_trial_type,
        cta_click_.cta as cta_click_cta,
        modal_load_.*
    from cta_click cta_click_
    left join modal_load modal_load_
        on cta_click_.account_id = modal_load_.account_id
        and modal_load_.original_timestamp >= cta_click_.original_timestamp
        and datediff(minute, cta_click_.original_timestamp, modal_load_.original_timestamp) <= 120
        --- Modal loads from compare plans clicks
        and modal_load_.source = 'CTA'
    --- Just compare plans clicks
    where cta_click_.cta = 'compare'
    qualify row_number() over (partition by cta_click_.account_id, cta_click_.original_timestamp order by modal_load_.original_timestamp) = 1

    union all

    --- Accounts with modal load, but no preceding compare plans click
    select
        --- Inputting nulls for compare plans clicks
        modal_load_.original_timestamp as cta_click_timestamp,
        modal_load_.original_timestamp_pt as cta_click_timestamp_pt,
        modal_load_.account_id as cta_click_account_id,
        'trial' as cta_click_trial_type,
        'compare' as cta_click_cta,
        modal_load_.*,
    from modal_load modal_load_
    left join cta_click cta_click_
        on cta_click_.account_id = modal_load_.account_id
        and modal_load_.original_timestamp >= cta_click_.original_timestamp
        and datediff(minute, cta_click_.original_timestamp, modal_load_.original_timestamp) <= 120
        and cta_click_.cta = 'compare'
    where 
        modal_load_.source = 'CTA'
        and cta_click_.account_id is null
),

compare_plans_and_auto_trigger as (
    select
        *
    from compare_plans_into_modal_load cta_click_
    union all
    select
        null as cta_click_timestamp,
        null as cta_click_timestamp_pt,
        null as cta_click_account_id,
        null as cta_click_trial_type,
        null as cta_click_cta,
        modal_load_.*,
    from modal_load modal_load_
    where modal_load_.source = 'auto_trigger'
),

--- Modal buy now
modal_load_buy_now_click as (
    select
        modal_load_.*,
        modal_buy_now_.original_timestamp as buy_now_timestamp
    from compare_plans_and_auto_trigger modal_load_
    left join modal_buy_now modal_buy_now_
        on modal_load_.account_id = modal_buy_now_.account_id
        and modal_buy_now_.original_timestamp >= modal_load_.original_timestamp
        and datediff(minute, modal_load_.original_timestamp, modal_buy_now_.original_timestamp) <= 120
    qualify row_number() over (partition by modal_load_.account_id, modal_load_.original_timestamp order by modal_buy_now_.original_timestamp) = 1
),

modal_load_buy_now_payment_visit as (
    select
        modal_load_buy_now_click_.*,
        ppv.timestamp as ppv_timestamp
    from modal_load_buy_now_click modal_load_buy_now_click_
    left join payment_page_visits ppv
        on modal_load_buy_now_click_.account_id = ppv.account_id
        and ppv.cta_name = 'Buy Trial Plan'
        and ppv.timestamp >= modal_load_buy_now_click_.buy_now_timestamp
        and datediff(minute, modal_load_buy_now_click_.buy_now_timestamp, ppv.timestamp) <= 120
    qualify row_number() over (partition by modal_load_buy_now_click_.account_id, modal_load_buy_now_click_.original_timestamp order by ppv.timestamp) = 1
),

modal_load_buy_now_payment_submit as (
    select
        modal_load_buy_now_payment_visit_.*,
        pps.timestamp as pps_timestamp
    from modal_load_buy_now_payment_visit modal_load_buy_now_payment_visit_
    left join payment_submission pps
        on modal_load_buy_now_payment_visit_.account_id = pps.account_id
        and pps.cta_name = 'Buy Trial Plan'
        and pps.timestamp >= modal_load_buy_now_payment_visit_.ppv_timestamp
        and datediff(minute, modal_load_buy_now_payment_visit_.ppv_timestamp, pps.timestamp) <= 120
    qualify row_number() over (partition by modal_load_buy_now_payment_visit_.account_id, modal_load_buy_now_payment_visit_.original_timestamp order by pps.timestamp) = 1
),

--- Modal see all plans

modal_load_see_all_plans_click as (
    select
        modal_load_.*,
        modal_see_all_plans_.original_timestamp as see_all_plans_timestamp
    from compare_plans_and_auto_trigger modal_load_
    left join modal_see_all_plans modal_see_all_plans_
        on modal_load_.account_id = modal_see_all_plans_.account_id
        and modal_see_all_plans_.original_timestamp >= modal_load_.original_timestamp
        and datediff(minute, modal_load_.original_timestamp, modal_see_all_plans_.original_timestamp) <= 120
    qualify row_number() over (partition by modal_load_.account_id, modal_load_.original_timestamp order by modal_see_all_plans_.original_timestamp) = 1
),

--- Support mini funnel

modal_load_see_all_plans_support as (
    select
        modal_load_see_all_plans_click_.*,
        pl.timestamp as plan_lineup_support_timestamp
    from modal_load_see_all_plans_click modal_load_see_all_plans_click_
    left join plan_lineup pl
        on modal_load_see_all_plans_click_.account_id = pl.account_id
        and pl.product = 'support'
        and pl.timestamp >= modal_load_see_all_plans_click_.see_all_plans_timestamp
        and datediff(minute, modal_load_see_all_plans_click_.see_all_plans_timestamp, pl.timestamp) <= 120
    qualify row_number() over (partition by modal_load_see_all_plans_click_.account_id, modal_load_see_all_plans_click_.original_timestamp order by pl.timestamp) = 1
),

modal_see_all_plans_support_payment_visit as (
    select
        modal_load_see_all_plans_support_.*,
        ppv.timestamp as ppv_support_timestamp
    from modal_load_see_all_plans_support modal_load_see_all_plans_support_
    left join payment_page_visits ppv
        on modal_load_see_all_plans_support_.account_id = ppv.account_id
        and ppv.cta_name = 'Compare plans'
        and ppv.product_cta = 'Support - All Plans'
        and ppv.timestamp >= modal_load_see_all_plans_support_.plan_lineup_support_timestamp
        and datediff(minute, modal_load_see_all_plans_support_.plan_lineup_support_timestamp, ppv.timestamp) <= 120
    --- Only for support lineup loads
    where modal_load_see_all_plans_support_.plan_lineup_support_timestamp is not null
    qualify row_number() over (partition by modal_load_see_all_plans_support_.account_id, modal_load_see_all_plans_support_.original_timestamp order by ppv.timestamp) = 1
),

modal_see_all_plans_support_payment_submit as (
    select
        modal_see_all_plans_support_payment_visit_.*,
        pps.timestamp as pps_support_timestamp
    from modal_see_all_plans_support_payment_visit modal_see_all_plans_support_payment_visit_
    left join payment_submission pps
        on modal_see_all_plans_support_payment_visit_.account_id = pps.account_id
        and pps.cta_name = 'Compare plans'
        and pps.product_cta = 'support'
        and pps.timestamp >= modal_see_all_plans_support_payment_visit_.ppv_support_timestamp
        and datediff(minute, modal_see_all_plans_support_payment_visit_.ppv_support_timestamp, pps.timestamp) <= 120
    qualify row_number() over (partition by modal_see_all_plans_support_payment_visit_.account_id, modal_see_all_plans_support_payment_visit_.original_timestamp order by pps.timestamp) = 1
),

--- Suite mini funnel

modal_load_see_all_plans_suite as (
    select
        modal_load_see_all_plans_click_.*,
        pl.timestamp as plan_lineup_suite_timestamp
    from modal_load_see_all_plans_click modal_load_see_all_plans_click_
    left join plan_lineup pl
        on modal_load_see_all_plans_click_.account_id = pl.account_id
        and pl.product <> 'support'
        and pl.timestamp >= modal_load_see_all_plans_click_.see_all_plans_timestamp
        and datediff(minute, modal_load_see_all_plans_click_.see_all_plans_timestamp, pl.timestamp) <= 120
    qualify row_number() over (partition by modal_load_see_all_plans_click_.account_id, modal_load_see_all_plans_click_.original_timestamp order by pl.timestamp) = 1
),

modal_see_all_plans_suite_payment_visit as (
    select
        modal_load_see_all_plans_suite_.*,
        ppv.timestamp as ppv_suite_timestamp
    from modal_load_see_all_plans_suite modal_load_see_all_plans_suite_
    left join payment_page_visits ppv
        on modal_load_see_all_plans_suite_.account_id = ppv.account_id
        and ppv.cta_name = 'Compare plans'
        and ppv.product_cta = 'Suite - All Plans'
        and ppv.timestamp >= modal_load_see_all_plans_suite_.plan_lineup_suite_timestamp
        and datediff(minute, modal_load_see_all_plans_suite_.plan_lineup_suite_timestamp, ppv.timestamp) <= 120
    --- Only for suite lineup loads
    where modal_load_see_all_plans_suite_.plan_lineup_suite_timestamp is not null
    qualify row_number() over (partition by modal_load_see_all_plans_suite_.account_id, modal_load_see_all_plans_suite_.original_timestamp order by ppv.timestamp) = 1
),

modal_see_all_plans_suite_payment_submit as (
    select
        modal_see_all_plans_suite_payment_visit_.*,
        pps.timestamp as pps_suite_timestamp
    from modal_see_all_plans_suite_payment_visit modal_see_all_plans_suite_payment_visit_
    left join payment_submission pps
        on modal_see_all_plans_suite_payment_visit_.account_id = pps.account_id
        and pps.cta_name = 'Compare plans'
        and pps.product_cta = 'zendesk_suite'
        and pps.timestamp >= modal_see_all_plans_suite_payment_visit_.ppv_suite_timestamp
        and datediff(minute, modal_see_all_plans_suite_payment_visit_.ppv_suite_timestamp, pps.timestamp) <= 120
    qualify row_number() over (partition by modal_see_all_plans_suite_payment_visit_.account_id, modal_see_all_plans_suite_payment_visit_.original_timestamp order by pps.timestamp) = 1
),

modal_see_all_plans_payment_submit_joined as (
    select
        modal_load_see_all_plans_click_.*,
        modal_see_all_plans_support_payment_submit_.plan_lineup_support_timestamp,
        modal_see_all_plans_support_payment_submit_.ppv_support_timestamp,
        modal_see_all_plans_support_payment_submit_.pps_support_timestamp,
        modal_see_all_plans_suite_payment_submit_.plan_lineup_suite_timestamp,
        modal_see_all_plans_suite_payment_submit_.ppv_suite_timestamp,
        modal_see_all_plans_suite_payment_submit_.pps_suite_timestamp,
        --- Payment visits or submits aggregated timestamps
        case 
            when ppv_support_timestamp is not null and ppv_suite_timestamp is not null then least(ppv_support_timestamp, ppv_suite_timestamp)
            when ppv_support_timestamp is not null then ppv_support_timestamp
            when ppv_suite_timestamp is not null then ppv_suite_timestamp
            else null
        end as ppv_timestamp,
        case 
            when pps_support_timestamp is not null and pps_suite_timestamp is not null then least(pps_support_timestamp, pps_suite_timestamp)
            when pps_support_timestamp is not null then pps_support_timestamp
            when pps_suite_timestamp is not null then pps_suite_timestamp
            else null
        end as pps_timestamp
    from modal_load_see_all_plans_click modal_load_see_all_plans_click_
    --- Joining at the timestamp level to measure events that happened in the same session
    left join modal_see_all_plans_support_payment_submit modal_see_all_plans_support_payment_submit_
        on modal_load_see_all_plans_click_.account_id = modal_see_all_plans_support_payment_submit_.account_id
        and modal_load_see_all_plans_click_.original_timestamp = modal_see_all_plans_support_payment_submit_.original_timestamp
    left join modal_see_all_plans_suite_payment_submit modal_see_all_plans_suite_payment_submit_
        on modal_load_see_all_plans_click_.account_id = modal_see_all_plans_suite_payment_submit_.account_id
        and modal_load_see_all_plans_click_.original_timestamp = modal_see_all_plans_suite_payment_submit_.original_timestamp
),


----------------------------------------------------------------------
--- Unioning all funnels into a single table for analysis

--- Aligning columns across all tables
-- Step 1: List columns for each table
-- buy_your_trial_payment_submit columns:
--   original_timestamp, original_timestamp_pt, account_id, trial_type, cta, ppv_timestamp, pps_timestamp

-- modal_load_buy_now_payment_submit columns:
--   cta_click_timestamp, cta_click_timestamp_pt, cta_click_account_id, cta_click_trial_type, cta_click_cta,
--   original_timestamp, original_timestamp_pt, account_id, offer_id, plan_name, preview_state, source,
--   buy_now_timestamp, ppv_timestamp, pps_timestamp

-- modal_see_all_plans_payment_submit_joined columns:
--   cta_click_timestamp, cta_click_timestamp_pt, cta_click_account_id, cta_click_trial_type, cta_click_cta,
--   original_timestamp, original_timestamp_pt, account_id, offer_id, plan_name, preview_state, source,
--   see_all_plans_timestamp, plan_lineup_support_timestamp, ppv_support_timestamp, pps_support_timestamp,
--   plan_lineup_suite_timestamp, ppv_suite_timestamp, pps_suite_timestamp, ppv_timestamp, pps_timestamp

-- Step 2: Union all with aligned columns and missing columns as nulls
master_cart_funnel as (
    select 
        account_id,
        'buy_your_trial' as funnel_type,
        original_timestamp as cta_click_timestamp,
        original_timestamp_pt as cta_click_timestamp_pt,
        'trial' as cta_click_trial_type,
        'purchase' as cta_click_cta,
        original_timestamp as buy_trial_or_modal_load_timestamp,
        original_timestamp_pt as buy_trial_or_modal_load_timestamp_pt,
        null as offer_id,
        null as plan_name,
        null as preview_state,
        null as modal_auto_load_or_cta,
        null as buy_now_timestamp,
        null as see_all_plans_timestamp,
        null as plan_lineup_support_timestamp,
        null as ppv_support_timestamp,
        null as pps_support_timestamp,
        null as plan_lineup_suite_timestamp,
        null as ppv_suite_timestamp,
        null as pps_suite_timestamp,
        --- PPV/PPS for buy your trial
        ppv_timestamp as buy_trial_ppv_timestamp,
        pps_timestamp as buy_trial_pps_timestamp,
        --- Payment visits or submit, separated for buy now/see all plans
        null as modal_buy_now_ppv_timestamp,
        null as modal_buy_now_pps_timestamp,
        null as modal_see_all_plans_ppv_timestamp,
        null as modal_see_all_plans_pps_timestamp
    from buy_your_trial_payment_submit

    union all

    select 
        --- Account id from either modal load or from CTA click. To include compare plan clicks 
        --- with no modal load
        coalesce(compare_plans_and_auto_trigger_.account_id, compare_plans_and_auto_trigger_.cta_click_account_id) as account_id,
        'modal_loads' as funnel_type,
        compare_plans_and_auto_trigger_.cta_click_timestamp,
        compare_plans_and_auto_trigger_.cta_click_timestamp_pt,
        compare_plans_and_auto_trigger_.cta_click_trial_type,
        compare_plans_and_auto_trigger_.cta_click_cta,
        coalesce(compare_plans_and_auto_trigger_.original_timestamp, compare_plans_and_auto_trigger_.cta_click_timestamp) as original_timestamp,
        coalesce(compare_plans_and_auto_trigger_.original_timestamp_pt, compare_plans_and_auto_trigger_.cta_click_timestamp_pt) as original_timestamp_pt,
        modal_buy_now.offer_id,
        modal_buy_now.plan_name,
        modal_buy_now.preview_state,
        modal_buy_now.source as modal_auto_load_or_cta,
        modal_buy_now.buy_now_timestamp,
        modal_see_all.see_all_plans_timestamp,
        modal_see_all.plan_lineup_support_timestamp,
        modal_see_all.ppv_support_timestamp,
        modal_see_all.pps_support_timestamp,
        modal_see_all.plan_lineup_suite_timestamp,
        modal_see_all.ppv_suite_timestamp,
        modal_see_all.pps_suite_timestamp,
        --- PPV/PPS for buy your trial
        null as buy_trial_ppv_timestamp,
        null as buy_trial_pps_timestamp,
        --- Payment visits or submit, separated for buy now/see all plans
        modal_buy_now.ppv_timestamp as modal_buy_now_ppv_timestamp,
        modal_buy_now.pps_timestamp as modal_buy_now_pps_timestamp,
        modal_see_all.ppv_timestamp as modal_see_all_plans_ppv_timestamp,
        modal_see_all.pps_timestamp as modal_see_all_plans_pps_timestamp
    --- Joining to the universe of customers (compare plans/auto trigger) the
    --- Buy now or see all plans funnels
    from compare_plans_and_auto_trigger compare_plans_and_auto_trigger_
    left join modal_load_buy_now_payment_submit modal_buy_now
        on compare_plans_and_auto_trigger_.account_id = modal_buy_now.account_id
        and compare_plans_and_auto_trigger_.original_timestamp = modal_buy_now.original_timestamp
    left join modal_see_all_plans_payment_submit_joined modal_see_all
        on compare_plans_and_auto_trigger_.account_id = modal_see_all.account_id
        and compare_plans_and_auto_trigger_.original_timestamp = modal_see_all.original_timestamp
),

----------------------------------------------------------------------
--- Final step: Join master funnel with wins & determine win path

master_cart_funnel_wins as (
    select 
        master_cart_funnel_.*,
        --- Relevant fields
        accounts_.instance_account_created_date,
        accounts_.crm_account_id,
        accounts_.win_date,
        accounts_.is_won,
        accounts_.is_won_arr,
        accounts_.is_won_ss,
        accounts_.is_won_ss_arr,
        accounts_.region,
        accounts_.help_desk_size_grouped,
        accounts_.seats_capacity_band_at_win,
    from master_cart_funnel master_cart_funnel_
    left join accounts accounts_
        on master_cart_funnel_.account_id = accounts_.instance_account_id
),

win_attribution as (
    select 
        account_id,
        buy_trial_or_modal_load_timestamp
    from master_cart_funnel_wins
    where 
        is_won = 1
        and (date_trunc('day', buy_trial_pps_timestamp) <= win_date or
        date_trunc('day', modal_buy_now_pps_timestamp) <= win_date or
        date_trunc('day', modal_see_all_plans_pps_timestamp) <= win_date)
    qualify row_number() over (partition by account_id order by buy_trial_or_modal_load_timestamp desc) = 1
),

master_cart_funnel_wins_attribution as (
    select 
        master_cart.*,
        --- Attribution field
        case 
            when win_attr.account_id is not null and master_cart.funnel_type = 'buy_your_trial' 
            then 'buy_your_trial_win'
            when 
                win_attr.account_id is not null and master_cart.funnel_type != 'buy_your_trial' 
                and coalesce(master_cart.modal_buy_now_pps_timestamp, '1900-01-01 00:00:00') > coalesce(master_cart.modal_see_all_plans_pps_timestamp, '1900-01-01 00:00:00')
            then 'modal_buy_now_win'
            when 
                win_attr.account_id is not null and master_cart.funnel_type != 'buy_your_trial' 
                and coalesce(master_cart.modal_buy_now_pps_timestamp, '1900-01-01 00:00:00') < coalesce(master_cart.modal_see_all_plans_pps_timestamp, '1900-01-01 00:00:00')
            then 'modal_see_all_plans_win'
            when master_cart.win_date is not null and win_attr_submit.account_id is null 
            then 'won_outside_agent_home'
            --- No customer should be categorized as this one. Including it to validate
            when win_attr.account_id is not null 
            then 'error - other_win'
            when master_cart.win_date is null 
            then 'not_won'
            else 'not_last_submit'
        end as win_attribution_flag
    from master_cart_funnel_wins master_cart
    left join win_attribution win_attr
        on master_cart.account_id = win_attr.account_id
        and master_cart.buy_trial_or_modal_load_timestamp = win_attr.buy_trial_or_modal_load_timestamp
    left join (select distinct account_id from win_attribution) win_attr_submit
        on master_cart.account_id = win_attr_submit.account_id
)



/* 
TOT_OBS	CTA_CLICK_ACCOUNT_ID	MODAL_LOADS
4640	3287	3218
*/
select 
    cta_click_cta,
    count(*) tot_obs,
    count(distinct account_id) as account_id,
    count(distinct case when modal_auto_load_or_cta is not null then account_id else null end) modal_loads
from master_cart_funnel_wins_attribution
group by 1






select
    win_attribution_flag,
    count(*) as total_wins,
    count(distinct account_id) as total_won_accounts
from master_cart_funnel_wins_attribution
group by 1





select
    count(distinct case when is_won = 1 then account_id else null end) as wins
from master_cart_funnel_wins





/*
TOT_OBS	TOTAL_ACCOUNTS	TO_SEE_ALL_PLANS	TO_PLAN_LINEUP	TO_PLAN_LINEUP_SUPPORT	TO_PLAN_LINEUP_SUITE	TO_PAYMENT_PAGE	TO_PAYMENT_PAGE_SUPPORT	TO_PAYMENT_PAGE_SUITE	TO_PAYMENT_SUBMIT	TO_PAYMENT_SUBMIT_SUPPORT	TO_PAYMENT_SUBMIT_SUITE
13883	6555	1702	1594	864	1570	397	161	251	238	113	125
*/

select 
    funnel_type,
    modal_auto_load_or_cta,
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    min(buy_trial_or_modal_load_timestamp) as events_since,
    --- Cart visits, CTA or auto trigger & interacted
    count(
        distinct 
            case 
                when (funnel_type = 'buy_your_trial' or modal_auto_load_or_cta = 'CTA') or 
                (modal_auto_load_or_cta = 'auto_trigger' and (buy_now_timestamp is not null or see_all_plans_timestamp is not null))
                then account_id else null 
                end
                ) as cart_visit,
    --- For modal loads, buy now or see all plans clicks
    count(distinct case when buy_now_timestamp is not null then account_id else null end) as to_buy_now,
    count(distinct case when see_all_plans_timestamp is not null then account_id else null end) as to_see_all_plans,
    --- For see all plan clicks, # of plan lineup loads (support or suite)
    count(distinct case when plan_lineup_support_timestamp is not null or plan_lineup_suite_timestamp is not null then account_id else null end) as to_plan_lineup,
    count(distinct case when buy_trial_ppv_timestamp is not null then account_id else null end) as to_payment_page_buy_trial,
    count(distinct case when modal_buy_now_ppv_timestamp is not null then account_id else null end) as to_payment_page_buy_now,
    count(distinct case when modal_see_all_plans_ppv_timestamp is not null then account_id else null end) as to_payment_page_see_all_plans,
    --count(
    --    distinct 
    --        case 
    --            when buy_trial_pps_timestamp is not null or 
    --            modal_buy_now_pps_timestamp is not null or 
    --            modal_see_all_plans_pps_timestamp is not null then account_id else null end) as to_payment_submit,
    count(distinct case when buy_trial_pps_timestamp is not null then account_id else null end) as to_payment_submit_buy_trial,
    count(distinct case when modal_buy_now_pps_timestamp is not null then account_id else null end) as to_payment_submit_buy_now,
    count(distinct case when modal_see_all_plans_pps_timestamp is not null then account_id else null end) as to_payment_submit_see_all_plans,
    --- Payment submits see all plans, support or suite
    --count(distinct case when pps_support_timestamp is not null then account_id else null end) as to_payment_submit_support,
    --count(distinct case when pps_suite_timestamp is not null then account_id else null end) as to_payment_submit_suite,
    count(distinct case when win_attribution_flag = 'buy_your_trial_win' then account_id else null end) as buy_your_trial_win,
    count(distinct case when win_attribution_flag = 'modal_buy_now_win' then account_id else null end) as modal_buy_now_win,
    count(distinct case when win_attribution_flag = 'modal_see_all_plans_win' then account_id else null end) as modal_see_all_plans_win

from master_cart_funnel_wins_attribution
--where funnel_type != 'buy_your_trial'
group by all
order by 1,2,3






select *
from master_cart_funnel_wins_attribution
where 
    funnel_type != 'buy_your_trial' 
    and (buy_now_timestamp is not null or see_all_plans_timestamp is not null)
    and win_attribution_flag = 'not_last_win'
order by account_id, buy_trial_or_modal_load_timestamp










select
    --funnel_type,
    count(*) as total_wins,
    count(distinct account_id) as total_won_accounts
from win_attribution
--group by 1





select *
from max_submit
--where account_id = 23010145













select 
    funnel_type,
    count(*) tot_obs,
    count(distinct account_id) account_id,
    count(distinct case when is_won = 1 then account_id else null end) as wins,
    count(distinct case when is_won = 1 and (
        buy_trial_pps_timestamp is not null or 
        modal_buy_now_pps_timestamp is not null or 
        modal_see_all_plans_pps_timestamp is not null
    ) then account_id else null end) as wins_submit
from master_cart_funnel_wins
group by 1



select *
from master_cart_funnel_wins
where account_id = 23010145
order by account_id, original_timestamp



select *
from win_attribution
order by account_id, original_timestamp





--- Validate whole funnel


/*
TOT_OBS	TOTAL_ACCOUNTS	TO_SEE_ALL_PLANS	TO_PLAN_LINEUP	TO_PLAN_LINEUP_SUPPORT	TO_PLAN_LINEUP_SUITE	TO_PAYMENT_PAGE	TO_PAYMENT_PAGE_SUPPORT	TO_PAYMENT_PAGE_SUITE	TO_PAYMENT_SUBMIT	TO_PAYMENT_SUBMIT_SUPPORT	TO_PAYMENT_SUBMIT_SUITE
13883	6555	1702	1594	864	1570	397	161	251	238	113	125
*/

select 
    funnel_type,
    --modal_auto_load_or_cta,
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    min(buy_trial_or_modal_load_timestamp) as events_since,
    --- Cart visits, CTA or auto trigger & interacted
    count(
        distinct 
            case 
                when (funnel_type = 'buy_your_trial' or modal_auto_load_or_cta = 'CTA') or 
                (modal_auto_load_or_cta = 'auto_trigger' and (buy_now_timestamp is not null or see_all_plans_timestamp is not null))
                then account_id else null 
                end
                ) as cart_visit,
    --- For modal loads, buy now or see all plans clicks
    count(distinct case when buy_now_timestamp is not null then account_id else null end) as to_buy_now,
    count(distinct case when see_all_plans_timestamp is not null then account_id else null end) as to_see_all_plans,
    --- For see all plan clicks, # of plan lineup loads (support or suite)
    count(distinct case when plan_lineup_support_timestamp is not null or plan_lineup_suite_timestamp is not null then account_id else null end) as to_plan_lineup,
    count(distinct case when plan_lineup_support_timestamp is not null then account_id else null end) as to_plan_lineup_support,
    count(distinct case when plan_lineup_suite_timestamp is not null then account_id else null end) as to_plan_lineup_suite,
    --- Payment page visits
    --count(
    --    distinct 
    --        case 
    --            when buy_trial_ppv_timestamp is not null or 
    --            modal_buy_now_ppv_timestamp is not null or 
    --            modal_see_all_plans_ppv_timestamp is not null then account_id else null end) as to_payment_page,
    count(distinct case when buy_trial_ppv_timestamp is not null then account_id else null end) as to_payment_page_buy_trial,
    count(distinct case when modal_buy_now_ppv_timestamp is not null then account_id else null end) as to_payment_page_buy_now,
    count(distinct case when modal_see_all_plans_ppv_timestamp is not null then account_id else null end) as to_payment_page_see_all_plans,
    --- Payment page visits see all plans, support or suite
    count(distinct case when ppv_support_timestamp is not null then account_id else null end) as to_payment_page_support,
    count(distinct case when ppv_suite_timestamp is not null then account_id else null end) as to_payment_page_suite,
    count(
        distinct 
            case 
                when buy_trial_pps_timestamp is not null or 
                modal_buy_now_pps_timestamp is not null or 
                modal_see_all_plans_pps_timestamp is not null then account_id else null end) as to_payment_submit,
    count(distinct case when buy_trial_pps_timestamp is not null then account_id else null end) as to_payment_submit_buy_trial,
    count(distinct case when modal_buy_now_pps_timestamp is not null then account_id else null end) as to_payment_submit_buy_now,
    count(distinct case when modal_see_all_plans_pps_timestamp is not null then account_id else null end) as to_payment_submit_see_all_plans,
    --- Payment submits see all plans, support or suite
    count(distinct case when pps_support_timestamp is not null then account_id else null end) as to_payment_submit_support,
    count(distinct case when pps_suite_timestamp is not null then account_id else null end) as to_payment_submit_suite
from master_cart_funnel
--where funnel_type != 'buy_your_trial'
group by all
--order by 1,2











--- Some accounts from buy_your trial or modal buy now do not have a payment page visit

select *
from master_cart_funnel
where 
    funnel_type in ('buy_your_trial', 'modal_buy_now')
    and ppv_timestamp is null
order by funnel_type, account_id, original_timestamp


-- Null from buy your trial
select 
    account_id,
    original_timestamp,
    timestamp,
    session_id,
    cart_screen,
    cart_step,
    cart_type,
    origin,
    product
from cleansed.segment_billing.segment_billing_cart_loaded_scd2
where account_id = 23010145
order by original_timestamp




-- Null from buy your trial
select 
    account_id,
    original_timestamp,
    session_id,
    cart_screen,
    cart_step,
    cart_type,
    origin,
    product
from cleansed.segment_billing.segment_billing_payment_loaded_scd2
where account_id = 24354296
order by original_timestamp












--- Validating modal see all plans funnel


/*
TOT_OBS	TOTAL_ACCOUNTS	TO_SEE_ALL_PLANS	TO_PLAN_LINEUP	TO_PLAN_LINEUP_SUPPORT	TO_PLAN_LINEUP_SUITE	TO_PAYMENT_PAGE	TO_PAYMENT_PAGE_SUPPORT	TO_PAYMENT_PAGE_SUITE	TO_PAYMENT_SUBMIT	TO_PAYMENT_SUBMIT_SUPPORT	TO_PAYMENT_SUBMIT_SUITE
13883	6555	1702	1594	864	1570	397	161	251	238	113	125
*/

select 
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    count(distinct case when see_all_plans_timestamp is not null then account_id else null end) as to_see_all_plans,
    count(distinct case when plan_lineup_support_timestamp is not null or plan_lineup_suite_timestamp is not null then account_id else null end) as to_plan_lineup,
    count(distinct case when plan_lineup_support_timestamp is not null then account_id else null end) as to_plan_lineup_support,
    count(distinct case when plan_lineup_suite_timestamp is not null then account_id else null end) as to_plan_lineup_suite,
    count(distinct case when ppv_timestamp is not null then account_id else null end) as to_payment_page,
    count(distinct case when ppv_support_timestamp is not null then account_id else null end) as to_payment_page_support,
    count(distinct case when ppv_suite_timestamp is not null then account_id else null end) as to_payment_page_suite,
    count(distinct case when pps_timestamp is not null then account_id else null end) as to_payment_submit,
    count(distinct case when pps_support_timestamp is not null then account_id else null end) as to_payment_submit_support,
    count(distinct case when pps_suite_timestamp is not null then account_id else null end) as to_payment_submit_suite
from modal_see_all_plans_payment_submit_joined
--where source = 'auto_trigger'







select *
from modal_see_all_plans_payment_submit_joined
where 
    --see_all_plans_timestamp is not null
    --and plan_lineup_support_timestamp is null 
    --and plan_lineup_suite_timestamp is null
    --ppv_timestamp is not null
    source = 'auto_trigger'
    and pps_timestamp is not null
order by account_id, original_timestamp









--- No success "TRUE" values
select
    SUCCESS,
    count(*)
from CLEANSED.SEGMENT_BILLING.SEGMENT_BILLING_UPDATE_SUBMIT_SCD2
group by 1



select *
from CLEANSED.SEGMENT_BILLING.SEGMENT_BILLING_UPDATE_SUBMIT_SCD2
where account_id = 25811817








--- Customer clicked see all plans but did not load any plan lineup

select 
    account_id,
    original_timestamp,
    session_id,
    cart_screen,
    cart_step,
    cart_type,
    origin,
    product
from cleansed.segment_billing.segment_billing_cart_loaded_scd2
where account_id = 25798621
order by original_timestamp


select
    origin,
    count(*)
from cleansed.segment_billing.segment_billing_cart_loaded_scd2
group by 1
order by 2 desc









/*
 TOT_OBS	TOTAL_ACCOUNTS	TO_SEE_ALL_PLANS	TO_PAYMENT_PAGE	TO_PAYMENT_PAGE_SUPPORT	TO_PAYMENT_PAGE_SUITE
13883	6555	1681	1581	832	1558
 */
select
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    count(distinct case when see_all_plans_timestamp is not null then account_id else null end) as to_see_all_plans,
    count(distinct case when ppv_support_timestamp is not null or ppv_suite_timestamp is not null then account_id else null end) as to_payment_page,
    count(distinct case when ppv_support_timestamp is not null then account_id else null end) as to_payment_page_support,
    count(distinct case when ppv_suite_timestamp is not null then account_id else null end) as to_payment_page_suite
from modal_load_see_all_plans_payment_visit_suite











select 
    cta_name,
    product_cta,
    count(*)
from payment_submission
group by 1,2
order by 1,2







--- Validating modal buy now funnel


/*
TOT_OBS	TOTAL_ACCOUNTS	TO_BUY_NOW	TO_PAYMENT_PAGE	TO_PAYMENT_SUBMIT
13883	6555	340	270	163
*/
select 
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    count(distinct case when buy_now_timestamp is not null then account_id else null end) as to_buy_now,
    count(distinct case when ppv_timestamp is not null then account_id else null end) as to_payment_page,
    count(distinct case when pps_timestamp is not null then account_id else null end) as to_payment_submit
from modal_load_buy_now_payment_submit
--where source = 'auto_trigger'


/*
TOT_OBS	TOTAL_ACCOUNTS	TO_BUY_NOW
13883	6555	340
*/
select 
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    count(distinct case when buy_now_timestamp is not null then account_id else null end) as to_buy_now
from modal_load_buy_now_click


/*
TOT_OBS	TOTAL_ACCOUNTS
13883	6555
*/
select 
    count(*) tot_obs,
    count(distinct account_id) as total_accounts
from modal_load























--- Validating buy your trial funnel
select 
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    count(distinct case when ppv_timestamp is not null then account_id else null end) as to_payment_page,
    count(distinct case when pps_timestamp is not null then account_id else null end) as to_payment_submit
from buy_your_trial_payment_submit

select 
    count(*) tot_obs,
    count(distinct account_id) as total_accounts
from cta_click
where cta = 'purchase'









--- Validate see all plans funnel using Agus's table
--- Step 0: Import relevant fields from trial accounts
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.crm_account_id,
        --- Overall Win data
        trial_accounts.win_date,
        case when trial_accounts.win_date is not null then 1 else null end as is_won,
        case when trial_accounts.win_date is not null then instance_account_arr_usd_at_win else null end as is_won_arr,
        --- SS wins
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won_ss,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_ss_arr,
        --- Trials extra info
        trial_accounts.region,
        trial_accounts.help_desk_size_grouped,
        trial_accounts.instance_account_created_date,
        trial_accounts.seats_capacity_band_at_win,
    from presentation.growth_analytics.trial_accounts trial_accounts 
),

modal_load as (
    select distinct
        load.original_timestamp,
        convert_timezone('UTC', 'America/Los_Angeles', load.original_timestamp) original_timestamp_pt,
        account_id,
        offer_id,
        plan_name,
        preview_state,
        source
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
    inner join accounts a
        on load.account_id = a.instance_account_id
),

joined_e2b as (
    select 
        modal_load_.*,
        trial_cart.* exclude(account_id)
    from modal_load modal_load_
    left join PRESENTATION.GROWTH_ANALYTICS.TRIAL_SHOPPING_CART_FUNNEL trial_cart
        on modal_load_.account_id = trial_cart.account_id
)

select 
    count(*) tot_obs,
    count(distinct modal_load_.account_id) as total_accounts,
    count(distinct case when first_cart_visit is not null then modal_load_.account_id else null end) as to_cart_visit,
    count(distinct case when first_all_plans_cta_ts is not null then modal_load_.account_id else null end) as to_see_all_plans,
    count(distinct case when first_payment_page_from_suite_ts is not null or first_payment_page_from_support_ts is not null then modal_load_.account_id else null end) as to_payment_page,
    count(distinct case when first_payment_page_from_suite_ts is not null then modal_load_.account_id else null end) as payment_page_from_suite_flag,
    count(distinct case when first_payment_page_from_support_ts is not null then modal_load_.account_id else null end) as payment_page_from_support_flag,
    count(distinct case when first_payment_submit_all_plans_ts is not null then modal_load_.account_id else null end) as payment_submit_all_plans_flag
from joined_e2b modal_load_







--- Legacy code to validate some accounts with weird results
--- Found with a payment page visit flag in agus table, mainly a suite visit
--- But not on my table
--- Reason: payment page visit from suite is counting visits from buy your trial


select *
from modal_load_see_all_plans_click
where account_id = 25559803



select *
from plan_lineup pl
where pl.account_id = 25559803
    --pl.product <> 'support'
    --and pl.account_id = 25559803




select *
from payment_page_visits ppv
where 
    ppv.cta_name = 'Compare plans'
    and ppv.product_cta = 'Suite - All Plans'
    and ppv.account_id = 25559803


select 
    account_id,
    original_timestamp,
    session_id,
    cart_screen,
    cart_step,
    cart_type,
    origin,
    product
from cleansed.segment_billing.segment_billing_cart_loaded_scd2
where account_id = 25559803
order by original_timestamp


select 
    account_id,
    original_timestamp,
    session_id,
    cart_screen,
    cart_step,
    cart_type,
    origin,
    product
from cleansed.segment_billing.segment_billing_payment_loaded_scd2
where account_id = 25559803
order by original_timestamp


select *
from modal_see_all_plans_payment_submit_joined
where ppv_suite_timestamp is not null





select 
    account_id,
    original_timestamp,
    session_id,
    cart_screen,
    cart_step,
    cart_type,
    origin,
    product
from cleansed.segment_billing.segment_billing_cart_loaded_scd2
where account_id = 25749104
order by original_timestamp







with payment_page_from_suite as (
    select distinct
        timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Suite - All Plans' as product_cta,
        '1' as origin_
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product <> 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Suite - All Plans' as product_cta,
        '2' as origin_
    from
        cleansed.segment_billing.segment_billing_payment_loaded_scd2
    where
        not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product <> 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Suite - All Plans' as product_cta,
        '3' as origin_
    from
        cleansed.bq_archive.billing_payment_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product <> 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
    union all
    select distinct
        timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name,
        'Suite - All Plans' as product_cta,
        '4' as origin_
    from
        cleansed.bq_archive.billing_cart_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        and cart_screen <> 'preset_trial_plan'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        and product <> 'support'
        --- Removing central admin & expired trial events
        and origin not in ('central_admin', 'expired-trial')
)

select *
from payment_page_from_suite
where account_id = 25559803





------------------------------------------------------
--- Comparing buy your trial segment events
--- Cart loaded vs growth engine
--- GE reports far less events than 


--- Growth engine - only buy your trial clicks
select 
    date_trunc('day', original_timestamp) as event_date,
    count(*) as tot_obs,
    count(distinct account_id) as total_accounts,
    min(original_timestamp) as first_event,
    max(original_timestamp) as last_event
from cleansed.segment_support.growth_engine_trial_cta_1_scd2 cta1
inner join presentation.growth_analytics.trial_accounts trials
    on trials.instance_account_id = cta1.account_id
where cta = 'purchase'
group by 1
order by 1 




--- Growth engine - including buy now clicks
with buy_trial_buy_now as (
    select cta1.account_id, cta1.original_timestamp
    from cleansed.segment_support.growth_engine_trial_cta_1_scd2 cta1
    inner join presentation.growth_analytics.trial_accounts trials
        on trials.instance_account_id = cta1.account_id
    where cta1.cta = 'purchase'

    union all 

    select buy_now.account_id, buy_now.original_timestamp
    from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 buy_now
    inner join presentation.growth_analytics.trial_accounts trials
        on trials.instance_account_id = buy_now.account_id
)

select 
    date_trunc('day', original_timestamp) as event_date,
    count(*) as tot_obs,
    count(distinct account_id) as total_accounts,
    min(original_timestamp) as first_event,
    max(original_timestamp) as last_event
from buy_trial_buy_now
where original_timestamp >= '2025-08-01'
group by 1
order by 1 




--- Cart loaded
with payment_page_from_trial_buy as (
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Buy Trial Plan' as cta_name,
        'Suite - Buy Trial' as product_cta
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        --and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_screen = 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        --and origin not in ('central_admin', 'expired-trial')
        and origin like '%trial_welcome%'
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Buy Trial Plan' as cta_name,
        'Suite - Buy Trial' as product_cta
    from
        cleansed.segment_billing.segment_billing_payment_loaded_scd2
    where
        not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        --and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_screen = 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        --and origin not in ('central_admin', 'expired-trial')
        and origin like '%trial_welcome%'
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Buy Trial Plan' as cta_name,
        'Suite - Buy Trial' as product_cta
    from
        cleansed.bq_archive.billing_payment_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        --and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_screen = 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        --and origin not in ('central_admin', 'expired-trial')
        and origin like '%trial_welcome%'
    union all
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        --- Adjusted Agus's query. Was compare plans before
        'Buy Trial Plan' as cta_name,
        'Suite - Buy Trial' as product_cta
    from
        cleansed.bq_archive.billing_cart_loaded
    where
        date(timestamp) >= '2024-01-01'
        and not paid_customer
        --- Adjusted cart screen to match what I observed in segment
        --- Added buy your trial
        --and cart_screen in ('preset_trial_plan', 'buy_your_trial')
        and cart_screen = 'buy_your_trial'
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
        --- Removing central admin & expired trial events
        --and origin not in ('central_admin', 'expired-trial')
        and origin like '%trial_welcome%'
)


select 
    date_trunc('day', timestamp) as event_date,
    count(*) as tot_obs,
    count(distinct account_id) as total_accounts,
    min(timestamp) as first_event,
    max(timestamp) as last_event
from payment_page_from_trial_buy pp
inner join presentation.growth_analytics.trial_accounts trials
    on trials.instance_account_id = pp.account_id
where timestamp >= '2025-08-01'
group by 1
order by 1 







select 
    win_attribution_flag,
    count(*) as total_wins,
    count(distinct account_id) as total_won_accounts
from sandbox.juan_salgado.cart_funnel_session
group by 1








--- Compare plan clicks into modal load

--- Step 0: Import relevant fields from trial accounts
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.crm_account_id,
        --- Overall Win data
        trial_accounts.win_date,
        case when trial_accounts.win_date is not null then 1 else null end as is_won,
        case when trial_accounts.win_date is not null then instance_account_arr_usd_at_win else null end as is_won_arr,
        --- SS wins
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won_ss,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_ss_arr,
        --- Trials extra info
        trial_accounts.region,
        trial_accounts.help_desk_size_grouped,
        trial_accounts.instance_account_created_date,
        trial_accounts.seats_capacity_band_at_win,
    from presentation.growth_analytics.trial_accounts trial_accounts 
),

--- Step 1: count interactions with each modal step
--- Inner join to trial accounts to remove testing accounts

cta_click as (
    select distinct
        cta_click.original_timestamp,
        convert_timezone('UTC', 'America/Los_Angeles', cta_click.original_timestamp) original_timestamp_pt,
        cta_click.account_id,
        cta_click.trial_type,
        cta_click.cta,
    from
        cleansed.segment_support.growth_engine_trial_cta_1_scd2 cta_click
    inner join accounts a
        on cta_click.account_id = a.instance_account_id
),

modal_load as (
    select distinct
        load.original_timestamp,
        convert_timezone('UTC', 'America/Los_Angeles', load.original_timestamp) original_timestamp_pt,
        account_id,
        offer_id,
        plan_name,
        preview_state,
        source
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
    inner join accounts a
        on load.account_id = a.instance_account_id
),

compare_plans_into_modal_load as (
    --- Accounts with compare plans clicks & modal load
    select
        cta_click_.original_timestamp as cta_click_timestamp,
        cta_click_.original_timestamp_pt as cta_click_timestamp_pt,
        cta_click_.account_id as cta_click_account_id,
        cta_click_.trial_type as cta_click_trial_type,
        cta_click_.cta as cta_click_cta,
        modal_load_.*
    from cta_click cta_click_
    left join modal_load modal_load_
        on cta_click_.account_id = modal_load_.account_id
        and modal_load_.original_timestamp >= cta_click_.original_timestamp
        and datediff(minute, cta_click_.original_timestamp, modal_load_.original_timestamp) <= 120
        --- Modal loads from compare plans clicks
        and modal_load_.source = 'CTA'
    --- Just compare plans clicks
    where cta_click_.cta = 'compare'
    qualify row_number() over (partition by cta_click_.account_id, cta_click_.original_timestamp order by modal_load_.original_timestamp) = 1
),


compare_plans_into_modal_load_v2 as (
    --- Accounts with compare plans clicks & modal load
    select
        cta_click_.original_timestamp as cta_click_timestamp,
        cta_click_.original_timestamp_pt as cta_click_timestamp_pt,
        cta_click_.account_id as cta_click_account_id,
        cta_click_.trial_type as cta_click_trial_type,
        cta_click_.cta as cta_click_cta,
        modal_load_.*
    from cta_click cta_click_
    left join modal_load modal_load_
        on cta_click_.account_id = modal_load_.account_id
        and modal_load_.original_timestamp >= cta_click_.original_timestamp
        and datediff(minute, cta_click_.original_timestamp, modal_load_.original_timestamp) <= 120
        --- Modal loads from compare plans clicks
        and modal_load_.source = 'CTA'
    --- Just compare plans clicks
    where cta_click_.cta = 'compare'
    qualify row_number() over (partition by cta_click_.account_id, cta_click_.original_timestamp order by modal_load_.original_timestamp) = 1

    union all

    --- Accounts with modal load, but no preceding compare plans click
    select
        --- Inputting nulls for compare plans clicks
        modal_load_.original_timestamp as cta_click_timestamp,
        modal_load_.original_timestamp_pt as cta_click_timestamp_pt,
        modal_load_.account_id as cta_click_account_id,
        'trial' as cta_click_trial_type,
        'compare' as cta_click_cta,
        modal_load_.*,
    from modal_load modal_load_
    left join cta_click cta_click_
        on cta_click_.account_id = modal_load_.account_id
        and modal_load_.original_timestamp >= cta_click_.original_timestamp
        and datediff(minute, cta_click_.original_timestamp, modal_load_.original_timestamp) <= 120
        and cta_click_.cta = 'compare'
    where 
        modal_load_.source = 'CTA'
        and cta_click_.account_id is null
)


/* 
TOT_OBS	CTA_CLICK_ACCOUNT_ID	MODAL_LOADS
4640	3287	3218
*/
select 
    count(*) tot_obs,
    count(distinct account_id) as account_id,
    count(distinct cta_click_account_id) as cta_click_account_id,
    count(distinct case when source is not null then cta_click_account_id else null end) modal_loads
from compare_plans_into_modal_load_v2





/* 
TOT_OBS	CTA_CLICK_ACCOUNT_ID	MODAL_LOADS
2878	2021	1947
*/
select 
    count(*) tot_obs,
    count(distinct cta_click_account_id) as cta_click_account_id,
    count(distinct case when account_id is not null then cta_click_account_id else null end) modal_loads
from compare_plans_into_modal_load







/* 
TOT_OBS	ACCOUNT_ID
2878	2021
*/
select 
    count(*) tot_obs,
    count(distinct account_id) as account_id
from cta_click
where cta = 'compare'




select *
from sandbox.juan_salgado.cart_funnel_session
where 
    cta_click_cta = 'compare'
    and modal_loaded_flag is null





select 
    count(distinct compare_clicked_flag)
from sandbox.juan_salgado.cart_funnel_session
where 
    cta_click_cta = 'compare'
    and modal_loaded_flag is null




/* 
TOT_OBS	CTA_CLICK_ACCOUNT_ID	MODAL_LOADS
4640	3287	3218
*/
select 
    cta_click_cta,
    count(*) tot_obs,
    count(distinct account_id) as account_id,
    count(distinct case when modal_auto_load_or_cta is not null then account_id else null end) modal_loads
from sandbox.juan_salgado.cart_funnel_session
group by 1





/* 
TOT_OBS	CTA_CLICK_ACCOUNT_ID	MODAL_LOADS
4640	3287	3218
*/
select 
    count(*) tot_obs,
    count(distinct account_id) as account_id,
    count(distinct case when modal_auto_load_or_cta is not null then account_id else null end) modal_loads
from sandbox.juan_salgado.cart_funnel_session




select 
    count(*) tot_obs,
    count(distinct account_id) as account_id,
from sandbox.juan_salgado.cart_funnel_session
where buy_trial_or_modal_load_timestamp is null




select 
    modal_auto_load_or_cta,
    count(*) tot_obs,
    count(distinct account_id) as account_id,
from sandbox.juan_salgado.cart_funnel_session
where cta_click_timestamp is null
group by 1

