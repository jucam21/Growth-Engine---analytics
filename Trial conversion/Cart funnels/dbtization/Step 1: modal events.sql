--- Union all modal events

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

--- Union all modal events
all_modal_events_tmp as (
    select
        'cta_click' as event_type,
        original_timestamp,
        original_timestamp_pt,
        account_id,
        null as offer_id,
        null as plan_name,
        null as preview_state,
        null as source,
        trial_type,
        cta,
        null as promo_code
    from cta_click

    union all

    select
        'modal_load' as event_type,
        original_timestamp,
        original_timestamp_pt,
        account_id,
        offer_id,
        plan_name,
        preview_state,
        source,
        null as trial_type,
        null as cta,
        null as promo_code
    from modal_load

    union all

    select
        'modal_buy_now' as event_type,
        original_timestamp,
        original_timestamp_pt,
        account_id,
        null as offer_id,
        null as plan_name,
        null as preview_state,
        null as source,
        null as trial_type,
        null as cta,
        promo_code
    from modal_buy_now

    union all

    select
        'modal_see_all_plans' as event_type,
        original_timestamp,
        original_timestamp_pt,
        account_id,
        null as offer_id,
        plan_name,
        null as preview_state,
        null as source,
        null as trial_type,
        null as cta,
        null as promo_code
    from modal_see_all_plans
),

--- Some accounts with modal load, but no preceding compare plans click
--- Inputting nulls for compare plans clicks

all_modal_events as (
    select *
    from all_modal_events_tmp

    union all

    select 
        'cta_click' as event_type,
        modal_load_.original_timestamp,
        modal_load_.original_timestamp_pt,
        modal_load_.account_id,
        null as offer_id,
        null as plan_name,
        null as preview_state,
        null as source,
        'Suite' as trial_type,
        'compare' as cta,
        null as promo_code
    from all_modal_events_tmp modal_load_
    left join all_modal_events_tmp cta_click_
        on cta_click_.account_id = modal_load_.account_id
        and modal_load_.original_timestamp >= cta_click_.original_timestamp
        and datediff(minute, cta_click_.original_timestamp, modal_load_.original_timestamp) <= 120
        and cta_click_.event_type = 'cta_click'
        and cta_click_.cta = 'compare'
    where 
        modal_load_.event_type = 'modal_load'
        and cta_click_.account_id is null
)

select *
from all_modal_events


