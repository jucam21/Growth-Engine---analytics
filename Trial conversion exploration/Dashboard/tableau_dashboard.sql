-----------------------------------------------
--- Work on new Tableau dashboard



----------------------------------------------------
--- 1.1.1: Uncohorted funnel - bookings


--- Adjusting query to use bookings

--- All trial accounts

with bookings as (
    select 
        pro_forma_signature_date,
        crm_account_id,
        crm_opportunity_id,
        total_booking_arr_usd
        --sum(total_booking_arr_usd) as total_booking_arr_usd
    from functional.finance.sfa_crm_bookings_current
    where
        pro_forma_market_segment_at_close_date = 'Digital'
        and sales_motion = 'Online'
        and type = 'New Business'
        --and pro_forma_signature_date >= '2025-05-01'
    --group by 1,2
),

--- User ever clicked on "buy now" or "see all plans" modals in the past
modal_buy_now as (
    select
        mapping.crm_account_id,
        buy_now.account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 buy_now
    --- Remove testing accounts
    inner join presentation.growth_analytics.trial_accounts trial_accounts  
        on 
            trial_accounts.instance_account_id = buy_now.account_id
            and date(buy_now.original_timestamp) <= trial_accounts.win_date
    --- Join CRM
    left join foundational.customer.entity_mapping_daily_snapshot mapping
        on 
            buy_now.account_id = mapping.instance_account_id
            --and date(buy_now.original_timestamp) = mapping.source_snapshot_date
    group by all
),

modal_see_all_plans as (
    select
        mapping.crm_account_id,
        see_all_plans.account_id,
        trial_accounts.win_date,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2 see_all_plans
    --- Remove testing accounts
    inner join presentation.growth_analytics.trial_accounts trial_accounts
        on
            trial_accounts.instance_account_id = see_all_plans.account_id
            and date(see_all_plans.original_timestamp) <= trial_accounts.win_date
    --- Join CRM
    left join foundational.customer.entity_mapping_daily_snapshot mapping
        on 
            see_all_plans.account_id = mapping.instance_account_id
            --and date(see_all_plans.original_timestamp) = mapping.source_snapshot_date
    group by all
),

--- Last billing cart loaded event
billing_cart_loaded as (
    select
        mapping.crm_account_id,
        cart_screen,
        cart_step,
        cart_version,
        origin,
        cart_type,
        date(original_timestamp) as max_date
    from 
        cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing_cart_loaded
    --- Remove testing accounts
    inner join presentation.growth_analytics.trial_accounts trial_accounts  
        on 
            trial_accounts.instance_account_id = billing_cart_loaded.account_id
            and date(billing_cart_loaded.original_timestamp) <= trial_accounts.win_date
            and date(billing_cart_loaded.original_timestamp) >= dateadd('day', -15, trial_accounts.win_date)
    --- CRM
    left join foundational.customer.entity_mapping_daily_snapshot mapping
        on 
            billing_cart_loaded.account_id = mapping.instance_account_id
            and date(billing_cart_loaded.original_timestamp) = mapping.source_snapshot_date
    where paid_customer = FALSE
    qualify row_number() over (partition by mapping.crm_account_id order by billing_cart_loaded.original_timestamp desc) = 1
),

--- Join all data
bookings_daily as (
    select
        bookings_.pro_forma_signature_date,
        billing_cart_loaded_.cart_screen,
        billing_cart_loaded_.cart_step,
        billing_cart_loaded_.cart_version,
        case 
            when billing_cart_loaded_.origin is null then 'No cart event' 
            when billing_cart_loaded_.origin in ('trial_welcome_screen', 'direct', 'expired-trial', 'central_admin') then billing_cart_loaded_.origin
            else 'other origin' 
        end as origin,
        billing_cart_loaded_.cart_type,
        billing_cart_loaded_.max_date as max_date_billing_cart_loaded,
        count(*) as total_wins,
        count(distinct bookings_.crm_account_id) as unique_crm_wins,
        sum(bookings_.total_booking_arr_usd) as total_booking_arr_usd,
        
        --- Total wins count
        --- At the opportunity level
        count(case
            when
                modal_buy_now_.crm_account_id is not null
                and modal_see_all_plans_.crm_account_id is null
                then bookings_.crm_account_id else null
        end) as total_bookings_just_buy_now,
        count(case
            when
                modal_buy_now_.crm_account_id is null
                and modal_see_all_plans_.crm_account_id is not null
                then bookings_.crm_account_id else null
        end) as total_bookings_just_see_all_plans,
        count(case
            when
                modal_buy_now_.crm_account_id is not null
                and modal_see_all_plans_.crm_account_id is not null
                then bookings_.crm_account_id else null
        end) as total_bookings_both,
        count(case
            when
                modal_buy_now_.crm_account_id is null
                and modal_see_all_plans_.crm_account_id is null
                then bookings_.crm_account_id else null
        end) as total_bookings_none,

        --- Bookings ARR
        sum(case
            when
                modal_buy_now_.crm_account_id is not null
                and modal_see_all_plans_.crm_account_id is null
                then bookings_.total_booking_arr_usd else null
        end) as total_bookings_just_buy_now_arr,
        sum(case
            when
                modal_buy_now_.crm_account_id is null
                and modal_see_all_plans_.crm_account_id is not null
                then bookings_.total_booking_arr_usd else null
        end) as total_bookings_just_see_all_plans_arr,
        sum(case
            when
                modal_buy_now_.crm_account_id is not null
                and modal_see_all_plans_.crm_account_id is not null
                then bookings_.total_booking_arr_usd else null
        end) as total_bookings_both_arr,
        sum(case
            when
                modal_buy_now_.crm_account_id is null
                and modal_see_all_plans_.crm_account_id is null
                then bookings_.total_booking_arr_usd else null
        end) as total_bookings_none_arr
        
    from bookings bookings_
    left join modal_buy_now modal_buy_now_
        on bookings_.crm_account_id = modal_buy_now_.crm_account_id
    left join modal_see_all_plans modal_see_all_plans_
        on bookings_.crm_account_id = modal_see_all_plans_.crm_account_id
    left join billing_cart_loaded billing_cart_loaded_
        on bookings_.crm_account_id = billing_cart_loaded_.crm_account_id
    where
        bookings_.pro_forma_signature_date >= '2025-05-01'
    group by all
)


select *, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) as updated_at
from bookings_daily














select 
    count(*) tot_obs,
    count(distinct account_id) account_id,
    sum(case when crm_account_id is null then 1 else null end) no_crm_account_id
from modal_see_all_plans




select *
from modal_see_all_plans
where crm_account_id is null
limit 10



--2025-09-26


select *
from foundational.customer.entity_mapping_daily_snapshot
where instance_account_id = 25757522
order by source_snapshot_date 
limit 10








