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
        --buy_now.account_id,
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
        --see_all_plans.account_id,
        --trial_accounts.win_date,
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
            -- Last ever billing cart loaded event before win date
            --and date(billing_cart_loaded.original_timestamp) >= dateadd('day', -15, trial_accounts.win_date)
    --- CRM
    left join foundational.customer.entity_mapping_daily_snapshot mapping
        on 
            billing_cart_loaded.account_id = mapping.instance_account_id
            --and date(billing_cart_loaded.original_timestamp) = mapping.source_snapshot_date
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
),

bookings_daily_v2 as (
    select 
        bookings_.crm_account_id,
        bookings_.pro_forma_signature_date,
        case
            when
                modal_buy_now_.crm_account_id is null
                and modal_see_all_plans_.crm_account_id is null
                then bookings_.crm_account_id else null
        end as no_modal,
        case 
            when billing_cart_loaded_.max_date is null then 'No cart event' 
            else 'cart_event'
        end as cart_event,
        billing_cart_loaded_.origin
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

select *
from bookings_daily_v2
where
    date_trunc('day', pro_forma_signature_date) = '2025-09-30'
    and no_modal is not null
    --and cart_event = 'No cart event'
    and origin = 'trial_welcome_screen'
--limit 10




select 
    date_trunc('day', pro_forma_signature_date) as month_,
    count(*) tot_obs,
    count(distinct crm_account_id) tot_crm_accounts,
    sum(case when no_modal is not null then 1 else 0 end) tot_no_modal
from bookings_daily_v2
group by 1
order by 1 desc
limit 20





select *, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) as updated_at
from bookings_daily





--- CRMs without modal events
001PC00000W3pZqYAJ
001PC00000WA32NYAT
001PC00000W6wYLYAZ
001PC00000W4dVAYAZ
001PC00000W7cSqYAJ
001PC00000VMdoyYAD



select 
    source_snapshot_date,
    crm_account_id,
    instance_account_id
from foundational.customer.entity_mapping_daily_snapshot
where crm_account_id = '001PC00000W3pZqYAJ'
order by source_snapshot_date desc
limit 10



--- Has modal load but no clicks
select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id = 25677258
order by original_timestamp desc





select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    *
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
where account_id = 25677258
order by original_timestamp desc




select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    *
from cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
where account_id = 25677258
order by original_timestamp desc








--- CRMs without cart loaded event

001PC00000W86h1YAB
001PC00000GgjdqYAB
001PC00000W7OBMYA3
001PC00000W9UVtYAN
001PC00000W4wxVYAR
001PC00000W2GHjYAN
001PC00000W3OD6YAN
001PC00000W3SrtYAF
001PC00000W9ITEYA3
001PC00000VzUKZYA3
001PC00000HzDugYAF



select 
    source_snapshot_date,
    crm_account_id,
    instance_account_id
from foundational.customer.entity_mapping_daily_snapshot
where crm_account_id = '001PC00000W86h1YAB'
order by source_snapshot_date desc
limit 10





select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    date(billing_cart_loaded.original_timestamp),
    *
from cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing_cart_loaded
where 
    account_id = 25782488
    and paid_customer = FALSE
order by original_timestamp desc



select 
    instance_account_id,
    win_date
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25782488



select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id = 25782488
order by original_timestamp desc







--- CRMs wiout modal event & trial_welcome_screen origin

001PC00000W53dxYAB
001PC00000VyvBNYAZ
001PC00000W71mhYAB
001PC00000VyQ5xYAF
001PC00000OlI8bYAF
001PC00000VynwvYAB
001PC00000W4V9OYAV
001PC00000W1hWlYAJ
001PC00000W4dVAYAZ
001PC00000VypcEYAR
001PC00000W5H1CYAV
001PC00000W6LdpYAF
0011E00001kVZFEQA4
001PC00000W2YmNYAV



select 
    source_snapshot_date,
    crm_account_id,
    instance_account_id
from foundational.customer.entity_mapping_daily_snapshot
where crm_account_id = '001PC00000W53dxYAB'
order by source_snapshot_date desc
limit 10


select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id = 25773298
order by original_timestamp desc


select *
    --pro_forma_signature_date,
    --crm_account_id,
    --crm_opportunity_id,
    --total_booking_arr_usd
    ----sum(total_booking_arr_usd) as total_booking_arr_usd
from functional.finance.sfa_crm_bookings_current
where crm_account_id = '001PC00000W53dxYAB'


select
    instance_account_id,
    instance_account_created_date,
    win_date,
    sales_model_at_win
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25773298





