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


---


select 
    count(*) as total_clicks,
    count(distinct modal_buy_now.crm_account_id) as unique_crm_accounts_buy_now,
    count(distinct modal_see_all_plans.crm_account_id) as unique_crm_accounts_see_all_plans,
    sum(case when modal_buy_now.crm_account_id is not null or modal_see_all_plans.crm_account_id is not null then 1 else null end) as unique_crm_accounts_both,
from modal_buy_now
full outer join modal_see_all_plans
    on modal_buy_now.crm_account_id = modal_see_all_plans.crm_account_id








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






---- Last load of old rules


select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) date_pt,
    *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where offer_id = '01K4NAVPM48ACDKKYN2RBKTH2A'
order by original_timestamp desc
limit 10





-----------------------------------------------------
--- Modal win attribution
--- Extract maximum modal load & CTA click (buy now, see all plans)
--- For trialists before their win date, or in case win date is
--- null, the last modal load or click.

with modal_load as (
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
        --- Trial extra info
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

modal_buy_now as (
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

modal_see_all_plans as (
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
    from modal_load modal_load_
    left join modal_buy_now modal_buy_now_
        on modal_load_.account_id = modal_buy_now_.account_id
    left join modal_see_all_plans modal_see_all_plans_
        on modal_load_.account_id = modal_see_all_plans_.account_id
)

select 
    count(*) tot_obs,
    count(distinct account_id) account_id,
    sum(is_won) is_won,
    sum(is_won_ss) is_won_ss,
    sum(case when win_cta_attribution in ('Buy now click', 'See all plans click') then is_won else 0 end) as wins_cta
from modal_funnel





select 
    win_cta_attribution,
    count(*) tot_obs,
    count(distinct account_id) account_id,
    sum(is_won) is_won,
    sum(is_won_ss) is_won_ss
from modal_funnel
group by 1
order by 2 desc





select *
from modal_funnel
where account_id in (
    25566975,
    25661185,
    25042437
)




select distinct account_id
from modal_funnel
where 
    is_won_ss = 1
    and win_cta_attribution = 'See all plans click'







select 
    win_cta_attribution,
    count(*) tot_obs,
    count(distinct account_id) account_id,
    sum(is_won) is_won,
    sum(is_won_ss) is_won_ss
from modal_funnel
group by 1
order by 2 desc






select *
from modal_funnel
where 
    win_cta_attribution = 'No modal click'
    and is_won_ss = 1
limit 10






select *
from modal_funnel
where win_cta_attribution = 'Other'
limit 10






--- Review win attribution is correct
--- 6452
select 
    count(*) tot_obs,
    count(distinct account_id) account_id
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2


select *
from cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
where account_id in (
    25788303,
    25562470,
    25508687,
    25726639,
    25515012,
    25564504,
    25535813,
    25599245
)

select *
from cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
where account_id in (
    25788303,
    25562470,
    25508687,
    25726639,
    25515012,
    25564504,
    25535813,
    25599245
)


--- Validate daily loads per offer

select 
    date_trunc('day', original_timestamp) as day_,
    offer_id,
    count(*) tot_obs,
    count(distinct account_id) account_id
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where offer_id = '01K4NAVPM48ACDKKYN2RBKTH2A'
group by 1, 2
order by 1 desc, 2





select 
    count(*) tot_obs,
    count(distinct account_id) account_id
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where 
    --offer_id = '01K4NAVPM48ACDKKYN2RBKTH2A'
    offer_id is not null




--- Multiple offers per customer on a day

with counts as (
    select 
        date_trunc('day', original_timestamp) as day_,
        account_id,
        offer_id,
        count(*) tot_obs
    from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
    where offer_id is not null
    group by 1, 2, 3
),

counts_offer as (
    select 
        day_,
        account_id,
        count(*) tot_offers
    from counts
    group by 1, 2
)

select *
from counts_offer
where day_ = '2025-10-03'
order by 3 desc



select 
    account_id,
    offer_id,
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where 
    account_id = 25764091
    and date_trunc('day', original_timestamp) = '2025-10-03'



SELECT total_booking_arr_band_primary, total_booking_arr_band_secondary, COUNT(*)
FROM FUNCTIONAL.GROWTH_ANALYTICS.CURATED_BOOKINGS
GROUP BY ALL
ORDER BY 1,2

