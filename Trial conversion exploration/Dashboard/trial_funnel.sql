------------------------------------
--- Query that will populate the trial recommendation dashboard
--- Requirements: https://docs.google.com/document/d/1_mvJ3R6S7e3O5Hy7U0OUaBG0QfSiHeE3S3z-pXecHb4/edit?tab=t.0#heading=h.yoq1yjfhflrr


----------------------------------------------------
--- 1.0: Uncohorted funnel - clicks


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
        modal_load.date as loaded_date,
        modal_load.account_id,
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

select *, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) as updated_at
from segment_events_all;


--- Validating results
select 
    loaded_date,
    --- Total counts
    sum(total_count_prompt_load) as total_count_prompt_load,
    sum(total_count_modal_loads) as total_count_modal_loads,
    sum(total_count_modal_loads_cta) as total_count_modal_loads_cta,
    sum(total_count_modal_loads_auto_trigger) as total_count_modal_loads_auto_trigger,

    --- Unique counts
    count(distinct unique_count_prompt_load) as unique_count_prompt_load,
    count(distinct unique_count_modal_loads) as unique_count_modal_loads,
    count(distinct unique_count_modal_loads_cta) as unique_count_modal_loads_cta,
    count(distinct unique_count_modal_loads_auto_trigger) as unique_count_modal_loads_auto_trigger
from segment_events_all
group by loaded_date
order by loaded_date 


select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where 
    source = 'auto_trigger'
    and date(original_timestamp) = '2025-08-03'
order by original_timestamp desc
limit 20


select 
    count(*) as total_count,
    count(distinct account_id) as unique_count,
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 a
inner join presentation.growth_analytics.trial_accounts trial_accounts 
    on a.account_id = trial_accounts.instance_account_id
where 
    source = 'auto_trigger'
    and date(original_timestamp) = '2025-08-03'


--- Users without prompt load, no auto-trigger 

select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 a
inner join presentation.growth_analytics.trial_accounts trial_accounts 
    on a.account_id = trial_accounts.instance_account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_scd2 prompt_load
    on a.account_id = prompt_load.account_id
    and date(a.original_timestamp) = date(prompt_load.original_timestamp)
where 
    date(a.original_timestamp) = '2025-07-25'
    and prompt_load.account_id is null


--- Examples of those accounts. Probably are testing created by zendesk employees
--- Amy's account: https://monitor.zende.sk/accounts/25489303/overview
select *
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where account_id in (25550478, 25489303)


select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id in (25550478, 25489303)








----------------------------------------------------
--- 1.1: Uncohorted funnel - wins




--- 










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

--- User ever clicked on "buy now" or "see all plans" modals in the past
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




---- Sharing query with EDA

--- All trial accounts
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

--- User ever clicked on "buy now" or "see all plans" modals in the past
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

--- User fired billing cart loaded in the last 15 days
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
            -- 15 day timeframe last 15 days before win date
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
    billing_cart_loaded_.* exclude (account_id),
        case when billing_cart_loaded_.account_id is null then 1 else 0 end as cart_null
from accounts accounts_
left join modal_buy_now modal_buy_now_ 
    on accounts_.instance_account_id = modal_buy_now_.account_id
left join modal_see_all_plans modal_see_all_plans_ 
    on accounts_.instance_account_id = modal_see_all_plans_.account_id
left join billing_cart_loaded billing_cart_loaded_ 
    on accounts_.instance_account_id = billing_cart_loaded_.account_id
where 
    accounts_.is_won = 1
    --- Selecting only wins from Jul29
    and win_date = '2025-07-29'
order by win_date





select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 a
where account_id = 24079522




--- Checking online dashboard query

--- Numbers match.
--- Have to determine opportunity id to account id mapping
select 
    close_year,
    year_month,
    count(*) as total_count,
    sum(total_booking_arr) as total_booking_arr
from presentation.growth_analytics.growth_analytics_online_dashboard_curated_bookings
where 
    close_date >= '2025-04-01'
    and sales_motion = 'Online'
group by all
order by 1,2







--- Checking account id to CRM mapping from modal loads
--- Using onlywins, since for not all accounts will be found in the table

with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        trial_accounts.instance_account_arr_usd_at_win
    from presentation.growth_analytics.trial_accounts trial_accounts 
    where 
        trial_accounts.win_date is not null 
        and trial_accounts.sales_model_at_win <> 'Assisted'
        and trial_accounts.is_direct_buy = FALSE
)


select 
    accounts_.instance_account_id,
    mapping.crm_account_id,
    accounts_.win_date,
    accounts_.instance_account_arr_usd_at_win
from accounts accounts_
left join foundational.customer.entity_mapping_daily_snapshot as mapping
    on accounts_.instance_account_id = mapping.instance_account_id
    and accounts_.win_date = mapping.source_snapshot_date
where 
    mapping.crm_account_id = '0016R000036b0nmQAA'
    and accounts_.win_date >= '2025-05-01'



select 
    mapping.crm_account_id,
    count(*) as total_count,
    count(distinct accounts_.instance_account_id) as unique_crm_accounts
from accounts accounts_
left join foundational.customer.entity_mapping_daily_snapshot as mapping
    on accounts_.instance_account_id = mapping.instance_account_id
    and accounts_.win_date = mapping.source_snapshot_date
where accounts_.win_date >= '2025-05-01'
group by 1
order by 3 desc
limit 10


select
    count(*) as total_count,
    count(distinct accounts_.instance_account_id) as unique_accounts,
    count(distinct mapping.crm_account_id) as unique_crm_accounts,
    count(case when mapping.crm_account_id is null then 1 else null end) as null_crms
from accounts accounts_
left join foundational.customer.entity_mapping_daily_snapshot as mapping
    on accounts_.instance_account_id = mapping.instance_account_id
    and accounts_.win_date = mapping.source_snapshot_date
where accounts_.win_date >= '2025-05-01'










select 
    win_date,
    count(*) as total_count,
    sum(instance_account_arr_usd_at_win) as total_arr
from accounts
where win_date >= '2025-07-01'
group by all
order by win_date






select *
from foundational.customer.entity_mapping_daily_snapshot --._bcv
where instance_account_id = 25500148
limit 10





select *
from foundational.customer.entity_mapping_daily_snapshot --._bcv
where instance_account_id = 25500148
limit 10






select *
from functional.finance.sfa_crm_bookings_current
where 
    crm_account_id = '0016R000036b0nmQAA'
    --and pro_forma_signature_date >= '2025-05-01'
order by pro_forma_signature_date desc







select 
    date_trunc('month', pro_forma_signature_date) pro_forma_signature_date,
    count(*) total_obs,
    count(distinct crm_account_id) crm_account_id,
    sum(total_booking_arr_usd) total_booking_arr_usd
from functional.finance.sfa_crm_bookings_current
where
    pro_forma_market_segment_at_close_date = 'Digital'
    and sales_motion = 'Online'
    and type = 'New Business'
    and pro_forma_signature_date >= '2025-03-01'
group by 1
order by 1







select 
    crm_account_id,
    count(*) total_obs,
    count(distinct crm_account_id) crm_account_id,
    sum(total_booking_arr_usd) total_booking_arr_usd
from functional.finance.sfa_crm_bookings_current
where
    pro_forma_market_segment_at_close_date = 'Digital'
    and sales_motion = 'Online'
    and type = 'New Business'
    and pro_forma_signature_date >= '2025-03-01'
group by 1
order by 2 desc
limit 10



select *
from functional.finance.sfa_crm_bookings_current
where
    pro_forma_market_segment_at_close_date = 'Digital'
    and crm_account_id = '001PC00000RsL9aYAF'





--- Checking bookings vs instance accounts

with bookings as (
    select 
        date_trunc('month', pro_forma_signature_date) month_closed,
        crm_account_id,
        sum(total_booking_arr_usd) as total_booking_arr_usd
    from functional.finance.sfa_crm_bookings_current
    where
        pro_forma_market_segment_at_close_date = 'Digital'
        and sales_motion = 'Online'
        and type = 'New Business'
        and pro_forma_signature_date >= '2025-05-01'
    group by 1,2
),

mapping as (
    select
        bookings_.*,
        mapping.instance_account_id
    from bookings bookings_
    left join foundational.customer.entity_mapping_daily_snapshot mapping
        on bookings_.crm_account_id = mapping.crm_account_id
        and bookings_.month_closed = mapping.source_snapshot_date
)


select 
    month_closed,
    crm_account_id,
    count(*) as total_obs,
    count(distinct instance_account_id) as instance_account_id
from mapping
group by 1, 2
order by 3 desc
limit 10




select *
from bookings
where crm_account_id = '0018000000vWQTcAAO'




select *
from mapping
where crm_account_id = '0018000000vWQTcAAO'








--- Adjusting query to use bookings

--- All trial accounts

with bookings as (
    select 
        --date_trunc('month', pro_forma_signature_date) month_closed,
        pro_forma_signature_date month_closed,
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
            and date(buy_now.original_timestamp) = mapping.source_snapshot_date
    group by all
),

modal_see_all_plans as (
    select
        mapping.crm_account_id,
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
            and date(see_all_plans.original_timestamp) = mapping.source_snapshot_date
    group by all
)

--- Testing counts

select
    date_trunc('month', month_closed) as month_closed,
    count(*) as total_obs,
    count(distinct bookings_.crm_account_id) as unique_crm_accounts,
    sum(case when modal_buy_now_.crm_account_id is not null then 1 end) as buy_now_clicked,
    sum(case when modal_see_all_plans_.crm_account_id is not null then 1 end) as see_all_plans_clicked,
    buy_now_clicked + see_all_plans_clicked as total_cta_clicked,
    sum(total_booking_arr_usd) as total_booking_arr_usd
from bookings bookings_
left join modal_buy_now modal_buy_now_
    on bookings_.crm_account_id = modal_buy_now_.crm_account_id
left join modal_see_all_plans modal_see_all_plans_
    on bookings_.crm_account_id = modal_see_all_plans_.crm_account_id
where
    bookings_.month_closed >= '2025-05-01'
group by 1
order by 1 desc










--- User fired billing cart loaded in the last 15 days
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
            -- 15 day timeframe last 15 days before win date
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
    billing_cart_loaded_.* exclude (account_id),
        case when billing_cart_loaded_.account_id is null then 1 else 0 end as cart_null
from accounts accounts_
left join modal_buy_now modal_buy_now_ 
    on accounts_.instance_account_id = modal_buy_now_.account_id
left join modal_see_all_plans modal_see_all_plans_ 
    on accounts_.instance_account_id = modal_see_all_plans_.account_id
left join billing_cart_loaded billing_cart_loaded_ 
    on accounts_.instance_account_id = billing_cart_loaded_.account_id
where 
    accounts_.is_won = 1
    --- Selecting only wins from Jul29
    and win_date = '2025-07-29'
order by win_date






