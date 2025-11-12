--------------------------------------------------------
--- Cart funnel session query


create or replace table _sandbox_juan_salgado.public.cart_funnel_session as


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
        --is_won = 1
        is_won_ss = 1
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
),

--- Add flags for easier filtering in dashboards
master_cart_funnel_wins_attribution_flags as (
    select 
        *,
        --- Step 0: funnel ingress
        case when cta_click_cta = 'purchase' then account_id else null end as buy_your_trial_clicked_flag,
        case when cta_click_cta = 'compare' then account_id else null end as compare_clicked_flag,
        case when modal_auto_load_or_cta = 'auto_trigger' then account_id else null end as modal_auto_trigger_load_flag,
        --- Step 1: modal loads
        case when modal_auto_load_or_cta = 'auto_trigger' or modal_auto_load_or_cta = 'CTA' then account_id else null end as modal_loaded_flag,
        case when modal_auto_load_or_cta = 'CTA' then account_id else null end as modal_cta_loaded_flag,
        --- Step 2: modal interactions
        case when buy_now_timestamp is not null or see_all_plans_timestamp is not null then account_id else null end as modal_clicked_flag,
        case when buy_now_timestamp is not null then account_id else null end as modal_buy_now_clicked_flag,
        case when see_all_plans_timestamp is not null then account_id else null end as modal_see_all_plans_clicked_flag,
        --- Step 3: plan lineup loads
        case when plan_lineup_support_timestamp is not null or plan_lineup_suite_timestamp is not null then account_id else null end as plan_lineup_loaded_flag,
        case when plan_lineup_support_timestamp is not null then account_id else null end as plan_lineup_support_loaded_flag,
        case when plan_lineup_suite_timestamp is not null then account_id else null end as plan_lineup_suite_loaded_flag,
        --- Step 4: payment page visits
        case 
        when 
            buy_trial_ppv_timestamp is not null or 
            modal_buy_now_ppv_timestamp is not null or 
            modal_see_all_plans_ppv_timestamp is not null 
        then account_id else null end as payment_page_visited_flag,
        case when buy_trial_ppv_timestamp is not null then account_id else null end as buy_your_trial_payment_page_visited_flag,
        case when modal_buy_now_ppv_timestamp is not null then account_id else null end as modal_buy_now_payment_page_visited_flag,
        case when modal_see_all_plans_ppv_timestamp is not null then account_id else null end as modal_see_all_plans_payment_page_visited_flag,
        --- Payment visits suite or support
        case when ppv_support_timestamp is not null then account_id else null end as payment_page_visited_support_flag,
        case when ppv_suite_timestamp is not null then account_id else null end as payment_page_visited_suite_flag,
        --- Step 5: payment submissions
        case 
            when 
                buy_trial_pps_timestamp is not null or 
                modal_buy_now_pps_timestamp is not null or
                modal_see_all_plans_pps_timestamp is not null
            then account_id else null end as payment_submitted_flag,
        case when buy_trial_pps_timestamp is not null then account_id else null end as buy_your_trial_payment_submitted_flag,
        case when modal_buy_now_pps_timestamp is not null then account_id else null end as modal_buy_now_payment_submitted_flag,
        case when modal_see_all_plans_pps_timestamp is not null then account_id else null end as modal_see_all_plans_payment_submitted_flag,
        --- Payment submission suite or support
        case when pps_support_timestamp is not null then account_id else null end as payment_submitted_support_flag,
        case when pps_suite_timestamp is not null then account_id else null end as payment_submitted_suite_flag,
        --- Step 6: win attribution
        case when is_won = 1 then account_id else null end as won_flag,
        case when is_won_ss = 1 then account_id else null end as won_ss_flag,
        case when win_attribution_flag in ('buy_your_trial_win', 'modal_buy_now_win', 'modal_see_all_plans_win') then account_id else null end as won_via_agent_home_flag,
        case when win_attribution_flag = 'buy_your_trial_win' then account_id else null end as won_via_buy_your_trial_flag,
        case when win_attribution_flag = 'modal_buy_now_win' then account_id else null end as won_via_modal_buy_now_flag,
        case when win_attribution_flag = 'modal_see_all_plans_win' then account_id else null end as won_via_modal_see_all_plans_flag,
        case when win_attribution_flag = 'won_outside_agent_home' then account_id else null end as won_outside_agent_home_flag
    from master_cart_funnel_wins_attribution
)

select *, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) as updated_at
from master_cart_funnel_wins_attribution_flags





