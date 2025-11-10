--- dbtized query

/* 
This query categorizes all possible payment page visits & payment submits
from all possible sources and CTAs.

The tables used for each interaction are:

Plan lineup:
  cleansed.segment_billing.segment_billing_cart_loaded_scd2
  cleansed.segment_billing.segment_billing_payment_loaded_scd2
  cleansed.bq_archive.billing_payment_loaded
  cleansed.bq_archive.billing_cart_loaded

Payment page visits:
  cleansed.segment_billing.segment_billing_cart_loaded_scd2
  cleansed.segment_billing.segment_billing_payment_loaded_scd2
  cleansed.bq_archive.billing_payment_loaded
  cleansed.bq_archive.billing_cart_loaded

Payment submissions:
  cleansed.segment_billing.segment_billing_update_submit_scd2
  cleansed.bq_archive.billing_update_submit
*/

/*
 The conditions used for each of the funnels are:
 
 • 'plan_lineup': User is viewing all available plans.
     - Not paid
     - Not central admin or expired trial
     - cart_screen = 'preset_all_plans'
     - cart_step = 'multi_step_plan'
     - cart_type = 'spp_self_service'

 • 'payment_page_from_support': User is on payment page for Support product.
     - Not paid
     - Not central admin or expired trial
     - cart_screen <> 'preset_trial_plan' and cart_screen <> 'buy_your_trial'
     - cart_step = 'multi_step_payment'
     - cart_type = 'spp_self_service'
     - product = 'support'

 • 'payment_page_from_suite': User is on payment page for Suite product.
     - Not paid
     - Not central admin or expired trial
     - cart_screen <> 'preset_trial_plan' and cart_screen <> 'buy_your_trial'
     - cart_step = 'multi_step_payment'
     - cart_type = 'spp_self_service'
     - product <> 'support'

 • 'payment_page_from_trial_buy': User is on payment page for buying trial plan.
     - Not paid
     - Not central admin or expired trial
     - cart_screen in ('preset_trial_plan', 'buy_your_trial')
     - cart_step = 'multi_step_payment'
     - cart_type = 'spp_self_service'

 • 'payment_submit_all_plans': User submits payment for any plan except trial.
     - Not paid
     - Not central admin or expired trial
     - cart_screen <> 'preset_trial_plan' and cart_screen <> 'buy_your_trial'
     - cart_step = 'multi_step_payment'
     - cart_type = 'spp_self_service'
     - source in ('segment_billing_update_submit_scd2', 'bq_archive_billing_update_submit')

 • 'payment_submit_buy_trial': User submits payment for trial plan.
     - Not paid
     - Not central admin or expired trial
     - cart_screen in ('preset_trial_plan', 'buy_your_trial')
     - cart_step = 'multi_step_payment'
     - cart_type = 'spp_self_service'
     - source in ('segment_billing_update_submit_scd2', 'bq_archive_billing_update_submit')

 • 'other': Any event not matching the above rules.
*/

--- Step 0: Union all payment page related events
with payment_page_events as (
    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        product,
        null as trial_days,
        null as plan_name,
        null as cta_name,
        null as product_cta,
        cart_screen,
        cart_step,
        cart_type,
        origin,
        paid_customer,
        'segment_billing_cart_loaded_scd2' as source
    from cleansed.segment_billing.segment_billing_cart_loaded_scd2

    union all

    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        product,
        trial_days,
        plan_name,
        null as cta_name,
        null as product_cta,
        cart_screen,
        cart_step,
        cart_type,
        origin,
        paid_customer,
        'segment_billing_payment_loaded_scd2' as source
    from cleansed.segment_billing.segment_billing_payment_loaded_scd2

    union all

    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        product,
        trial_days,
        plan_name,
        null as cta_name,
        null as product_cta,
        cart_screen,
        cart_step,
        cart_type,
        origin,
        paid_customer,
        'bq_archive_billing_payment_loaded' as source
    from cleansed.bq_archive.billing_payment_loaded

    union all

    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        product,
        trial_days,
        plan_name,
        null as cta_name,
        null as product_cta,
        cart_screen,
        cart_step,
        cart_type,
        origin,
        paid_customer,
        'bq_archive_billing_cart_loaded' as source
    from cleansed.bq_archive.billing_cart_loaded

    union all

    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        null as product,
        trial_days,
        null as plan_name,
        null as cta_name,
        null as product_cta,
        cart_screen,
        cart_step,
        cart_type,
        origin,
        paid_customer,
        'segment_billing_update_submit_scd2' as source
    from cleansed.segment_billing.segment_billing_update_submit_scd2

    union all

    select distinct
        original_timestamp as timestamp,
        session_id,
        user_id,
        account_id,
        null as product,
        trial_days,
        null as plan_name,
        null as cta_name,
        null as product_cta,
        cart_screen,
        cart_step,
        cart_type,
        origin,
        paid_customer,
        'bq_archive_billing_update_submit' as source
    from cleansed.bq_archive.billing_update_submit
),

--- Step 1: Create categorized payment events
categorized_payment_events as (
    select distinct
        timestamp,
        session_id,
        user_id,
        account_id,
        product,
        trial_days,
        plan_name,
        cart_screen,
        cart_step,
        cart_type,
        origin,
        paid_customer,
        source,
        case
            when not paid_customer
                and origin not in ('central_admin', 'expired-trial')
                and cart_screen = 'preset_all_plans'
                and cart_step = 'multi_step_plan'
                and cart_type = 'spp_self_service'
                then 'plan_lineup'
            when not paid_customer
                and origin not in ('central_admin', 'expired-trial')
                and cart_screen <> 'preset_trial_plan'
                and cart_screen <> 'buy_your_trial'
                and cart_step = 'multi_step_payment'
                and cart_type = 'spp_self_service'
                and product = 'support'
                then 'payment_page_from_support'
            when not paid_customer
                and origin not in ('central_admin', 'expired-trial')
                and cart_screen <> 'preset_trial_plan'
                and cart_screen <> 'buy_your_trial'
                and cart_step = 'multi_step_payment'
                and cart_type = 'spp_self_service'
                and product <> 'support'
                then 'payment_page_from_suite'
            when not paid_customer
                and origin not in ('central_admin', 'expired-trial')
                and cart_screen in ('preset_trial_plan', 'buy_your_trial')
                and cart_step = 'multi_step_payment'
                and cart_type = 'spp_self_service'
                then 'payment_page_from_trial_buy'
            when not paid_customer
                and origin not in ('central_admin', 'expired-trial')
                and cart_screen <> 'preset_trial_plan'
                and cart_screen <> 'buy_your_trial'
                and cart_step = 'multi_step_payment'
                and cart_type = 'spp_self_service'
                and source in ('segment_billing_update_submit_scd2', 'bq_archive_billing_update_submit')
                then 'payment_submit_all_plans'
            when not paid_customer
                and origin not in ('central_admin', 'expired-trial')
                and cart_screen in ('preset_trial_plan', 'buy_your_trial')
                and cart_step = 'multi_step_payment'
                and cart_type = 'spp_self_service'
                and source in ('segment_billing_update_submit_scd2', 'bq_archive_billing_update_submit')
                then 'payment_submit_buy_trial'
            else 'other'
        end as event_category
    from payment_page_events
),

select *
from categorized_payment_events





--- Queries to validate numbers
--- It matches E2B numbers, removing all filters

ga_trial_accounts as (
    select distinct
        instance_account_id as account_id,
        instance_account_arr_usd_at_win as arr,
        win_date as win_date,
        paid_products_at_win as paid_products,
        core_base_plan_at_win as core_base_plan,
        is_startup_program as startup_flag,
        seats_capacity_at_win as seats_at_win,
        instance_account_created_date,
        sales_model_at_win
    from presentation.growth_analytics.trial_accounts
    where
        win_date is not null
        and not is_direct_buy
        and is_abusive = false
),

min_account as (
    select
        coalesce(events.account_id, ga_accounts.account_id) as account_id,
        min(coalesce(events.timestamp, ga_accounts.instance_account_created_date)) as first_event_timestamp
    from categorized_payment_events events
    inner join presentation.growth_analytics.trial_accounts trial_accounts 
        on 
            events.account_id = trial_accounts.instance_account_id
            and trial_accounts.first_verified_date is not null
            and trial_accounts.is_abusive = false
            and trial_accounts.instance_account_created_date >= '2024-01-01'
    full outer join ga_trial_accounts ga_accounts
        on events.account_id = ga_accounts.account_id
    --where events.event_category not like '%submit%'
    group by 1
)

select 
    date_trunc('month', first_event_timestamp) as first_event_date,
    count(distinct account_id) as account_id_count
from min_account
where first_event_timestamp >= '2024-07-01'
group by 1
order by 1




