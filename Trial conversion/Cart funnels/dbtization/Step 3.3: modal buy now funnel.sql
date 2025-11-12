--- Modal buy now funnel (triggered from compare plans)
--- Determining closest next event in the funnel sequence after modal load
--- Limiting to 120 minutes between events

modal_load_buy_now_click as (
    select
        compare_plans_and_auto_trigger.*,
        modal_buy_now_.original_timestamp as buy_now_timestamp,
        modal_buy_now_.promo_code
    from compare_plans_and_auto_trigger
    left join all_modal_events modal_buy_now_
        on compare_plans_and_auto_trigger.account_id = modal_buy_now_.account_id
        and modal_buy_now_.event_type = 'modal_buy_now'
        and modal_buy_now_.original_timestamp >= compare_plans_and_auto_trigger.original_timestamp
        and datediff(minute, compare_plans_and_auto_trigger.original_timestamp, modal_buy_now_.original_timestamp) <= 120
    qualify row_number() over (partition by compare_plans_and_auto_trigger.account_id, compare_plans_and_auto_trigger.original_timestamp order by modal_buy_now_.original_timestamp) = 1
),

modal_load_buy_now_payment_visit as (
    select
        modal_load_buy_now_click.*,
        ppv.timestamp as ppv_timestamp,
        ppv.session_id,
        ppv.user_id as ppv_user_id,
        ppv.trial_days,
        ppv.plan_name as ppv_plan_name,
        ppv.product as ppv_product,
        ppv.cta_name,
        ppv.product_cta
    from modal_load_buy_now_click
    left join categorized_payment_events ppv
        on modal_load_buy_now_click.account_id = ppv.account_id
        and ppv.is_payment_page_from_trial_buy = 1
        and ppv.timestamp >= modal_load_buy_now_click.buy_now_timestamp
        and datediff(minute, modal_load_buy_now_click.buy_now_timestamp, ppv.timestamp) <= 120
    where modal_load_buy_now_click.buy_now_timestamp is not null
    qualify row_number() over (partition by modal_load_buy_now_click.account_id, modal_load_buy_now_click.original_timestamp order by ppv.timestamp) = 1
),

modal_load_buy_now_payment_submit as (
    select
        modal_load_buy_now_payment_visit.*,
        pps.timestamp as pps_timestamp,
        pps.session_id as pps_session_id,
        pps.user_id as pps_user_id,
        pps.trial_days as pps_trial_days,
        pps.cta_name as pps_cta_name,
        pps.product_cta as pps_product_cta
    from modal_load_buy_now_payment_visit
    left join categorized_payment_events pps
        on modal_load_buy_now_payment_visit.account_id = pps.account_id
        and pps.is_payment_submit_buy_trial = 1
        and pps.timestamp >= modal_load_buy_now_payment_visit.ppv_timestamp
        and datediff(minute, modal_load_buy_now_payment_visit.ppv_timestamp, pps.timestamp) <= 120
    where modal_load_buy_now_payment_visit.ppv_timestamp is not null
    qualify row_number() over (partition by modal_load_buy_now_payment_visit.account_id, modal_load_buy_now_payment_visit.original_timestamp order by pps.timestamp) = 1
)

select *
from modal_load_buy_now_payment_submit
