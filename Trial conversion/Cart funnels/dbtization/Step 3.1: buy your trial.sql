
--- Buy your trial CTA funnel

buy_your_trial_payment_page as (
    select
        all_modal_events_.*,
        ppv.timestamp as ppv_timestamp,
    from all_modal_events all_modal_events_
    left join categorized_payment_events ppv
        on all_modal_events_.account_id = ppv.account_id
        and ppv.event_category = 'payment_page_from_trial_buy'
        and ppv.timestamp >= all_modal_events_.original_timestamp
        and datediff(minute, all_modal_events_.original_timestamp, ppv.timestamp) <= 120
    where 
        all_modal_events_.event_type = 'cta_click'
        and all_modal_events_.cta = 'purchase'
    qualify row_number() over (partition by all_modal_events_.account_id, all_modal_events_.original_timestamp order by ppv.timestamp) = 1
),

buy_your_trial_payment_submit as (
    select
        buy_your_trial_payment_page_.*,
        pps.timestamp as pps_timestamp
    from buy_your_trial_payment_page buy_your_trial_payment_page_
    left join categorized_payment_events pps
        on buy_your_trial_payment_page_.account_id = pps.account_id
        and pps.event_category = 'payment_submit_buy_trial'
        and pps.timestamp >= buy_your_trial_payment_page_.ppv_timestamp
        and datediff(minute, buy_your_trial_payment_page_.ppv_timestamp, pps.timestamp) <= 120
    qualify row_number() over (partition by buy_your_trial_payment_page_.account_id, buy_your_trial_payment_page_.original_timestamp order by pps.timestamp) = 1
)

select *
from buy_your_trial_payment_submit


