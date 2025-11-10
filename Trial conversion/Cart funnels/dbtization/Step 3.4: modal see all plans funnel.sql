--- Modal see all plans funnel (triggered from compare plans)
--- Includes support and suite sub-funnels
--- Determining closest next event in the funnel sequence after modal load
--- Limiting to 120 minutes between events

modal_load_see_all_plans_click as (
    select
        compare_plans_and_auto_trigger.*,
        modal_see_all_plans_.original_timestamp as see_all_plans_timestamp,
        modal_see_all_plans_.plan_name as see_all_plans_plan_name
    from compare_plans_and_auto_trigger
    left join all_modal_events modal_see_all_plans_
        on compare_plans_and_auto_trigger.account_id = modal_see_all_plans_.account_id
        and modal_see_all_plans_.event_type = 'modal_see_all_plans'
        and modal_see_all_plans_.original_timestamp >= compare_plans_and_auto_trigger.original_timestamp
        and datediff(minute, compare_plans_and_auto_trigger.original_timestamp, modal_see_all_plans_.original_timestamp) <= 120
    qualify row_number() over (partition by compare_plans_and_auto_trigger.account_id, compare_plans_and_auto_trigger.original_timestamp order by modal_see_all_plans_.original_timestamp) = 1
),

--- Support mini funnel
modal_load_see_all_plans_support as (
    select
        modal_load_see_all_plans_click.*,
        pl.timestamp as plan_lineup_support_timestamp,
        pl.session_id as plan_lineup_support_session_id,
        pl.user_id as plan_lineup_support_user_id,
        pl.product as plan_lineup_support_product
    from modal_load_see_all_plans_click
    left join categorized_payment_events pl
        on modal_load_see_all_plans_click.account_id = pl.account_id
        and pl.event_category = 'plan_lineup'
        and pl.product = 'support'
        and pl.timestamp >= modal_load_see_all_plans_click.see_all_plans_timestamp
        and datediff(minute, modal_load_see_all_plans_click.see_all_plans_timestamp, pl.timestamp) <= 120
    where modal_load_see_all_plans_click.see_all_plans_timestamp is not null
    qualify row_number() over (partition by modal_load_see_all_plans_click.account_id, modal_load_see_all_plans_click.original_timestamp order by pl.timestamp) = 1
),

modal_see_all_plans_support_payment_visit as (
    select
        modal_load_see_all_plans_support.*,
        ppv.timestamp as ppv_support_timestamp,
        ppv.session_id as ppv_support_session_id,
        ppv.user_id as ppv_support_user_id,
        ppv.trial_days as ppv_support_trial_days,
        ppv.plan_name as ppv_support_plan_name,
        ppv.product as ppv_support_product,
        ppv.cta_name as ppv_support_cta_name,
        ppv.product_cta as ppv_support_product_cta
    from modal_load_see_all_plans_support
    left join categorized_payment_events ppv
        on modal_load_see_all_plans_support.account_id = ppv.account_id
        and ppv.event_category = 'payment_page_from_support'
        and ppv.timestamp >= modal_load_see_all_plans_support.plan_lineup_support_timestamp
        and datediff(minute, modal_load_see_all_plans_support.plan_lineup_support_timestamp, ppv.timestamp) <= 120
    where modal_load_see_all_plans_support.plan_lineup_support_timestamp is not null
    qualify row_number() over (partition by modal_load_see_all_plans_support.account_id, modal_load_see_all_plans_support.original_timestamp order by ppv.timestamp) = 1
),

modal_see_all_plans_support_payment_submit as (
    select
        modal_see_all_plans_support_payment_visit.*,
        pps.timestamp as pps_support_timestamp,
        pps.session_id as pps_support_session_id,
        pps.user_id as pps_support_user_id,
        pps.trial_days as pps_support_trial_days,
        pps.cta_name as pps_support_cta_name,
        pps.product_cta as pps_support_product_cta
    from modal_see_all_plans_support_payment_visit
    left join categorized_payment_events pps
        on modal_see_all_plans_support_payment_visit.account_id = pps.account_id
        and pps.event_category = 'payment_submit_all_plans'
        and pps.product_cta = 'support'
        and pps.timestamp >= modal_see_all_plans_support_payment_visit.ppv_support_timestamp
        and datediff(minute, modal_see_all_plans_support_payment_visit.ppv_support_timestamp, pps.timestamp) <= 120
    where modal_see_all_plans_support_payment_visit.ppv_support_timestamp is not null
    qualify row_number() over (partition by modal_see_all_plans_support_payment_visit.account_id, modal_see_all_plans_support_payment_visit.original_timestamp order by pps.timestamp) = 1
),

--- Suite mini funnel
modal_load_see_all_plans_suite as (
    select
        modal_load_see_all_plans_click.*,
        pl.timestamp as plan_lineup_suite_timestamp,
        pl.session_id as plan_lineup_suite_session_id,
        pl.user_id as plan_lineup_suite_user_id,
        pl.product as plan_lineup_suite_product
    from modal_load_see_all_plans_click
    left join categorized_payment_events pl
        on modal_load_see_all_plans_click.account_id = pl.account_id
        and pl.event_category = 'plan_lineup'
        and pl.product <> 'support'
        and pl.timestamp >= modal_load_see_all_plans_click.see_all_plans_timestamp
        and datediff(minute, modal_load_see_all_plans_click.see_all_plans_timestamp, pl.timestamp) <= 120
    where modal_load_see_all_plans_click.see_all_plans_timestamp is not null
    qualify row_number() over (partition by modal_load_see_all_plans_click.account_id, modal_load_see_all_plans_click.original_timestamp order by pl.timestamp) = 1
),

modal_see_all_plans_suite_payment_visit as (
    select
        modal_load_see_all_plans_suite.*,
        ppv.timestamp as ppv_suite_timestamp,
        ppv.session_id as ppv_suite_session_id,
        ppv.user_id as ppv_suite_user_id,
        ppv.trial_days as ppv_suite_trial_days,
        ppv.plan_name as ppv_suite_plan_name,
        ppv.product as ppv_suite_product,
        ppv.cta_name as ppv_suite_cta_name,
        ppv.product_cta as ppv_suite_product_cta
    from modal_load_see_all_plans_suite
    left join categorized_payment_events ppv
        on modal_load_see_all_plans_suite.account_id = ppv.account_id
        and ppv.event_category = 'payment_page_from_suite'
        and ppv.timestamp >= modal_load_see_all_plans_suite.plan_lineup_suite_timestamp
        and datediff(minute, modal_load_see_all_plans_suite.plan_lineup_suite_timestamp, ppv.timestamp) <= 120
    where modal_load_see_all_plans_suite.plan_lineup_suite_timestamp is not null
    qualify row_number() over (partition by modal_load_see_all_plans_suite.account_id, modal_load_see_all_plans_suite.original_timestamp order by ppv.timestamp) = 1
),

modal_see_all_plans_suite_payment_submit as (
    select
        modal_see_all_plans_suite_payment_visit.*,
        pps.timestamp as pps_suite_timestamp,
        pps.session_id as pps_suite_session_id,
        pps.user_id as pps_suite_user_id,
        pps.trial_days as pps_suite_trial_days,
        pps.cta_name as pps_suite_cta_name,
        pps.product_cta as pps_suite_product_cta
    from modal_see_all_plans_suite_payment_visit
    left join categorized_payment_events pps
        on modal_see_all_plans_suite_payment_visit.account_id = pps.account_id
        and pps.event_category = 'payment_submit_all_plans'
        and pps.product_cta = 'zendesk_suite'
        and pps.timestamp >= modal_see_all_plans_suite_payment_visit.ppv_suite_timestamp
        and datediff(minute, modal_see_all_plans_suite_payment_visit.ppv_suite_timestamp, pps.timestamp) <= 120
    where modal_see_all_plans_suite_payment_visit.ppv_suite_timestamp is not null
    qualify row_number() over (partition by modal_see_all_plans_suite_payment_visit.account_id, modal_see_all_plans_suite_payment_visit.original_timestamp order by pps.timestamp) = 1
),

--- Joined output combining support and suite paths
modal_see_all_plans_payment_submit_joined as (
    select
        modal_load_see_all_plans_click.*,
        modal_see_all_plans_support_payment_submit.plan_lineup_support_timestamp,
        modal_see_all_plans_support_payment_submit.ppv_support_timestamp,
        modal_see_all_plans_support_payment_submit.pps_support_timestamp,
        modal_load_see_all_plans_suite.plan_lineup_suite_timestamp,
        modal_see_all_plans_suite_payment_visit.ppv_suite_timestamp,
        modal_see_all_plans_suite_payment_submit.pps_suite_timestamp,
        --- Payment visits or submits aggregated timestamps
        case 
            when modal_see_all_plans_support_payment_submit.ppv_support_timestamp is not null 
                and modal_see_all_plans_suite_payment_visit.ppv_suite_timestamp is not null 
            then least(modal_see_all_plans_support_payment_submit.ppv_support_timestamp, modal_see_all_plans_suite_payment_visit.ppv_suite_timestamp)
            when modal_see_all_plans_support_payment_submit.ppv_support_timestamp is not null 
            then modal_see_all_plans_support_payment_submit.ppv_support_timestamp
            when modal_see_all_plans_suite_payment_visit.ppv_suite_timestamp is not null 
            then modal_see_all_plans_suite_payment_visit.ppv_suite_timestamp
            else null
        end as ppv_timestamp,
        case 
            when modal_see_all_plans_support_payment_submit.pps_support_timestamp is not null 
                and modal_see_all_plans_suite_payment_submit.pps_suite_timestamp is not null 
            then least(modal_see_all_plans_support_payment_submit.pps_support_timestamp, modal_see_all_plans_suite_payment_submit.pps_suite_timestamp)
            when modal_see_all_plans_support_payment_submit.pps_support_timestamp is not null 
            then modal_see_all_plans_support_payment_submit.pps_support_timestamp
            when modal_see_all_plans_suite_payment_submit.pps_suite_timestamp is not null 
            then modal_see_all_plans_suite_payment_submit.pps_suite_timestamp
            else null
        end as pps_timestamp
    from modal_load_see_all_plans_click
    --- Joining at the timestamp level to measure events that happened in the same session
    left join modal_see_all_plans_support_payment_submit
        on modal_load_see_all_plans_click.account_id = modal_see_all_plans_support_payment_submit.account_id
        and modal_load_see_all_plans_click.original_timestamp = modal_see_all_plans_support_payment_submit.original_timestamp
    left join modal_load_see_all_plans_suite
        on modal_load_see_all_plans_click.account_id = modal_load_see_all_plans_suite.account_id
        and modal_load_see_all_plans_click.original_timestamp = modal_load_see_all_plans_suite.original_timestamp
    left join modal_see_all_plans_suite_payment_visit
        on modal_load_see_all_plans_click.account_id = modal_see_all_plans_suite_payment_visit.account_id
        and modal_load_see_all_plans_click.original_timestamp = modal_see_all_plans_suite_payment_visit.original_timestamp
    left join modal_see_all_plans_suite_payment_submit
        on modal_load_see_all_plans_click.account_id = modal_see_all_plans_suite_payment_submit.account_id
        and modal_load_see_all_plans_click.original_timestamp = modal_see_all_plans_suite_payment_submit.original_timestamp
)

select *
from modal_see_all_plans_payment_submit_joined
