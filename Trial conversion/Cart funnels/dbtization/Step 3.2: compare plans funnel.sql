--- Compare plans CTA funnel
--- Merging compare plans clicks with modal loads
--- Some modal loads from CTA do not have a preceding compare plans click
--- Limiting to 120 minutes between events

compare_plans_into_modal_load as (
    --- Accounts with compare plans clicks & modal load
    select
        cta_click_.original_timestamp as cta_click_timestamp,
        cta_click_.original_timestamp_pt as cta_click_timestamp_pt,
        cta_click_.account_id as cta_click_account_id,
        cta_click_.trial_type as cta_click_trial_type,
        cta_click_.cta as cta_click_cta,
        modal_load_.*
    from all_modal_events cta_click_
    left join all_modal_events modal_load_
        on cta_click_.account_id = modal_load_.account_id
        and modal_load_.event_type = 'modal_load'
        and modal_load_.original_timestamp >= cta_click_.original_timestamp
        and datediff(minute, cta_click_.original_timestamp, modal_load_.original_timestamp) <= 120
        --- Modal loads from compare plans clicks
        and modal_load_.source = 'CTA'
    --- Just compare plans clicks
    where 
        cta_click_.event_type = 'cta_click'
        and cta_click_.cta = 'compare'
    qualify row_number() over (partition by cta_click_.account_id, cta_click_.original_timestamp order by modal_load_.original_timestamp) = 1
),

compare_plans_and_auto_trigger as (
    select
        *
    from compare_plans_into_modal_load
    
    union all
    
    select
        null as cta_click_timestamp,
        null as cta_click_timestamp_pt,
        null as cta_click_account_id,
        null as cta_click_trial_type,
        null as cta_click_cta,
        modal_load_.*
    from all_modal_events modal_load_
    where 
        modal_load_.event_type = 'modal_load'
        and modal_load_.source = 'auto_trigger'
)

select *
from compare_plans_and_auto_trigger
