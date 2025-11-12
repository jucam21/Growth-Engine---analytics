--- Admin center CTA funnel
--- Handles payment flows from central admin origin
--- Based on is_payment_page_from_admin_center and is_payment_submit_admin_center flags

admin_center_payment_page as (
    select
        events.timestamp as original_timestamp,
        convert_timezone('UTC', 'America/Los_Angeles', events.timestamp) as original_timestamp_pt,
        events.account_id,
        events.session_id,
        events.user_id,
        events.product,
        events.trial_days,
        events.plan_name,
        events.cta_name,
        events.product_cta,
        'suite' as trial_type,
        'admin_center' as cta,
        events.timestamp as ppv_timestamp
    from categorized_payment_events events
    where 
        events.is_payment_page_from_admin_center = 1
        and events.timestamp >= '2025-01-01'
),

admin_center_payment_submit as (
    select
        admin_center_payment_page.*,
        pps.timestamp as pps_timestamp
    from admin_center_payment_page
    left join categorized_payment_events pps
        on admin_center_payment_page.account_id = pps.account_id
        and pps.is_payment_submit_admin_center = 1
        and pps.timestamp >= admin_center_payment_page.ppv_timestamp
        and datediff(minute, admin_center_payment_page.ppv_timestamp, pps.timestamp) <= 120
    qualify row_number() over (partition by admin_center_payment_page.account_id, admin_center_payment_page.original_timestamp order by pps.timestamp) = 1
)

select *
from admin_center_payment_submit
