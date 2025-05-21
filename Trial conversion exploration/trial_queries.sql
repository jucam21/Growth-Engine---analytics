--- Document with the queries for the trial conversion campaign
--- https://docs.google.com/spreadsheets/d/1QWQm0SdXNQss0K_PNuJIROIqThzL3NBIFaOwx_WJwlY/edit?gid=2099012964#gid=2099012964


-- 1. days_to_expiry & trial_age

with trial_expiry as (
    select
        instance_account_id,
        instance_account_is_trial,
        instance_account_trial_expires_on,
        -- Days to trial expiry
        datediff('day', current_date, instance_account_trial_expires_on) as days_to_expiry,
        -- Trial account age
        datediff('day', date(instance_account_created_timestamp), current_date) as trial_age
    from
        foundational.customer.dim_instance_accounts_daily_snapshot_bcv
    where instance_account_is_trial = True
    and days_to_expiry > 0
)

select *
from
    trial_expiry
limit 10


-- 2. Last cart visit

with last_cart_visit as (
    select
        account_id,
        date(max(original_timestamp)) last_cart_visit
    from
        cleansed.segment_billing.segment_billing_cart_loaded_bcv
    group by account_id
)

select *
from last_cart_visit
limit 10


-- 