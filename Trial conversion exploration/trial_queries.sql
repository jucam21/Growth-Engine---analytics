--- Document with the queries for the trial conversion campaign
--- https://docs.google.com/spreadsheets/d/1QWQm0SdXNQss0K_PNuJIROIqThzL3NBIFaOwx_WJwlY/edit?gid=2099012964#gid=2099012964


----------------------------------------------
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


----------------------------------------------
-- 2. Last cart visit & total cart visits
with last_cart_visit as (
    select
        segment_cart_data.account_id,
        -- Last cart visit
        date(max(segment_cart_data.original_timestamp)) last_cart_visit,
        -- Total cart visits
        count(*) as total_cart_visits
    from
        cleansed.segment_billing.segment_billing_cart_loaded_bcv segment_cart_data
        -- This join is done to make sure we are counting cart visits after the account was created
    inner join foundational.customer.dim_instance_accounts_daily_snapshot_bcv instance_accounts_bcv
        on segment_cart_data.account_id = instance_accounts_bcv.instance_account_id
        and segment_cart_data.original_timestamp >= instance_accounts_bcv.instance_account_created_timestamp
    where
        instance_accounts_bcv.instance_account_is_trial = True
    group by segment_cart_data.account_id
)

select *
from last_cart_visit
order by 3 desc
limit 10



----------------------------------------------
-- 3. Startup flag
-- Startups are identified on the crm account in Salesforce. Assume all instances under the account are startup accounts
with startup as (
    select
        emd.crm_account_id,
        emd.instance_account_id,
        max(1) as startup_flag
    from
        cleansed.salesforce.salesforce_account_bcv as sab
    inner join
        foundational.customer.entity_mapping_daily_snapshot_bcv as emd
        on
            sab.id = emd.crm_account_id
    where
        sab.startup_program_c
        and sab.valid_to_timestamp = date('9999-12-31')
        and emd.instance_account_id is not null
    group by
        emd.crm_account_id,
        emd.instance_account_id
)

select *
from startup
limit 10


----------------------------------------------
-- 4. Product trial
-- I suggest we use the same query we used to extract SKUs for all customers. 
import_skus_with_parsed_settings as (
    select
        account_id,
        name,
        state,
        boost,
        parse_json(plan_settings) as parsed_plan_settings,
        parsed_plan_settings:plan:value::string as plan,
        parsed_plan_settings:maxAgents:value::string as max_agents,
        row_number() over (
            partition by account_id, name, state -- Adding or removing state change results
            order by updated_at desc
        ) as rank
    from propagated_formatted.accountsdb.skus
    qualify rank = 1
),

int_account_skus as (
    select
        account_id,
        array_agg(
            object_construct(
                'name',
                name,
                'state',
                state,
                'plan',
                plan,
                'boost',
                boost,
                'quantity',
                max_agents
            )
        ) as skus
    from import_skus_with_parsed_settings
    group by account_id
)


-----------------------------------------------
-- 5. Tickets
-- Counting # of tickets per channel
with all_tickets as (
    select
        instance_account_id,
        date_trunc('day', min(source_snapshot_date)) as first_ticket_created_date,
        -- Total tickets created
        sum(count_created_tickets) as total_tickets,
        -- Total tickets closed & solved flag
        sum(count_closed_tickets) as total_solved_tickets,
        case when total_solved_tickets > 0 then 1 else 0 end as ticket_solved_flag,
        -- Total tickets created by channel
        sum(case when ticket_via_id in (33, 34, 35, 44, 45, 46) then count_created_tickets end) as talk_tickets,
        sum(case when ticket_via_id in (29) then count_created_tickets end) as chat_tickets,
        sum(case when ticket_via_id in (83, 41, 38, 78, 84, 82, 86, 85, 72, 76, 77, 75, 57, 79, 80, 88, 30, 26, 23, 81, 73, 74, 91) then count_created_tickets end) as msg_tickets,
        sum(case when ticket_via_id in (75) then count_created_tickets end) as native_msg_tickets,
        sum(case when ticket_via_id in (38, 41, 78) then count_created_tickets end) as fb_msg_tickets,
        sum(case when ticket_via_id in (0) then count_created_tickets end) as web_form_tickets,
        sum(case when ticket_via_id in (1, 4) then count_created_tickets end) as mail_tickets
    from
        propagated_functional.product_analytics.fact_aggregated_tickets_data_daily_snapshot tickets
    inner join foundational.customer.dim_instance_accounts_daily_snapshot_bcv instance_accounts_bcv
        on tickets.instance_account_id = instance_accounts_bcv.instance_account_id
        and tickets.source_snapshot_date >= instance_accounts_bcv.instance_account_created_timestamp
    where
        -- excludes sample tickets
        ticket_via_id not in (51, 52)
    group by
        instance_account_id
)



-----------------------------------------------
-- 6. Tickets
-- Counting # of tickets per channel


