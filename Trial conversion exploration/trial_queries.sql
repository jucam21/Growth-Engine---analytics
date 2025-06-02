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
        tickets.instance_account_id,
        date_trunc('day', min(tickets.source_snapshot_date)) as first_ticket_created_date,
        -- Total tickets created
        sum(count_created_tickets) as total_tickets,
        sum(case when ticket_via_id in (51, 52) then count_created_tickets end) as total_sample_tickets,
        -- Total tickets closed & solved flag
        sum(count_closed_tickets) as total_solved_tickets,
        sum(case when ticket_via_id in (51, 52) then count_closed_tickets end) as total_solved_sample_tickets,
        case when total_solved_tickets > 0 then 1 else 0 end as ticket_solved_flag,
        case when total_solved_sample_tickets > 0 then 1 else 0 end as ticket_sample_solved_flag,
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
    group by
        tickets.instance_account_id
)

select *
from all_tickets
limit 10

-----------------------------------------------
-- 6. Agent comments
-- Table is in snowflake regional (AMER)
-- This query is REALLY SLOW because the table is huge. 
-- I could not get this query to run. 
--We might need help from product analytics to get this data
with ticket_comments as (
    select instance_account_id, count(*) as total_comments
    from cleansed.product_support.base_ticket_comment_events comments
    where date(created_timestamp) >= '2025-05-01'
    and instance_account_id = 18288435
    group by instance_account_id
)

select *
from ticket_comments
limit 10

--- Example of comments for one account
select *
from cleansed.product_support.base_ticket_comment_events comments
where date(created_timestamp) >= '2025-05-01'
and instance_account_id = 18288435
limit 100



-----------------------------------------------
-- 7. Number of active agents
with active_agents as (
    select
        instance_account_id, 
        count(distinct agent_id) as num_agents
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    where agent_is_active = True
    group by instance_account_id
)

select *
from active_agents
limit 10



-----------------------------------------------
-- 8. Help center articles

select 
    instance_account_id, 
    count(distinct guide_article_id) total_help_center_articles,
    case when hca_counts > 0 then 1 else 0 end as help_center_created_flag
from propagated_cleansed.product_guide.base_guide_articles
group by all
limit 10




-----------------------------------------------
-- 9. Payment page visits

with payment_page_visits as (
    select distinct
        timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'

    union all

    select distinct
        timestamp,
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product
    from
        cleansed.segment_billing.segment_billing_payment_loaded_scd2
    where
        not paid_customer
        and cart_step = 'multi_step_payment'
        and cart_type = 'spp_self_service'
),

aggregated_payment_page_visits as (
    select 
        account_id,
        count(distinct session_id) as total_payment_page_visits,
        case when total_payment_page_visits > 0 then 1 else 0 end as payment_page_flag
    from 
        payment_page_visits
    group by
        account_id
)

select *
from aggregated_payment_page_visits
limit 10



-----------------------------------------------
-- 10. Pricing and plan page visits
-- Trials can enter the cart through different CTAs:
-- all plans, buy your trial, and other.

-- When a customer enters the cart, two segment events are fired:
-- segment_billing_cart_loaded_scd2 & segment_billing_payment_loaded_scd2
-- To be general, we combine both events into one table.

-- The filters are defined for each type of CTA
with all_plans_cta as ( -- Triggered by clicking on "Compare Plans" CTA
    select distinct
        date(timestamp),
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        and (cart_screen in ('preset_all_plans', 'preset_support', 'presets', 'preset_suite') or (cart_screen is null and cart_step in ('multi_step_plan', 'multi_step_customization')))  --> CTA specific filter
        and cart_type = 'spp_self_service'

    union all

    select distinct
        date(timestamp),
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Compare plans' as cta_name
    from
        cleansed.segment_billing.segment_billing_payment_loaded_scd2
    where
        not paid_customer
        and (cart_screen in ('preset_all_plans', 'preset_support', 'presets', 'preset_suite') or (cart_screen is null and cart_step in ('multi_step_plan', 'multi_step_customization', 'multi_step_payment'))) --> CTA specific filter
        and cart_type = 'spp_self_service'
),

buy_your_trial_cta as ( -- Triggered by clicking on "Buy Trial Plan" CTA

    select distinct
        date(timestamp),
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Buy Trial Plan' as cta_name
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        and cart_screen = 'preset_trial_plan'
        and cart_type = 'spp_self_service'

    union all

    select distinct
        date(timestamp),
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Buy Trial Plan' as cta_name
    from
        cleansed.segment_billing.segment_billing_payment_loaded_scd2
    where
        not paid_customer
        and cart_screen = 'preset_trial_plan' --> CTA specific filter
        and cart_type = 'spp_self_service'
),

other_cta as ( -- Triggered by clicking on "Compare Plans" CTA
    select distinct
        date(timestamp),
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Other' as cta_name
    from
        cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where
        not paid_customer
        and cart_screen not in ('preset_all_plans', 'preset_support', 'presets', 'preset_suite', 'preset_trial_plan') --> CTA specific filter
        and cart_type = 'spp_self_service'

    union all

    select distinct
        date(timestamp),
        session_id,
        user_id,
        account_id,
        trial_days,
        plan_name,
        product,
        'Other' as cta_name
    from
        cleansed.segment_billing.segment_billing_payment_loaded_scd2
    where
        not paid_customer
        and cart_screen not in ('preset_all_plans', 'preset_support', 'presets', 'preset_suite', 'preset_trial_plan') --> CTA specific filter
        and cart_type = 'spp_self_service'
),

cart_entrances as (
    select *
    from
        all_plans_cta
    union all
    select *
    from
        buy_your_trial_cta
    union all
    select *
    from
        other_cta
),

aggregated_cart_entrances as (
    select 
        account_id,
        count(distinct session_id) as total_cart_visits,
        -- all plans sessions
        count(distinct case when cta_name = 'Compare plans' then session_id end) as pricing_plan_page_visits,
        -- buy your trial sessions
        count(distinct case when cta_name = 'Buy Trial Plan' then session_id end) as buy_your_trial_visits,
        -- other sessions
        count(distinct case when cta_name = 'Other' then session_id end) as other_visits,
        -- Flags for each CTA
        case when pricing_plan_page_visits > 0 then 1 else 0 end as all_plans_flag,
        case when buy_your_trial_visits > 0 then 1 else 0 end as buy_your_trial_flag,
        case when other_visits > 0 then 1 else 0 end as other_flag
    from 
        cart_entrances
    group by
        account_id
)

select *
from aggregated_cart_entrances
limit 10


-----------------------------------------------
-- 11. Verified trial
with verified_dates as (
select
    instance_account_id, 
    source_snapshot_timestamp as first_verified_timestamp, 
from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot
where 
    agent_is_verified = true
    and agent_is_owner = true
    and date(created_timestamp) >= '2025-01-01'
group by instance_account_id, first_verified_timestamp  
qualify rank() over (partition by instance_account_id order by first_verified_timestamp) = 1
), 

verified_date_email as (
select
    instance_account_id, 
    date(min(first_verified_timestamp)) as first_verified_date,
    1 as verified_flag
from verified_dates
group by instance_account_id
)

select *
from verified_date_email
limit 10


-----------------------------------------------
-- 12. External email connected 
with connect_email_dates as (			
select			
    source_snapshot_date,
    support_address_instance_account_id AS instance_account_id,
    support_address_num_external_verified_addresses
from propagated_foundational.product_user_entities.dim_support_addresses_aggregated_daily_snapshot
), 

connected_email AS (			
select						
    instance_account_id,
    min(
        case
            when 
                coalesce(support_address_num_external_verified_addresses, 0) > 0 then date(source_snapshot_date)
        end) as external_email_connected_date,
    1 as external_email_connected_flag
from connect_email_dates			
group by 1			
)	

select *
from connected_email
limit 10


-----------------------------------------------
-- 13. Opportunity stage rep name
-- Have to join CRM id to instance account id for joining this data

with opportunity_owner as (
    select distinct
        slfc_opp.account_id as crm_account_id,
        slfc_acc.name as crm_account_name,
        true as has_rep_assigned
    FROM foundational.customer.dim_crm_opportunities_daily_snapshot_bcv daily_opps
    left join cleansed.salesforce.salesforce_opportunity_bcv slfc_opp
        on daily_opps.crm_opportunity_id = slfc_opp.id
    left join foundational.customer.dim_crm_users_daily_snapshot_bcv users_daily
        on users_daily.crm_user_id = slfc_opp.owner_id
        and users_daily.run_date = current_date
    left join cleansed.salesforce.salesforce_account_bcv slfc_acc
        on slfc_acc.id = slfc_opp.account_id
    where 
        -- CRM has a rep assigned
        users_daily.user_name is not null
        -- CRM has at least one opportunity open
        and daily_opps.opportunity_stage_name in (
            '03 - Establish Value',
            '04 - Demonstrate Value',
            '05 - Secure Commitment',
            '06 - Contracting',
            '07 - Signed'
        )
)

select *
from opportunity_owner
order by 1
limit 10




-----------------------------------------------
-- 14. Prior created account with same email

--- Trial email data can be in two places: 
-- either in propagated_cleansed.product_accounts.base_trial_extras or 
-- propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot
-- The majority of trials will have email in the first table, but if for some reason it is not there, we can use the second table.
with trial_emails as (
    select
        instance_account_id, 
        lower(max(trial_extra_value)) as cust_owner_email
    from propagated_cleansed.product_accounts.base_trial_extras
    where lower(trial_extra_key) = 'email'
    group by instance_account_id
),

verified_date_email0 as (
select
    instance_account_id, 
    source_snapshot_timestamp as first_verified_timestamp, 
    min(agent_email) as cust_owner_email
    from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot
    where 
        agent_is_verified = true
        and agent_is_owner = true
        and date(created_timestamp) >= '2025-01-01'
    group by instance_account_id, first_verified_timestamp  
    qualify rank() over (partition by instance_account_id order by first_verified_timestamp) = 1
), 

verified_date_email as (
    select
        instance_account_id, 
        min(first_verified_timestamp) as first_verified_timestamp, 
        min(cust_owner_email) as cust_owner_email
    from verified_date_email0
    group by instance_account_id
),

-- Joining both email sources 
import_accounts_emails as (
    select
        d.instance_account_id, 
        d.instance_account_created_timestamp, 
        date(d.instance_account_created_timestamp) as instance_account_created_date, 
        coalesce(tee.cust_owner_email,vd.cust_owner_email) as cust_owner_email
    from foundational.customer.dim_instance_accounts_daily_snapshot_bcv d
    left join trial_emails tee
        on d.instance_account_id = tee.instance_account_id
    left join verified_date_email vd
        on d.instance_account_id = vd.instance_account_id
    where 
        date(d.instance_account_created_timestamp) >= '2022-01-01'
        and d.instance_account_derived_type not in ('Internal Instance', 'Sandbox')
        and instance_account_is_sandbox = false
),

-- Cleaning up emails
int_accounts_emails as (
    select 
        instance_account_id, 
        instance_account_created_date, 
        instance_account_created_timestamp, 
        concat(
            split_part(split_part(cust_owner_email, '@', 1), '+', 1), 
            '@', 
            split_part(cust_owner_email, '@', 2)
        ) as email_stripped
    from import_accounts_emails
),

last_created_account as (
    select 
        *, 
        -- Days since the last account was created for the same email
        instance_account_created_date - lag(instance_account_created_date) over (
            partition by email_stripped order by instance_account_created_timestamp
        ) as days_since_last_account_created -- calculating the days since the last trial was created
    from int_accounts_emails
), 

days_since_last_created as (
    select 
        distinct
        instance_account_id, 
        instance_account_created_date, 
        instance_account_created_timestamp, 
        email_stripped, 
        -- Field to determine threshold for the last created account
        days_since_last_account_created,
        iff(days_since_last_account_created <= 365 , 1, 0) as created_last_year_flag
    from last_created_account
    order by email_stripped, instance_account_created_timestamp
)

select *
from days_since_last_created
limit 10


-----------------------------------------------
-- 15. Tryal type


with ranked_skus as (
    select
        instance_account_id,
        created_timestamp,
        case
            when sku_name in ('sell_suite', 'sell') then 'Sell'
            when sku_name = 'support' then 'Support'
            when sku_name in ('zendesk_suite', 'suite') then 'Suite'
        end as sku_name,
        lag(created_timestamp) over (partition by instance_account_id order by created_timestamp) as prev_timestamp
    from
        propagated_cleansed.product_accounts.base_skus_scd2
    where
        sku_name in ('sell_suite', 'sell', 'support', 'zendesk_suite', 'suite')
),

ranked_trials as (
    select
        instance_account_id,
        created_timestamp,
        sku_name
    from
        ranked_skus
    -- assume it's the same trial if created < 5 sec apart#}
    qualify
        dense_rank()
            over (
                partition by
                    instance_account_id
                order by
                    case
                        when prev_timestamp is null then created_timestamp
                        when datediff(second, prev_timestamp, created_timestamp) <= 5 then prev_timestamp
                        else created_timestamp
                    end
            ) = 1
),

trial_types as (
    select
        instance_account_id,
        listagg(distinct sku_name, ' + ') within group (order by sku_name) as trial_type
    from
        ranked_trials
    group by
        instance_account_id
)

select *
from trial_types
limit 10

