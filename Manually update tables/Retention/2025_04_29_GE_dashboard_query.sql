--- Query to update table

----------------------------------------------------------
--- Create table with all data

--create or replace table sandbox.juan_salgado.ge_dashboard_test_more_vars as
create or replace table sandbox.juan_salgado.ge_dashboard_test as

-- Step 1: Segment events

-- First, counting how many events are triggered daily per account_id, offer_id and promo_code_id
-- Unique count: uses account_id and will use function count distinct in Tableau
-- Total count: counts total interactions per day and will use function sum in Tableau
-- Prompt events

--- Added UNION code to take into account both segment event sources

with prompt_load_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
        from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_load_1_scd2
    union all
    select 
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    -- Date when support segment started capturing data
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

prompt_click_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2
    union all
    select 
       --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

prompt_dismiss_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_dismiss_offer_1_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_dismiss_offer_1_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

work_modal_1_click_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_couponmodal_claim_offer_click_2_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_couponmodal_claim_offer_click_2_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

work_modal_1_dismiss_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_couponmodal_dismiss_offer_1_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_couponmodal_dismiss_offer_1_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

work_modal_2_click_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

work_modal_2_dismiss_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_dismiss_offer_1_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_couponmodal_work_modal_2_dismiss_offer_1_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

work_modal_2_go_back_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_couponmodal_go_back_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_couponmodal_go_back_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

follow_up_close_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_close_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_couponmodal_subscription_submitted_close_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

follow_up_dismiss_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_dismiss_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_couponmodal_subscription_submitted_dismiss_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

follow_up_subscription_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_subscription_details_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then instance.instance_account_id
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_couponmodal_subscription_submitted_subscription_details_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
),

prompt_load as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        prompt_load_union
    group by all
),
prompt_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        prompt_click_union
    group by all
),
prompt_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        prompt_dismiss_union
    group by all
),
work_modal_1_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        work_modal_1_click_union
    group by all
),
work_modal_1_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        work_modal_1_dismiss_union
    group by all
),
work_modal_2_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        work_modal_2_click_union
    group by all
),
work_modal_2_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        work_modal_2_dismiss_union
    group by all
),
work_modal_2_go_back as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        work_modal_2_go_back_union
    group by all
),
follow_up_close as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        follow_up_close_union
    group by all
),
follow_up_dismiss as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        follow_up_dismiss_union
    group by all
),
follow_up_subscription as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        follow_up_subscription_union
    group by all
),
segment_events_all_tmp as (
    select
        prompt_load_.date as loaded_date,
        prompt_load_.account_id,
        prompt_load_.offer_id,
        prompt_load_.promo_code_id,
        prompt_load_.total_count as total_count_prompt_loads,
        prompt_load_.unique_count as unique_count_prompt_loads,
        prompt_click_.total_count as total_count_prompt_clicks,
        prompt_click_.unique_count as unique_count_prompt_clicks,
        prompt_dismiss_.total_count as total_count_prompt_dismiss,
        prompt_dismiss_.unique_count as unique_count_prompt_dismiss,
        work_modal_1_click_.total_count as total_count_work_modal_1_click,
        work_modal_1_click_.unique_count as unique_count_work_modal_1_click,
        work_modal_1_dismiss_.total_count as total_count_work_modal_1_dismiss,
        work_modal_1_dismiss_.unique_count as unique_count_work_modal_1_dismiss,
        work_modal_2_click_.total_count as total_count_work_modal_2_click,
        work_modal_2_click_.unique_count as unique_count_work_modal_2_click,
        work_modal_2_dismiss_.total_count as total_count_work_modal_2_dismiss,
        work_modal_2_dismiss_.unique_count as unique_count_work_modal_2_dismiss,
        work_modal_2_go_back_.total_count as total_count_work_modal_2_go_back,
        work_modal_2_go_back_.unique_count as unique_count_work_modal_2_go_back,
        follow_up_close_.total_count as total_count_follow_up_close,
        follow_up_close_.unique_count as unique_count_follow_up_close,
        follow_up_dismiss_.total_count as total_count_follow_up_dismiss,
        follow_up_dismiss_.unique_count as unique_count_follow_up_dismiss,
        follow_up_subscription_.total_count
            as total_count_follow_up_subscription,
        follow_up_subscription_.unique_count
            as unique_count_follow_up_subscription
    from prompt_load as prompt_load_
        left join prompt_click as prompt_click_
            on
                prompt_load_.account_id = prompt_click_.account_id
                and prompt_load_.date = prompt_click_.date
                and prompt_load_.offer_id = prompt_click_.offer_id
                and prompt_load_.promo_code_id = prompt_click_.promo_code_id
        left join prompt_dismiss as prompt_dismiss_
            on
                prompt_load_.account_id = prompt_dismiss_.account_id
                and prompt_load_.date = prompt_dismiss_.date
                and prompt_load_.offer_id = prompt_dismiss_.offer_id
                and prompt_load_.promo_code_id = prompt_dismiss_.promo_code_id
        left join work_modal_1_click as work_modal_1_click_
            on
                prompt_load_.account_id = work_modal_1_click_.account_id
                and prompt_load_.date = work_modal_1_click_.date
                and prompt_load_.offer_id = work_modal_1_click_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_1_click_.promo_code_id
        left join work_modal_1_dismiss as work_modal_1_dismiss_
            on
                prompt_load_.account_id = work_modal_1_dismiss_.account_id
                and prompt_load_.date = work_modal_1_dismiss_.date
                and prompt_load_.offer_id = work_modal_1_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_1_dismiss_.promo_code_id
        left join work_modal_2_click as work_modal_2_click_
            on
                prompt_load_.account_id = work_modal_2_click_.account_id
                and prompt_load_.date = work_modal_2_click_.date
                and prompt_load_.offer_id = work_modal_2_click_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_click_.promo_code_id
        left join work_modal_2_dismiss as work_modal_2_dismiss_
            on
                prompt_load_.account_id = work_modal_2_dismiss_.account_id
                and prompt_load_.date = work_modal_2_dismiss_.date
                and prompt_load_.offer_id = work_modal_2_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_dismiss_.promo_code_id
        left join work_modal_2_go_back as work_modal_2_go_back_
            on
                prompt_load_.account_id = work_modal_2_go_back_.account_id
                and prompt_load_.date = work_modal_2_go_back_.date
                and prompt_load_.offer_id = work_modal_2_go_back_.offer_id
                and prompt_load_.promo_code_id
                = work_modal_2_go_back_.promo_code_id
        left join follow_up_close as follow_up_close_
            on
                prompt_load_.account_id = follow_up_close_.account_id
                and prompt_load_.date = follow_up_close_.date
                and prompt_load_.offer_id = follow_up_close_.offer_id
                and prompt_load_.promo_code_id = follow_up_close_.promo_code_id
        left join follow_up_dismiss as follow_up_dismiss_
            on
                prompt_load_.account_id = follow_up_dismiss_.account_id
                and prompt_load_.date = follow_up_dismiss_.date
                and prompt_load_.offer_id = follow_up_dismiss_.offer_id
                and prompt_load_.promo_code_id
                = follow_up_dismiss_.promo_code_id
        left join follow_up_subscription as follow_up_subscription_
            on
                prompt_load_.account_id = follow_up_subscription_.account_id
                and prompt_load_.date = follow_up_subscription_.date
                and prompt_load_.offer_id = follow_up_subscription_.offer_id
                and prompt_load_.promo_code_id
                = follow_up_subscription_.promo_code_id
),
first_loaded as (
    select
        account_id,
        min(loaded_date) as first_loaded_date
    from segment_events_all_tmp
    group by 1
),
segment_events_all as (
    select
        a.*,
        b.first_loaded_date,
        datediff('day', b.first_loaded_date, a.loaded_date)
            as days_since_first_loaded,
        b.first_loaded_date as date_to_join,
        date(b.first_loaded_date) - 1 as date_to_join_cc
    from segment_events_all_tmp as a
        left join first_loaded as b
            on a.account_id = b.account_id
),

-- Step 2: Join additional variables from the previous day of event load

---- State, category and account age
segment_events_all_states as (
    select
        a.*,
        b.instance_account_state as account_state,
        -- Harcoding these instance as internal instances
        case 
            when a.account_id in (24905253, 24853211, 25627661, 25439461, 25627671) then 'Internal Instance'
            else b.instance_account_derived_type 
        end as account_category,
        datediff(
            day, b.instance_account_created_timestamp::date, a.date_to_join
        ) as account_age_in_days
    from segment_events_all as a
        left join
            foundational.customer.dim_instance_accounts_daily_snapshot as b
            on
                a.account_id = b.instance_account_id
                and a.date_to_join = b.source_snapshot_date
),

---- Renewal date and subscription age

/* 
import_finance_subscriptions as (
    select
        distinct 
        finance.service_date,
        snapshot.instance_account_id as zendesk_account_id,
        finance.subscription_status,
        finance.subscription_term_start_date as subscription_start_date,
        finance.subscription_term_end_date as subscription_renewal_date,
        datediff(day, finance.subscription_term_start_date, finance.service_date)
            as subscription_age_in_days,
        datediff(day, finance.service_date, finance.subscription_term_end_date)
            as days_to_subscription_renewal,
        row_number() over (
            partition by finance.service_date, zendesk_account_id
            order by finance.subscription_term_end_date asc
        ) as rank
    from
        foundational.finance.fact_recurring_revenue_daily_snapshot_enriched
            as finance
        inner join
            foundational.customer.entity_mapping_daily_snapshot as snapshot
            on
                finance.billing_account_id = snapshot.billing_account_id
                and finance.service_date = snapshot.source_snapshot_date
    where finance.service_date >= '2025-01-01'
        --finance.subscription_status = 'Active'
        -- Have to validate this filter
        -- Account 10214376 had subscription_status = 'Expired' before April 18, the day it redeemed the offer
        and (
            finance.subscription_kind = 'Primary'
            or finance.subscription_kind is null
        )
        and finance.service_date >= '2025-01-01'
    qualify rank = 1
),
*/

import_finance_subscriptions as (
    select
        distinct 
        finance.service_date,
        snapshot.instance_account_id as zendesk_account_id,
        finance.subscription_term_start_date as subscription_start_date,
        finance.subscription_term_end_date as subscription_renewal_date,
        datediff(day, finance.subscription_term_start_date, finance.service_date)
            as subscription_age_in_days,
        datediff(day, finance.service_date, finance.subscription_term_end_date)
            as days_to_subscription_renewal,
        row_number() over (
            partition by finance.service_date, zendesk_account_id
            order by finance.subscription_term_end_date asc
        ) as rank
    from
        foundational.finance.fact_recurring_revenue_daily_snapshot_enriched
            as finance
        inner join
            foundational.customer.entity_mapping_daily_snapshot as snapshot
            on
                finance.billing_account_id = snapshot.billing_account_id
                and finance.service_date = snapshot.source_snapshot_date
    where
        -- Removed filter on subscription_status because it was showing some accounts as "Expired"
        --finance.subscription_status = 'Active'
        ( 
            finance.subscription_kind = 'Primary'
            or finance.subscription_kind is null
        )
        and finance.service_date >= '2025-01-01'
    qualify rank = 1
),

segment_events_all_renewal_dates as (
    select
        a.*,
        b.subscription_start_date,
        b.subscription_renewal_date,
        b.subscription_age_in_days,
        b.days_to_subscription_renewal,
        datediff(day, b.subscription_start_date, b.subscription_renewal_date)
            as subscription_term_length,
        case
            when
                subscription_term_length >= 28
                and subscription_term_length <= 32
                then 'monthly'
            when
                subscription_term_length >= 360
                and subscription_term_length <= 370
                then 'annual'
            else 'other'
        end as subscription_term_type
    from segment_events_all_states as a
        left join
            import_finance_subscriptions as b
            on
                a.account_id = b.zendesk_account_id
                and a.date_to_join = b.service_date
),

---- Region, market segment and sales model
snapshot as (
    select distinct
        source_snapshot_date,
        crm_account_id,
        instance_account_id
    from foundational.customer.entity_mapping_daily_snapshot
    where source_snapshot_date >= '2025-01-01'
),

segment_events_all_region_mkt_segment as (
    select
        a.*,
        snapshot.crm_account_id,
        b.crm_region as region,
        b.pro_forma_market_segment as market_segment,
        b.crm_sales_model as sales_model,
        b.crm_employee_range as employee_count_range
    from segment_events_all_renewal_dates as a
        left join snapshot
            on
                a.account_id = snapshot.instance_account_id
                and a.date_to_join = snapshot.source_snapshot_date
        left join
            foundational.customer.dim_crm_accounts_daily_snapshot as b
            on
                snapshot.crm_account_id = b.crm_account_id
                and a.date_to_join = b.source_snapshot_date
),


---- Instance & CRM ARR
import_finance_recurring_revenue_instance_arr as (
    select
        finance.service_date,
        snapshot.instance_account_id as zendesk_account_id,
        sum(finance.net_arr_usd) as net_arr_usd
    from foundational.finance.fact_recurring_revenue_daily_snapshot_enriched as finance
    left join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on finance.billing_account_id = snapshot.billing_account_id
        and finance.service_date = snapshot.source_snapshot_date
    where finance.service_date >= '2025-01-01'
    group by all
),

import_finance_recurring_revenue_crm_arr as (
    select
        service_date,
        crm_account_id,
        sum(net_arr_usd) as net_arr_usd
    from foundational.finance.FACT_RECURRING_REVENUE_DAILY_SNAPSHOT_ENRICHED
    where service_date >= '2025-01-01'
    group by crm_account_id, service_date
),

segment_events_all_arr as (
    select
        a.*,
        instance_arr.net_arr_usd as net_arr_usd_instance,
        crm_arr.net_arr_usd as net_arr_usd_crm,
        case
            when net_arr_usd_crm is null
                then '0. NULL'
            when net_arr_usd_crm < 5000 
                then '1. < 5K'
            when
                net_arr_usd_crm >= 5000 and net_arr_usd_crm < 10000
                then '2. 5K-10K'
            when
                net_arr_usd_crm >= 10000 and net_arr_usd_crm < 25000
                then '3. 10K-25K'
            when
                net_arr_usd_crm >= 25000 and net_arr_usd_crm < 50000
                then '4. 25K-50K'
            when
                net_arr_usd_crm >= 50000 and net_arr_usd_crm < 100000
                then '5. 50K-100K'
            when
                net_arr_usd_crm >= 100000 
                then '6. >100K'
            else '7. other'
        end as crm_arr_range,
        case
            when net_arr_usd_instance is null
                then '0. NULL'
            when net_arr_usd_instance < 5000 
                then '1. < 5K'
            when
                net_arr_usd_instance >= 5000 and net_arr_usd_instance < 10000
                then '2. 5K-10K'
            when
                net_arr_usd_instance >= 10000 and net_arr_usd_instance < 25000
                then '3. 10K-25K'
            when
                net_arr_usd_instance >= 25000 and net_arr_usd_instance < 50000
                then '4. 25K-50K'
            when
                net_arr_usd_instance >= 50000 and net_arr_usd_instance < 100000
                then '5. 50K-100K'
            when
                net_arr_usd_instance >= 100000 
                then '6. >100K'
            else '7. other'
        end as instance_arr_range
    from segment_events_all_region_mkt_segment as a
        left join import_finance_recurring_revenue_instance_arr instance_arr
            on
                a.account_id = instance_arr.zendesk_account_id
                and a.date_to_join = instance_arr.service_date
        left join import_finance_recurring_revenue_crm_arr crm_arr
            on
                a.crm_account_id = crm_arr.crm_account_id
                and a.date_to_join = crm_arr.service_date
),

-- Step 2.2 Churn driver
-- Has to be adjusted to use the closest date to the segment event loaded date.
-- This is an aproximation to the real date where C/C data was pulled

--- Joining on previous day of first load, because I fould a lot of nulls when joining on the same day
segment_events_all_cc as (
    select
        a.*,
        b.driver_1 as churn_driver,
        b.tool_tip_1 as churn_tooltip,
        b.predicted_probability
    from segment_events_all_arr as a
        left join
            functional.eda_ml_outcome.churn_score_predictions as b
            on
                a.account_id = b.instance_account_id
                and a.date_to_join_cc = b.source_snapshot_date
),

--- Step 2.3 Extracting SKUs, addons & suite/support seats
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

-- Product SKUs have a plan associated
product_skus as (
    select
        account_id,
        listagg(concat(name, '_', plan), ', ') within GROUP (ORDER BY concat(name, '_', plan)) AS sku_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is not null
    group by account_id

),

-- Addon SKUs do not have a plan associated
addons_skus as (
    select
        account_id,
        listagg(name, ', ') within GROUP (ORDER BY name) AS addon_mix
    from import_skus_with_parsed_settings
    where 
        state = 'subscribed'
        and plan is null
    group by account_id
),

support_seats as (
    select
        account_id,
        max(max_agents) as max_support_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%support%'
    group by account_id

),

suite_seats as (
    select
        account_id,
        max(max_agents) as max_suite_seats
    from import_skus_with_parsed_settings
    where
        state = 'subscribed'
        and lower(name) like '%suite%'
    group by account_id

),

main_skus as (
    select
        product_.*,
        addon_.addon_mix,
        support_.max_support_seats,
        suite_.max_suite_seats,
        --- Create flags for seat type and number of seats
        case 
            when 
                product_.sku_mix like '%support%' 
                and product_.sku_mix not like '%suite%' 
                then 'support'
            when 
                product_.sku_mix like '%suite%' 
                then 'suite'
            else 'other'
        end as sku_type,
        case 
            when sku_type = 'support' then support_.max_support_seats 
            when sku_type = 'suite' then suite_.max_suite_seats 
            else null
        end as seats_capacity
    from product_skus as product_
        left join addons_skus as addon_
            on product_.account_id = addon_.account_id
        left join support_seats as support_
            on product_.account_id = support_.account_id
        left join suite_seats as suite_
            on product_.account_id = suite_.account_id
),

-- Step 2.4 Payment method

payment_method as (
    select 
        TRY_CAST(accounts.zendesk_account_id_c as INTEGER)
            as instance_account_id,
        payment_methods.type as payment_method_name
    from cleansed.zuora.zuora_accounts_bcv accounts
        left join cleansed.zuora.ZUORA_PAYMENT_METHODS_BCV payment_methods
            on accounts.default_payment_method_id = payment_methods.id
),

-- Step 2.5 Zuora data

redeemed_zuora as (
    select 
        mapping.instance_account_id,
        zuora.up_to_periods,
        zuora.billing_period,
        zuora.charge_model,
        tiers.currency,
        tiers.discount_amount,
        tiers.discount_percentage,
        SPLIT_PART(zuora.description, '-', 1) as coupon_id,
        count(*) as total_records,
        total_records - 1 as direct_charges,
        case when direct_charges > 0 then 1 else 0 end as coupon_redeemed
    from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
        left join
            foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
            on zuora.account_id = mapping.billing_account_id
        left join cleansed.zuora.zuora_subscriptions_bcv as subscription
            on zuora.subscription_id = subscription.id
        left join cleansed.zuora.zuora_rate_plan_charge_tiers_bcv as tiers
            on zuora.id = tiers.rate_plan_charge_id
        inner join
            (select distinct account_id from segment_events_all_arr) accounts
            on mapping.instance_account_id = accounts.account_id
    where
        zuora.is_last_segment = true
        and subscription.status in ('Active', 'Expired')
        and zuora.created_date >= '2025-01-01'
        and LOWER(coupon_id) like '%save%'
    group by
        all
),


-- Step 3: Join all data

-- Step 3.1: Join additional variables data

main as (
    select 
        segment_events_all_.*,
        -- SKU data
        main_skus_.sku_mix,
        main_skus_.addon_mix,
        main_skus_.sku_type,
        main_skus_.seats_capacity,
        -- Payment method
        payment_method_.payment_method_name,
        -- Zuora data
        redeemed_zuora_.up_to_periods zuora_up_to_periods,
        redeemed_zuora_.billing_period zuora_billing_period,
        redeemed_zuora_.charge_model zuora_charge_model,
        redeemed_zuora_.currency zuora_currency,
        redeemed_zuora_.discount_amount zuora_discount_amount,
        redeemed_zuora_.discount_percentage zuora_discount_percentage,
        redeemed_zuora_.total_records as zuora_total_records,
        redeemed_zuora_.direct_charges as zuora_effective_charges,
        redeemed_zuora_.coupon_redeemed as zuora_coupon_redeemed,
        case 
            when 
                redeemed_zuora_.coupon_redeemed = 1 
                then segment_events_all_.account_id 
                else null 
        end as zuora_unique_coupon_redeemed,
        ---- Adjust for fixed amount coupons
        case 
            when 
                zuora_billing_period = 'Annual' 
                then zuora_up_to_periods * (zuora_discount_percentage / 100) * segment_events_all_.net_arr_usd_instance
            when
                zuora_billing_period = 'Month'
                then (zuora_up_to_periods) * (zuora_discount_percentage / 100) * (segment_events_all_.net_arr_usd_instance / 12)
            else null
        end as zuora_total_discount_amount,
        case 
            when 
                zuora_billing_period = 'Annual' 
                then zuora_effective_charges * (zuora_discount_percentage / 100) * segment_events_all_.net_arr_usd_instance
            when
                zuora_billing_period = 'Month'
                then (zuora_effective_charges) * (zuora_discount_percentage / 100) * (segment_events_all_.net_arr_usd_instance / 12)
            else null
        end as zuora_effective_discount_amount
        
    from segment_events_all_cc segment_events_all_
        left join main_skus main_skus_
            on segment_events_all_.account_id = main_skus_.account_id
        left join payment_method payment_method_
            on segment_events_all_.account_id = payment_method_.instance_account_id
        left join redeemed_zuora redeemed_zuora_
            on segment_events_all_.account_id = redeemed_zuora_.instance_account_id
            and lower(segment_events_all_.promo_code_id) = lower(redeemed_zuora_.coupon_id)
)

select *, current_date() as updated_at
from main;
