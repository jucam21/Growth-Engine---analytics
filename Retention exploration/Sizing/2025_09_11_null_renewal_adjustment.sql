---------------------------------------------------
--- Investigate renewal dates that are Null



---------------------------------------------------
--- Weird account ids

select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp,
    account_id
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
order by 1


select *
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
limit 10


select 
    account_id,
    count(*) as tot_loads,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as min_timestamp,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as max_timestamp
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
group by 1
order by 2 desc



select *
from propagated_foundational.product_agent_info.dim_agent_emails_bcv
where agent_id = 345821841



select *
from propagated_foundational.product_agent_info.dim_agent_emails_bcv
--limit 10
where agent_id = 387329482878



select *
from propagated_foundational.product_agent_info.dim_agent_emails_bcv
where agent_id = 14877898449436



select
    account_id,
    timestamp
from cleansed.segment_billing.segment_billing_cart_loaded_scd2
where
    not paid_customer
    and account_id = 25681793




unique_count_work_modal_2_click


select *
from propagated_cleansed.segment_support.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
order by original_timestamp 


---------------------------------------------------
--- Investigate renewal dates that are Null


select *
from sandbox.juan_salgado.ge_dashboard_test
where 
    unique_count_work_modal_2_click is not null
    and subscription_renewal_date is null
    and account_category is null




with import_finance_subscriptions as (
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
)

select *
from import_finance_subscriptions
where zendesk_account_id = 25627661




25439461
25627661
25627671
