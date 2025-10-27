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


--- Validate last event date


select max(original_timestamp) as last_event_date
from cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_load_1_scd2


select max(original_timestamp) as last_event_date
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2



select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    account_id
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
order by 1 desc
limit 100




select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    account_id,
    user_id
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
order by 1 desc
limit 20



select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    account_id,
    user_id
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
where user_id is null
order by 1 desc
limit 20





select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) as original_timestamp_pt,
    account_id,
    user_id
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
where
    account_id is null
    and user_id is null
order by 1 desc
limit 20




select *
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
where user_id = 40067450711195



select *
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
where user_id = 40067450711195



--- It seems only 6-8 account ids lenghts are valid
with lenghts as (
    select 
        account_id,
        length(account_id) as id_length
    from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
    where 
        account_id is not null
        --and convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-20'
    order by 2 desc
)

select 
    id_length,
    count(*) as tot_ids,
    count(distinct account_id) as tot_unique_ids
from lenghts
group by 1
order by 1 


select distinct *
from lenghts
where id_length = 10
limit 10







select 
    loaded_date,
    account_id
from sandbox.juan_salgado.ge_dashboard_test
order by 1 desc
limit 100



select 
    count(*) as tot_rows,
    count(distinct account_id) as tot_accounts,
    count(case when account_id is null then 1 else null end) as tot_null_account_ids
from sandbox.juan_salgado.ge_dashboard_test




select 
    account_id,
    length(account_id) as id_length,
    count(*) as tot_loads,
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
where 
    date(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) = '2025-10-09'
group by all
order by 2





select *
from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2
where user_id is null



------------------------------------------------
--- Checking account id join logic


with prompt_load as (
    select 
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then coalesce(instance.instance_account_id, user_id.instance_account_id)
            --- Between 6-8 chars is the length of a valid account id. Majority are 8
            when length(segment.account_id) >= 6 and length(segment.account_id) <= 8 then segment.account_id
            when length(segment.account_id) < 6 or length(segment.account_id) > 8 then instance.instance_account_id
            else null
        end as account_id_1,
        user_id.instance_account_id as account_id_2,
        segment.user_id,
        offer_id,
        promo_code_id,
        original_timestamp 
    from propagated_cleansed.segment_support.growth_engine_adminhomebanner1_prompt_load_1_scd2 segment
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv instance
        on segment.account_id = instance.agent_id
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv user_id
        on segment.user_id = user_id.agent_id
    -- Date when support segment started capturing data
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
)


select *
from prompt_load
where account_id_2 is null
limit 20



select 
    count(*) as tot_rows,
    count(distinct account_id_1) as tot_account_id_1,
    count(distinct account_id_2) as tot_account_id_2,
    sum(case when account_id_1 is null then 1 else null end) as tot_null_account_ids_1,
    sum(case when account_id_2 is null then 1 else null end) as tot_null_account_ids_2
from prompt_load





select *
from propagated_foundational.product_agent_info.dim_agent_emails_bcv
where agent_id = 345821841



select *
from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot
where agent_id = 345821841
limit 10







------------------------------------------------
--- Zuora coupon redemptions


select *
from sandbox.juan_salgado.ge_dashboard_test
where zuora_unique_coupon_redeemed is not null 



select 
    count(distinct account_id) as tot_accounts,
    count(distinct unique_count_work_modal_2_click) as final_confirmation,
    count(distinct zuora_unique_coupon_redeemed) as tot_coupons
from sandbox.juan_salgado.ge_dashboard_test
where account_category != 'Internal Instance' or account_category is null




select *
from sandbox.juan_salgado.ge_dashboard_test
where 
    zuora_unique_coupon_redeemed is not null 
    and unique_count_work_modal_2_click is null






select *
from sandbox.juan_salgado.ge_dashboard_test
where account_id = 20630105








with redeemed_zuora as (
    select 
        mapping.instance_account_id,
        zuora.created_date,
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
    where
        zuora.is_last_segment = true
        and subscription.status in ('Active', 'Expired')
        and zuora.created_date >= '2025-01-01'
        and LOWER(coupon_id) like '%save%'
        and mapping.instance_account_id = 20630105
    group by
        all
)

select *
from redeemed_zuora





select *
from propagated_cleansed.segment_support.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
where date_trunc('day', original_timestamp) = '2025-10-16'
order by original_timestamp








with work_modal_2_click_union as (
    select account_id,
        offer_id,
        promo_code_id,
        original_timestamp  from cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
    union all
    select
        --- Case to pull account id from either agent emails or directly from segment table
        case
            when segment.account_id is null then coalesce(instance.instance_account_id, user_id.instance_account_id)
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
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv user_id
        on segment.user_id = user_id.agent_id
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
)

select *
from work_modal_2_click_union
where account_id = 20630105








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
            when segment.account_id is null then coalesce(instance.instance_account_id, user_id.instance_account_id)
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
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv user_id
        on segment.user_id = user_id.agent_id
    -- Date when support segment started capturing data
    where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-14' 
)

select *
from prompt_load_union
where account_id = 20630105







