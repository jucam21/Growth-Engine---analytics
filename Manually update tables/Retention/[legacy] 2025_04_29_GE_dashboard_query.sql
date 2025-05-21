--- Query to update table

----------------------------------------------------------
--- Create table with all data

create or replace table sandbox.juan_salgado.ge_dashboard_test as

-- Step 1: Segment events

-- First, counting how many events are triggered daily per account_id, offer_id and promo_code_id
-- Unique count: uses account_id and will use function count distinct in Tableau
-- Total count: counts total interactions per day and will use function sum in Tableau
-- Prompt events
with prompt_load as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_load_1_scd2
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
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_claim_offer_click_1_scd2
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
        cleansed.segment_central_admin.growth_engine_adminhomebanner1_prompt_dismiss_offer_1_scd2
    group by all
),

-- Work modal 1 events
work_modal_1_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_claim_offer_click_2_scd2
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
        cleansed.segment_central_admin.growth_engine_couponmodal_dismiss_offer_1_scd2
    group by all
),

-- Work modal 2 events
work_modal_2_click as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
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
        cleansed.segment_central_admin.growth_engine_couponmodal_work_modal_2_dismiss_offer_1_scd2
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
        cleansed.segment_central_admin.growth_engine_couponmodal_go_back_scd2
    group by all
),

-- Follow up modal events
follow_up_close as (
    select
        account_id,
        offer_id,
        promo_code_id,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_close_scd2
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
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_dismiss_scd2
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
        cleansed.segment_central_admin.growth_engine_couponmodal_subscription_submitted_subscription_details_scd2
    group by all
),

-- Joining all events
-- Joining using date (day), account_id, offer_id and promo_code_id as keys and
-- left joining all events against prompt_load, since is the starting point of the flow
segment_events_all as (
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

-- Step 2: adding additional variables

-- Step 2.1 Churn driver
-- Has to be adjusted to use the closest date to the segment event loaded date.
-- This is an aproximation to the real date where C/C data was pulled

cc_model as (
    select 
        instance_account_id,
        driver_1 as churn_driver,
        tool_tip_1 as churn_tooltip,
        row_number() over (
            partition by instance_account_id
            order by source_snapshot_date desc
        ) as rank
    from dev_functional.growth_engine.dim_growth_engine_churn_predictions
    qualify rank = 1
),

--- Step 2.2 Extracting data from GE accounts table
ge_table_vars as (
    select
        zendesk_account_id,
        account_age_in_days,
        region,
        employee_count_range,
        days_to_subscription_renewal,
        market_segment,
        net_arr_usd_crm,
        net_arr_usd_instance,
        account_state,
        account_category,
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
    from functional.growth_engine.dim_growth_engine_customer_accounts
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
        suite_.max_suite_seats
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

-- Step 3: Join all data

-- Step 3.1: Join additional variables data

joined_additional_vars as (
    select *
    from ge_table_vars ge_table_vars_
        left join cc_model cc_model_
            on ge_table_vars_.zendesk_account_id = cc_model_.instance_account_id
        left join main_skus main_skus_
            on ge_table_vars_.zendesk_account_id = main_skus_.account_id
        left join payment_method payment_method_
            on ge_table_vars_.zendesk_account_id = payment_method_.instance_account_id
),

-- Step 3.2: Join all data

main as (
    select 
        segment_events_all_.*,
        joined_additional_vars_.account_category,
        joined_additional_vars_.account_state,
        joined_additional_vars_.churn_driver,
        joined_additional_vars_.churn_tooltip,
        joined_additional_vars_.account_age_in_days,
        joined_additional_vars_.region,
        joined_additional_vars_.employee_count_range,
        joined_additional_vars_.days_to_subscription_renewal,
        joined_additional_vars_.market_segment,
        joined_additional_vars_.crm_arr_range,
        joined_additional_vars_.instance_arr_range,
        joined_additional_vars_.net_arr_usd_crm,
        joined_additional_vars_.net_arr_usd_instance,
        joined_additional_vars_.sku_mix,
        joined_additional_vars_.addon_mix,
        joined_additional_vars_.max_support_seats,
        joined_additional_vars_.max_suite_seats,
        joined_additional_vars_.payment_method_name
    from segment_events_all segment_events_all_
    left join joined_additional_vars joined_additional_vars_
        on segment_events_all_.account_id = joined_additional_vars_.zendesk_account_id
)

select *
from main;
