--- Query to update table

----------------------------------------------------------
--- Create table with all data

-----------------------------------------------------------
-- Main Query

--- Selecting only 1 record per account_id
--- For redeemed accounts, we take the record with the redemption
--- For non-redeemed accounts, we take the most recent record

create or replace table sandbox.juan_salgado.ge_growth_metrics_test as

with redeemed as (
    select
        *,
        'Redeemed' as data_type
    from sandbox.juan_salgado.ge_dashboard_test
    where unique_count_work_modal_2_click is not null
),

not_redeemed as (
    select a.*,
row_number() over (
            partition by a.account_id
            order by a.loaded_date desc
        ) as rank,
        'Not redeemed' as data_type
from sandbox.juan_salgado.ge_dashboard_test as a
left join redeemed as b
on a.account_id = b.account_id
where b.account_id is null
qualify rank = 1
),

unique_obs as (
    select
        loaded_date,
        account_id,
        offer_id,
        promo_code_id,
        total_count_prompt_loads,
        unique_count_prompt_loads,
        total_count_prompt_clicks,
        unique_count_prompt_clicks,
        total_count_prompt_dismiss,
        unique_count_prompt_dismiss,
        total_count_work_modal_1_click,
        unique_count_work_modal_1_click,
        total_count_work_modal_1_dismiss,
        unique_count_work_modal_1_dismiss,
        total_count_work_modal_2_click,
        unique_count_work_modal_2_click,
        total_count_work_modal_2_dismiss,
        unique_count_work_modal_2_dismiss,
        total_count_work_modal_2_go_back,
        unique_count_work_modal_2_go_back,
        total_count_follow_up_close,
        unique_count_follow_up_close,
        total_count_follow_up_dismiss,
        unique_count_follow_up_dismiss,
        total_count_follow_up_subscription,
        unique_count_follow_up_subscription,
        first_loaded_date,
        days_since_first_loaded,
        date_to_join,
        account_state,
        account_category,
        account_age_in_days,
        subscription_start_date,
        subscription_renewal_date,
        subscription_age_in_days,
        days_to_subscription_renewal,
        subscription_term_length,
        subscription_term_type,
        crm_account_id,
        region,
        market_segment,
        sales_model,
        employee_count_range,
        net_arr_usd_instance,
        net_arr_usd_crm,
        crm_arr_range,
        instance_arr_range,
        churn_driver,
        churn_tooltip,
        predicted_probability,
        sku_mix,
        addon_mix,
        max_support_seats,
        max_suite_seats,
        payment_method_name,
        zuora_coupon_redeemed,
        zuora_unique_coupon_redeemed,
        zuora_charge_model,
        zuora_total_discount_amount,
        zuora_effective_discount_amount,
        zuora_total_records,
        zuora_effective_charges,
        data_type
    from redeemed
    union all
    select
        loaded_date,
        account_id,
        offer_id,
        promo_code_id,
        total_count_prompt_loads,
        unique_count_prompt_loads,
        total_count_prompt_clicks,
        unique_count_prompt_clicks,
        total_count_prompt_dismiss,
        unique_count_prompt_dismiss,
        total_count_work_modal_1_click,
        unique_count_work_modal_1_click,
        total_count_work_modal_1_dismiss,
        unique_count_work_modal_1_dismiss,
        total_count_work_modal_2_click,
        unique_count_work_modal_2_click,
        total_count_work_modal_2_dismiss,
        unique_count_work_modal_2_dismiss,
        total_count_work_modal_2_go_back,
        unique_count_work_modal_2_go_back,
        total_count_follow_up_close,
        unique_count_follow_up_close,
        total_count_follow_up_dismiss,
        unique_count_follow_up_dismiss,
        total_count_follow_up_subscription,
        unique_count_follow_up_subscription,
        first_loaded_date,
        days_since_first_loaded,
        date_to_join,
        account_state,
        account_category,
        account_age_in_days,
        subscription_start_date,
        subscription_renewal_date,
        subscription_age_in_days,
        days_to_subscription_renewal,
        subscription_term_length,
        subscription_term_type,
        crm_account_id,
        region,
        market_segment,
        sales_model,
        employee_count_range,
        net_arr_usd_instance,
        net_arr_usd_crm,
        crm_arr_range,
        instance_arr_range,
        churn_driver,
        churn_tooltip,
        predicted_probability,
        sku_mix,
        addon_mix,
        max_support_seats,
        max_suite_seats,
        payment_method_name,
        zuora_coupon_redeemed,
        zuora_unique_coupon_redeemed,
        zuora_charge_model,
        zuora_total_discount_amount,
        zuora_effective_discount_amount,
        zuora_total_records,
        zuora_effective_charges,
        data_type
    from not_redeemed
    union all
    select
        loaded_date,
        account_id,
        offer_id,
        promo_code_id,
        total_count_prompt_loads,
        unique_count_prompt_loads,
        total_count_prompt_clicks,
        unique_count_prompt_clicks,
        total_count_prompt_dismiss,
        unique_count_prompt_dismiss,
        total_count_work_modal_1_click,
        unique_count_work_modal_1_click,
        total_count_work_modal_1_dismiss,
        unique_count_work_modal_1_dismiss,
        total_count_work_modal_2_click,
        unique_count_work_modal_2_click,
        total_count_work_modal_2_dismiss,
        unique_count_work_modal_2_dismiss,
        total_count_work_modal_2_go_back,
        unique_count_work_modal_2_go_back,
        total_count_follow_up_close,
        unique_count_follow_up_close,
        total_count_follow_up_dismiss,
        unique_count_follow_up_dismiss,
        total_count_follow_up_subscription,
        unique_count_follow_up_subscription,
        first_loaded_date,
        days_since_first_loaded,
        date_to_join,
        account_state,
        account_category,
        account_age_in_days,
        subscription_start_date,
        subscription_renewal_date,
        subscription_age_in_days,
        days_to_subscription_renewal,
        subscription_term_length,
        subscription_term_type,
        crm_account_id,
        region,
        market_segment,
        sales_model,
        employee_count_range,
        net_arr_usd_instance,
        net_arr_usd_crm,
        crm_arr_range,
        instance_arr_range,
        churn_driver,
        churn_tooltip,
        predicted_probability,
        sku_mix,
        addon_mix,
        max_support_seats,
        max_suite_seats,
        payment_method_name,
        zuora_coupon_redeemed,
        zuora_unique_coupon_redeemed,
        zuora_charge_model,
        zuora_total_discount_amount,
        zuora_effective_discount_amount,
        zuora_total_records,
        zuora_effective_charges,
        'Baseline 1' as data_type
    from not_redeemed
),

max_date_finance as (
    select
        max(service_date) as last_date_finance
    from foundational.finance.fact_recurring_revenue_bcv_enriched
),

--- BCV ARR for renewal rate, churn rate, and expansion rate
import_finance_recurring_revenue_instance_arr as (
    select
        snapshot_bcv.instance_account_id as zendesk_account_id,
        sum(finance.net_arr_usd) as net_arr_usd
    from foundational.finance.fact_recurring_revenue_bcv_enriched as finance
        left join
            foundational.customer.entity_mapping_daily_snapshot_bcv
                as snapshot_bcv
            on finance.billing_account_id = snapshot_bcv.billing_account_id
    group by all
),

unique_obs_arr_bcv as (
    select
        a.*,
        c.last_date_finance,
        b.net_arr_usd as instance_net_arr_usd_bcv
    from unique_obs as a
        left join import_finance_recurring_revenue_instance_arr as b
            on a.account_id = b.zendesk_account_id
        cross join max_date_finance as c
),

--- Growth metrics

growth_metrics as (
    select
        *,
        -- Renewal Rate
        case
            when subscription_renewal_date <= last_date_finance then 1 else 0
        end as renewal_passed,
        case
            when
                instance_net_arr_usd_bcv > 0 and renewal_passed = 1 then 1 else 0
        end as subscription_renewed,
        -- ARR changes
        instance_net_arr_usd_bcv - net_arr_usd_instance as arr_change,
        -- Same ARR
        case when instance_net_arr_usd_bcv = net_arr_usd_instance and net_arr_usd_instance > 0 then 1 else 0 end as no_change,
        case when instance_net_arr_usd_bcv = net_arr_usd_instance and net_arr_usd_instance > 0 then net_arr_usd_instance else 0 end as no_change_arr,
        -- Churn ARR
        case when (instance_net_arr_usd_bcv = 0 or instance_net_arr_usd_bcv is null) and net_arr_usd_instance > 0 then 1 else 0 end as churn,
        case when (instance_net_arr_usd_bcv = 0 or instance_net_arr_usd_bcv is null) and net_arr_usd_instance > 0 then net_arr_usd_instance else 0 end as churn_arr,
        -- Expansion ARR
        case when arr_change > 0 and net_arr_usd_instance > 0 then 1 else 0 end as expansion,
        case when arr_change > 0 and net_arr_usd_instance > 0 then arr_change else 0 end as expansion_arr,
        -- Contraction ARR
        case when arr_change < 0 and net_arr_usd_instance > 0 then 1 else 0 end as contraction,
        case when arr_change < 0 and net_arr_usd_instance > 0 then -1 * arr_change else 0 end as contraction_arr

    from unique_obs_arr_bcv
)

select *, current_date() as updated_at
from growth_metrics;

