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
        sku_type,
        seats_capacity,
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
        sku_type,
        seats_capacity,
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
        sku_type,
        seats_capacity,
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

--- Daily ARR for retention curves

daily_arr as (
    select
        finance.service_date,
        snapshot.instance_account_id as zendesk_account_id,
        sum(finance.net_arr_usd) as net_arr_usd
    from foundational.finance.fact_recurring_revenue_daily_snapshot_enriched as finance
    left join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on finance.billing_account_id = snapshot.billing_account_id
        and finance.service_date = snapshot.source_snapshot_date
    where finance.service_date >= '2025-03-01'
    group by all
),

--- Creating future dates, based on initial renewal date, for joining ARR data

unique_obs_future_dates as (
    select
        *,
        -- Adding X months or years to the renewal date based on the subscription term type
        case
            when subscription_term_type = 'monthly' then dateadd(month, 1, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 1, subscription_renewal_date)
            else null
        end as renewal_p1_date,
        case
            when subscription_term_type = 'monthly' then dateadd(month, 2, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 2, subscription_renewal_date)
            else null
        end as renewal_p2_date,
        case
            when subscription_term_type = 'monthly' then dateadd(month, 3, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 3, subscription_renewal_date)
            else null
        end as renewal_p3_date,
        case
            when subscription_term_type = 'monthly' then dateadd(month, 4, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 4, subscription_renewal_date)
            else null
        end as renewal_p4_date,
        case
            when subscription_term_type = 'monthly' then dateadd(month, 5, subscription_renewal_date)
            when subscription_term_type = 'annual' then dateadd(year, 5, subscription_renewal_date)
            else null
        end as renewal_p5_date,
        -- Determining if the renewal date have passed
        -- Customers available to renew on each future date
        case when renewal_p1_date <= last_date_finance then 1 else 0 end as renewal_p1_date_passed,
        case when renewal_p2_date <= last_date_finance then 1 else 0 end as renewal_p2_date_passed,
        case when renewal_p3_date <= last_date_finance then 1 else 0 end as renewal_p3_date_passed,
        case when renewal_p4_date <= last_date_finance then 1 else 0 end as renewal_p4_date_passed,
        case when renewal_p5_date <= last_date_finance then 1 else 0 end as renewal_p5_date_passed
    from unique_obs_arr_bcv
),

--- Joining future renewal dates with daily ARR data

unique_retention_curves as (
    select
        a.*,
        -- Creating flags for dashboard
        -- ARR that was renewed on each future date
        renewal_1.net_arr_usd as renewed_p1_arr,
        renewal_2.net_arr_usd as renewed_p2_arr,
        renewal_3.net_arr_usd as renewed_p3_arr,
        renewal_4.net_arr_usd as renewed_p4_arr,
        renewal_5.net_arr_usd as renewed_p5_arr,
        --- ARR available to renew
        case when renewal_p1_date_passed = 1 then net_arr_usd_instance else null end as renewal_p1_arr_passed,
        case when renewal_p2_date_passed = 1 then net_arr_usd_instance else null end as renewal_p2_arr_passed,
        case when renewal_p3_date_passed = 1 then net_arr_usd_instance else null end as renewal_p3_arr_passed,
        case when renewal_p4_date_passed = 1 then net_arr_usd_instance else null end as renewal_p4_arr_passed,
        case when renewal_p5_date_passed = 1 then net_arr_usd_instance else null end as renewal_p5_arr_passed,
        -- Flag if customer renewed on each future date
        case when renewed_p1_arr > 0 then 1 else 0 end as renewed_p1_account,
        case when renewed_p2_arr > 0 then 1 else 0 end as renewed_p2_account,
        case when renewed_p3_arr > 0 then 1 else 0 end as renewed_p3_account,
        case when renewed_p4_arr > 0 then 1 else 0 end as renewed_p4_account,
        case when renewed_p5_arr > 0 then 1 else 0 end as renewed_p5_account
    from unique_obs_future_dates as a
        left join daily_arr as renewal_1
            on a.account_id = renewal_1.zendesk_account_id
            and a.renewal_p1_date = renewal_1.service_date
        left join daily_arr as renewal_2
            on a.account_id = renewal_2.zendesk_account_id
            and a.renewal_p2_date = renewal_2.service_date
        left join daily_arr as renewal_3
            on a.account_id = renewal_3.zendesk_account_id
            and a.renewal_p3_date = renewal_3.service_date
        left join daily_arr as renewal_4
            on a.account_id = renewal_4.zendesk_account_id
            and a.renewal_p4_date = renewal_4.service_date
        left join daily_arr as renewal_5
            on a.account_id = renewal_5.zendesk_account_id
            and a.renewal_p5_date = renewal_5.service_date
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

    from unique_retention_curves
)

select *, current_date() as updated_at
from growth_metrics;

