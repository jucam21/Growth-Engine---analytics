--- Master cart funnel aggregation and win attribution
--- Combines all funnel steps and determines win attribution
--- Based on Step 1, Step 2, Step 3.x outputs

with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.crm_account_id,
        --- Overall Win data
        trial_accounts.win_date,
        case when trial_accounts.win_date is not null then 1 else null end as is_won,
        case when trial_accounts.win_date is not null then instance_account_arr_usd_at_win else null end as is_won_arr,
        --- SS wins
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won_ss,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_ss_arr,
        --- Trials extra info
        trial_accounts.region,
        trial_accounts.help_desk_size_grouped,
        trial_accounts.instance_account_created_date,
        trial_accounts.seats_capacity_band_at_win,
    from presentation.growth_analytics.trial_accounts trial_accounts 
),

master_cart_funnel_wins as (
    select 
        accounts.*,
        funnel_data.*,
        --- Determine funnel type based on which funnel has data
        case 
            when buy_your_trial_payment_submit.account_id is not null then 'buy_your_trial'
            when compare_plans_and_auto_trigger.account_id is not null then 'compare_plans'
            when see_all_plans_payment_page.account_id is not null then 'see_all_plans'
            else 'other'
        end as funnel_type,
        --- Create unified timestamp for ordering
        coalesce(
            buy_your_trial_payment_submit.original_timestamp,
            compare_plans_and_auto_trigger.original_timestamp,
            see_all_plans_payment_page.original_timestamp
        ) as buy_trial_or_modal_load_timestamp
    from accounts
    full outer join buy_your_trial_payment_submit 
        on accounts.instance_account_id = buy_your_trial_payment_submit.account_id
    full outer join compare_plans_and_auto_trigger 
        on accounts.instance_account_id = compare_plans_and_auto_trigger.account_id
    full outer join see_all_plans_payment_page 
        on accounts.instance_account_id = see_all_plans_payment_page.account_id
    where coalesce(
        buy_your_trial_payment_submit.account_id,
        compare_plans_and_auto_trigger.account_id,
        see_all_plans_payment_page.account_id,
        accounts.instance_account_id
    ) is not null
),

win_attribution as (
    select 
        account_id,
        buy_trial_or_modal_load_timestamp
    from master_cart_funnel_wins
    where 
        is_won_ss = 1
        and (date_trunc('day', buy_trial_pps_timestamp) <= win_date or
        date_trunc('day', modal_buy_now_pps_timestamp) <= win_date or
        date_trunc('day', modal_see_all_plans_pps_timestamp) <= win_date)
    qualify row_number() over (partition by account_id order by buy_trial_or_modal_load_timestamp desc) = 1
),

master_cart_funnel_wins_attribution as (
    select 
        master_cart.*,
        --- Attribution field
        case 
            when win_attr.account_id is not null and master_cart.funnel_type = 'buy_your_trial' 
            then 'buy_your_trial_win'
            when 
                win_attr.account_id is not null and master_cart.funnel_type != 'buy_your_trial' 
                and coalesce(master_cart.modal_buy_now_pps_timestamp, '1900-01-01 00:00:00') > coalesce(master_cart.modal_see_all_plans_pps_timestamp, '1900-01-01 00:00:00')
            then 'modal_buy_now_win'
            when 
                win_attr.account_id is not null and master_cart.funnel_type != 'buy_your_trial' 
                and coalesce(master_cart.modal_buy_now_pps_timestamp, '1900-01-01 00:00:00') < coalesce(master_cart.modal_see_all_plans_pps_timestamp, '1900-01-01 00:00:00')
            then 'modal_see_all_plans_win'
            when master_cart.win_date is not null and win_attr_submit.account_id is null 
            then 'won_outside_agent_home'
            when win_attr.account_id is not null 
            then 'error - other_win'
            when master_cart.win_date is null 
            then 'not_won'
            else 'not_last_submit'
        end as win_attribution_flag
    from master_cart_funnel_wins master_cart
    left join win_attribution win_attr
        on master_cart.account_id = win_attr.account_id
        and master_cart.buy_trial_or_modal_load_timestamp = win_attr.buy_trial_or_modal_load_timestamp
    left join (select distinct account_id from win_attribution) win_attr_submit
        on master_cart.account_id = win_attr_submit.account_id
),

master_cart_funnel_wins_attribution_flags as (
    select 
        *,
        --- Step 0: funnel ingress
        case when cta_click_cta = 'purchase' then account_id else null end as buy_your_trial_clicked_flag,
        case when cta_click_cta = 'compare' then account_id else null end as compare_clicked_flag,
        case when modal_auto_load_or_cta = 'auto_trigger' then account_id else null end as modal_auto_trigger_load_flag,
        --- Step 1: modal loads
        case when modal_auto_load_or_cta = 'auto_trigger' or modal_auto_load_or_cta = 'CTA' then account_id else null end as modal_loaded_flag,
        case when modal_auto_load_or_cta = 'CTA' then account_id else null end as modal_cta_loaded_flag,
        --- Step 2: modal interactions
        case when buy_now_timestamp is not null or see_all_plans_timestamp is not null then account_id else null end as modal_clicked_flag,
        case when buy_now_timestamp is not null then account_id else null end as modal_buy_now_clicked_flag,
        case when see_all_plans_timestamp is not null then account_id else null end as modal_see_all_plans_clicked_flag,
        --- Step 3: plan lineup loads
        case when plan_lineup_support_timestamp is not null or plan_lineup_suite_timestamp is not null then account_id else null end as plan_lineup_loaded_flag,
        case when plan_lineup_support_timestamp is not null then account_id else null end as plan_lineup_support_loaded_flag,
        case when plan_lineup_suite_timestamp is not null then account_id else null end as plan_lineup_suite_loaded_flag,
        --- Step 4: payment page visits
        case 
        when 
            buy_trial_ppv_timestamp is not null or 
            modal_buy_now_ppv_timestamp is not null or 
            modal_see_all_plans_ppv_timestamp is not null 
        then account_id else null end as payment_page_visited_flag,
        case when buy_trial_ppv_timestamp is not null then account_id else null end as buy_your_trial_payment_page_visited_flag,
        case when modal_buy_now_ppv_timestamp is not null then account_id else null end as modal_buy_now_payment_page_visited_flag,
        case when modal_see_all_plans_ppv_timestamp is not null then account_id else null end as modal_see_all_plans_payment_page_visited_flag,
        --- Payment visits suite or support
        case when ppv_support_timestamp is not null then account_id else null end as payment_page_visited_support_flag,
        case when ppv_suite_timestamp is not null then account_id else null end as payment_page_visited_suite_flag,
        --- Step 5: payment submissions
        case 
            when 
                buy_trial_pps_timestamp is not null or 
                modal_buy_now_pps_timestamp is not null or
                modal_see_all_plans_pps_timestamp is not null
            then account_id else null end as payment_submitted_flag,
        case when buy_trial_pps_timestamp is not null then account_id else null end as buy_your_trial_payment_submitted_flag,
        case when modal_buy_now_pps_timestamp is not null then account_id else null end as modal_buy_now_payment_submitted_flag,
        case when modal_see_all_plans_pps_timestamp is not null then account_id else null end as modal_see_all_plans_payment_submitted_flag,
        --- Payment submission suite or support
        case when pps_support_timestamp is not null then account_id else null end as payment_submitted_support_flag,
        case when pps_suite_timestamp is not null then account_id else null end as payment_submitted_suite_flag,
        --- Step 6: win attribution
        case when is_won = 1 then account_id else null end as won_flag,
        case when is_won_ss = 1 then account_id else null end as won_ss_flag,
        case when win_attribution_flag in ('buy_your_trial_win', 'modal_buy_now_win', 'modal_see_all_plans_win') then account_id else null end as won_via_agent_home_flag,
        case when win_attribution_flag = 'buy_your_trial_win' then account_id else null end as won_via_buy_your_trial_flag,
        case when win_attribution_flag = 'modal_buy_now_win' then account_id else null end as won_via_modal_buy_now_flag,
        case when win_attribution_flag = 'modal_see_all_plans_win' then account_id else null end as won_via_modal_see_all_plans_flag,
        case when win_attribution_flag = 'won_outside_agent_home' then account_id else null end as won_outside_agent_home_flag
    from master_cart_funnel_wins_attribution
)

select *, convert_timezone('UTC', 'America/Los_Angeles', current_timestamp) as updated_at
from master_cart_funnel_wins_attribution_flags
