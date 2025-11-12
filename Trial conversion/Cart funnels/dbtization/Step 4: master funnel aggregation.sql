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

master_cart_funnel as (
    -- Buy your trial funnel
    select 
        account_id,
        'buy_your_trial' as funnel_type,
        original_timestamp as cta_click_timestamp,
        original_timestamp_pt as cta_click_timestamp_pt,
        'trial' as cta_click_trial_type,
        'purchase' as cta_click_cta,
        original_timestamp as buy_trial_or_modal_load_timestamp,
        original_timestamp_pt as buy_trial_or_modal_load_timestamp_pt,
        null as offer_id,
        null as plan_name,
        null as preview_state,
        null as modal_auto_load_or_cta,
        null as buy_now_timestamp,
        null as see_all_plans_timestamp,
        null as plan_lineup_support_timestamp,
        null as ppv_support_timestamp,
        null as pps_support_timestamp,
        null as plan_lineup_suite_timestamp,
        null as ppv_suite_timestamp,
        null as pps_suite_timestamp,
        -- PPV/PPS for buy your trial
        ppv_timestamp as buy_trial_ppv_timestamp,
        pps_timestamp as buy_trial_pps_timestamp,
        -- Payment visits or submit, separated for buy now/see all plans
        null as modal_buy_now_ppv_timestamp,
        null as modal_buy_now_pps_timestamp,
        null as modal_see_all_plans_ppv_timestamp,
        null as modal_see_all_plans_pps_timestamp,
        -- Admin center specific columns
        null as admin_center_ppv_timestamp,
        null as admin_center_pps_timestamp
    from buy_your_trial_payment_submit

    union all

    -- Compare plans modal funnel (combining buy now and see all plans)
    select 
        coalesce(compare_plans.account_id, compare_plans.cta_click_account_id) as account_id,
        'modal_loads' as funnel_type,
        compare_plans.cta_click_timestamp,
        compare_plans.cta_click_timestamp_pt,
        compare_plans.cta_click_trial_type,
        compare_plans.cta_click_cta,
        coalesce(compare_plans.original_timestamp, compare_plans.cta_click_timestamp) as buy_trial_or_modal_load_timestamp,
        coalesce(compare_plans.original_timestamp_pt, compare_plans.cta_click_timestamp_pt) as buy_trial_or_modal_load_timestamp_pt,
        compare_plans.offer_id,
        compare_plans.plan_name,
        compare_plans.preview_state,
        compare_plans.source as modal_auto_load_or_cta,
        modal_buy_now.buy_now_timestamp,
        modal_see_all_plans.see_all_plans_timestamp,
        modal_see_all_plans.plan_lineup_support_timestamp,
        modal_see_all_plans.ppv_support_timestamp,
        modal_see_all_plans.pps_support_timestamp,
        modal_see_all_plans.plan_lineup_suite_timestamp,
        modal_see_all_plans.ppv_suite_timestamp,
        modal_see_all_plans.pps_suite_timestamp,
        -- PPV/PPS for buy your trial
        null as buy_trial_ppv_timestamp,
        null as buy_trial_pps_timestamp,
        -- Payment visits or submit, separated for buy now/see all plans
        modal_buy_now.ppv_timestamp as modal_buy_now_ppv_timestamp,
        modal_buy_now.pps_timestamp as modal_buy_now_pps_timestamp,
        modal_see_all_plans.ppv_timestamp as modal_see_all_plans_ppv_timestamp,
        modal_see_all_plans.pps_timestamp as modal_see_all_plans_pps_timestamp,
        -- Admin center specific columns
        null as admin_center_ppv_timestamp,
        null as admin_center_pps_timestamp
    from compare_plans_funnel compare_plans
    left join modal_load_buy_now_payment_submit modal_buy_now
        on compare_plans.account_id = modal_buy_now.account_id
        and compare_plans.original_timestamp = modal_buy_now.original_timestamp
    left join modal_see_all_plans_payment_submit_joined modal_see_all_plans
        on compare_plans.account_id = modal_see_all_plans.account_id
        and compare_plans.original_timestamp = modal_see_all_plans.original_timestamp

    union all

    -- Admin center funnel
    select 
        account_id,
        'admin_center' as funnel_type,
        original_timestamp as cta_click_timestamp,
        original_timestamp_pt as cta_click_timestamp_pt,
        trial_type as cta_click_trial_type,
        cta as cta_click_cta,
        original_timestamp as buy_trial_or_modal_load_timestamp,
        original_timestamp_pt as buy_trial_or_modal_load_timestamp_pt,
        null as offer_id,
        plan_name,
        null as preview_state,
        null as modal_auto_load_or_cta,
        null as buy_now_timestamp,
        null as see_all_plans_timestamp,
        null as plan_lineup_support_timestamp,
        null as ppv_support_timestamp,
        null as pps_support_timestamp,
        null as plan_lineup_suite_timestamp,
        null as ppv_suite_timestamp,
        null as pps_suite_timestamp,
        -- PPV/PPS for buy your trial
        null as buy_trial_ppv_timestamp,
        null as buy_trial_pps_timestamp,
        -- Payment visits or submit, separated for buy now/see all plans
        null as modal_buy_now_ppv_timestamp,
        null as modal_buy_now_pps_timestamp,
        null as modal_see_all_plans_ppv_timestamp,
        null as modal_see_all_plans_pps_timestamp,
        -- Admin center specific columns
        ppv_timestamp as admin_center_ppv_timestamp,
        pps_timestamp as admin_center_pps_timestamp
    from admin_center_payment_submit
),

master_cart_funnel_wins_joined as (
    select 
        master_cart.*,
        --- Relevant fields
        accounts_.instance_account_created_date,
        accounts_.crm_account_id,
        accounts_.win_date,
        accounts_.is_won,
        accounts_.is_won_arr,
        accounts_.is_won_ss,
        accounts_.is_won_ss_arr,
        accounts_.region,
        accounts_.help_desk_size_grouped,
        accounts_.seats_capacity_band_at_win,
    from master_cart_funnel master_cart
    left join accounts
        on master_cart.account_id = accounts.instance_account_id
),

win_attribution as (
    select 
        account_id,
        buy_trial_or_modal_load_timestamp
    from master_cart_funnel_wins_joined
    where 
        is_won_ss = 1
        and (date_trunc('day', buy_trial_pps_timestamp) <= win_date or
        date_trunc('day', modal_buy_now_pps_timestamp) <= win_date or
        date_trunc('day', modal_see_all_plans_pps_timestamp) <= win_date or
        date_trunc('day', admin_center_pps_timestamp) <= win_date)
    qualify row_number() over (partition by account_id order by buy_trial_or_modal_load_timestamp desc) = 1
),

master_cart_funnel_wins_attribution as (
    select 
        joined.*,
        --- Attribution field
        case 
            when win_attr.account_id is not null and joined.funnel_type = 'buy_your_trial' 
            then 'buy_your_trial_win'
            when win_attr.account_id is not null and joined.funnel_type = 'admin_center'
            then 'admin_center_win'
            when 
                win_attr.account_id is not null and joined.funnel_type != 'buy_your_trial' and joined.funnel_type != 'admin_center'
                and coalesce(joined.modal_buy_now_pps_timestamp, '1900-01-01 00:00:00') > coalesce(joined.modal_see_all_plans_pps_timestamp, '1900-01-01 00:00:00')
            then 'modal_buy_now_win'
            when 
                win_attr.account_id is not null and joined.funnel_type != 'buy_your_trial' and joined.funnel_type != 'admin_center'
                and coalesce(joined.modal_buy_now_pps_timestamp, '1900-01-01 00:00:00') < coalesce(joined.modal_see_all_plans_pps_timestamp, '1900-01-01 00:00:00')
            then 'modal_see_all_plans_win'
            when joined.win_date is not null and win_attr_submit.account_id is null 
            then 'won_outside_categorized_cases'
            when win_attr.account_id is not null 
            then 'error - other_win'
            when joined.win_date is null 
            then 'not_won'
            else 'not_last_submit'
        end as win_attribution_flag
    from master_cart_funnel_wins_joined joined
    left join win_attribution win_attr
        on joined.account_id = win_attr.account_id
        and joined.buy_trial_or_modal_load_timestamp = win_attr.buy_trial_or_modal_load_timestamp
    left join (select distinct account_id from win_attribution) win_attr_submit
        on joined.account_id = win_attr_submit.account_id
),

master_cart_funnel_wins_attribution_flags as (
    select 
        *,
        --- Step 0: funnel ingress
        case when cta_click_cta = 'purchase' then account_id else null end as buy_your_trial_clicked_flag,
        case when cta_click_cta = 'compare' then account_id else null end as compare_clicked_flag,
        case when cta_click_cta = 'admin_center' then account_id else null end as admin_center_clicked_flag,
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
            modal_see_all_plans_ppv_timestamp is not null or
            admin_center_ppv_timestamp is not null
        then account_id else null end as payment_page_visited_flag,
        case when buy_trial_ppv_timestamp is not null then account_id else null end as buy_your_trial_payment_page_visited_flag,
        case when modal_buy_now_ppv_timestamp is not null then account_id else null end as modal_buy_now_payment_page_visited_flag,
        case when modal_see_all_plans_ppv_timestamp is not null then account_id else null end as modal_see_all_plans_payment_page_visited_flag,
        case when admin_center_ppv_timestamp is not null then account_id else null end as admin_center_payment_page_visited_flag,
        --- Payment visits suite or support
        case when ppv_support_timestamp is not null then account_id else null end as payment_page_visited_support_flag,
        case when ppv_suite_timestamp is not null then account_id else null end as payment_page_visited_suite_flag,
        --- Step 5: payment submissions
        case 
            when 
                buy_trial_pps_timestamp is not null or 
                modal_buy_now_pps_timestamp is not null or
                modal_see_all_plans_pps_timestamp is not null or
                admin_center_pps_timestamp is not null
            then account_id else null end as payment_submitted_flag,
        case when buy_trial_pps_timestamp is not null then account_id else null end as buy_your_trial_payment_submitted_flag,
        case when modal_buy_now_pps_timestamp is not null then account_id else null end as modal_buy_now_payment_submitted_flag,
        case when modal_see_all_plans_pps_timestamp is not null then account_id else null end as modal_see_all_plans_payment_submitted_flag,
        case when admin_center_pps_timestamp is not null then account_id else null end as admin_center_payment_submitted_flag,
        --- Payment submission suite or support
        case when pps_support_timestamp is not null then account_id else null end as payment_submitted_support_flag,
        case when pps_suite_timestamp is not null then account_id else null end as payment_submitted_suite_flag,
        --- Step 6: win attribution
        case when is_won = 1 then account_id else null end as won_flag,
        case when is_won_ss = 1 then account_id else null end as won_ss_flag,
        case when win_attribution_flag in ('buy_your_trial_win', 'modal_buy_now_win', 'modal_see_all_plans_win', 'admin_center_win') then account_id else null end as won_via_agent_home_flag,
        case when win_attribution_flag = 'buy_your_trial_win' then account_id else null end as won_via_buy_your_trial_flag,
        case when win_attribution_flag = 'modal_buy_now_win' then account_id else null end as won_via_modal_buy_now_flag,
        case when win_attribution_flag = 'modal_see_all_plans_win' then account_id else null end as won_via_modal_see_all_plans_flag,
        case when win_attribution_flag = 'admin_center_win' then account_id else null end as won_via_admin_center_flag,
        case when win_attribution_flag = 'won_outside_agent_home' then account_id else null end as won_outside_agent_home_flag
    from master_cart_funnel_wins_attribution
)

select *, convert_timezone('UTC', 'America/Los_Angeles', current_timestamp) as updated_at
from master_cart_funnel_wins_attribution_flags
