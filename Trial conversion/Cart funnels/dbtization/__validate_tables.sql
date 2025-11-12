-------------------------------------------------------------------------
--- Compare outputs from initial query and dbtized query


--- Count of rows in initial query


/*
ROW_COUNT	TOTAL_ACCOUNTS
25394	11164
*/

select 
    count(*) as row_count,
    count(distinct account_id) as total_accounts
from _sandbox_juan_salgado.public.cart_funnel_session


/*
ROW_COUNT	TOTAL_ACCOUNTS
25394	11164
*/

select 
    count(*) as row_count,
    count(distinct account_id) as total_accounts
from _feature_e2bcartfunnels001_cloud.growth_analytics.trial_shopping_cart_session_funnel






--- Overall numbers initial query
select 
    funnel_type,
    --modal_auto_load_or_cta,
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    min(buy_trial_or_modal_load_timestamp) as events_since,
    --- Cart visits, CTA or auto trigger & interacted
    count(
        distinct 
            case 
                when (funnel_type = 'buy_your_trial' or modal_auto_load_or_cta = 'CTA') or 
                (modal_auto_load_or_cta = 'auto_trigger' and (buy_now_timestamp is not null or see_all_plans_timestamp is not null))
                then account_id else null 
                end
                ) as cart_visit,
    --- For modal loads, buy now or see all plans clicks
    count(distinct case when buy_now_timestamp is not null then account_id else null end) as to_buy_now,
    count(distinct case when see_all_plans_timestamp is not null then account_id else null end) as to_see_all_plans,
    --- For see all plan clicks, # of plan lineup loads (support or suite)
    count(distinct case when plan_lineup_support_timestamp is not null or plan_lineup_suite_timestamp is not null then account_id else null end) as to_plan_lineup,
    count(distinct case when plan_lineup_support_timestamp is not null then account_id else null end) as to_plan_lineup_support,
    count(distinct case when plan_lineup_suite_timestamp is not null then account_id else null end) as to_plan_lineup_suite,
    --- Payment page visits
    --count(
    --    distinct 
    --        case 
    --            when buy_trial_ppv_timestamp is not null or 
    --            modal_buy_now_ppv_timestamp is not null or 
    --            modal_see_all_plans_ppv_timestamp is not null then account_id else null end) as to_payment_page,
    count(distinct case when buy_trial_ppv_timestamp is not null then account_id else null end) as to_payment_page_buy_trial,
    count(distinct case when modal_buy_now_ppv_timestamp is not null then account_id else null end) as to_payment_page_buy_now,
    count(distinct case when modal_see_all_plans_ppv_timestamp is not null then account_id else null end) as to_payment_page_see_all_plans,
    --- Payment page visits see all plans, support or suite
    count(distinct case when ppv_support_timestamp is not null then account_id else null end) as to_payment_page_support,
    count(distinct case when ppv_suite_timestamp is not null then account_id else null end) as to_payment_page_suite,
    count(
        distinct 
            case 
                when buy_trial_pps_timestamp is not null or 
                modal_buy_now_pps_timestamp is not null or 
                modal_see_all_plans_pps_timestamp is not null then account_id else null end) as to_payment_submit,
    count(distinct case when buy_trial_pps_timestamp is not null then account_id else null end) as to_payment_submit_buy_trial,
    count(distinct case when modal_buy_now_pps_timestamp is not null then account_id else null end) as to_payment_submit_buy_now,
    count(distinct case when modal_see_all_plans_pps_timestamp is not null then account_id else null end) as to_payment_submit_see_all_plans,
    --- Payment submits see all plans, support or suite
    count(distinct case when pps_support_timestamp is not null then account_id else null end) as to_payment_submit_support,
    count(distinct case when pps_suite_timestamp is not null then account_id else null end) as to_payment_submit_suite
from _sandbox_juan_salgado.public.cart_funnel_session
--where funnel_type != 'buy_your_trial'
group by all
order by 1





--- Overall numbers new query
select 
    funnel_type,
    --modal_auto_load_or_cta,
    count(*) tot_obs,
    count(distinct account_id) as total_accounts,
    min(buy_trial_or_modal_load_timestamp) as events_since,
    --- Cart visits, CTA or auto trigger & interacted
    count(
        distinct 
            case 
                when (funnel_type = 'buy_your_trial' or modal_auto_load_or_cta = 'CTA' or funnel_type = 'admin_center') or 
                (modal_auto_load_or_cta = 'auto_trigger' and (buy_now_timestamp is not null or see_all_plans_timestamp is not null))
                then account_id else null 
                end
                ) as cart_visit,
    --- For modal loads, buy now or see all plans clicks
    count(distinct case when buy_now_timestamp is not null then account_id else null end) as to_buy_now,
    count(distinct case when see_all_plans_timestamp is not null then account_id else null end) as to_see_all_plans,
    --- For see all plan clicks, # of plan lineup loads (support or suite)
    count(distinct case when plan_lineup_support_timestamp is not null or plan_lineup_suite_timestamp is not null then account_id else null end) as to_plan_lineup,
    count(distinct case when plan_lineup_support_timestamp is not null then account_id else null end) as to_plan_lineup_support,
    count(distinct case when plan_lineup_suite_timestamp is not null then account_id else null end) as to_plan_lineup_suite,
    --- Payment page visits
    --count(
    --    distinct 
    --        case 
    --            when buy_trial_ppv_timestamp is not null or 
    --            modal_buy_now_ppv_timestamp is not null or 
    --            modal_see_all_plans_ppv_timestamp is not null then account_id else null end) as to_payment_page,
    count(distinct case when buy_trial_ppv_timestamp is not null then account_id else null end) as to_payment_page_buy_trial,
    count(distinct case when modal_buy_now_ppv_timestamp is not null then account_id else null end) as to_payment_page_buy_now,
    count(distinct case when modal_see_all_plans_ppv_timestamp is not null then account_id else null end) as to_payment_page_see_all_plans,
    --- Payment page visits see all plans, support or suite
    count(distinct case when ppv_support_timestamp is not null then account_id else null end) as to_payment_page_support,
    count(distinct case when ppv_suite_timestamp is not null then account_id else null end) as to_payment_page_suite,
    count(
        distinct 
            case 
                when buy_trial_pps_timestamp is not null or 
                modal_buy_now_pps_timestamp is not null or 
                modal_see_all_plans_pps_timestamp is not null or
                admin_center_pps_timestamp is not null then account_id else null end) as to_payment_submit,
    count(distinct case when buy_trial_pps_timestamp is not null then account_id else null end) as to_payment_submit_buy_trial,
    count(distinct case when modal_buy_now_pps_timestamp is not null then account_id else null end) as to_payment_submit_buy_now,
    count(distinct case when modal_see_all_plans_pps_timestamp is not null then account_id else null end) as to_payment_submit_see_all_plans,
    --- Payment submits see all plans, support or suite
    count(distinct case when pps_support_timestamp is not null then account_id else null end) as to_payment_submit_support,
    count(distinct case when pps_suite_timestamp is not null then account_id else null end) as to_payment_submit_suite,
    count(
        distinct 
            case 
                when buy_trial_ppv_timestamp is not null or 
                modal_buy_now_ppv_timestamp is not null or 
                modal_see_all_plans_ppv_timestamp is not null or
                admin_center_ppv_timestamp is not null then account_id else null end) as to_payment_visit
from _feature_e2bcartfunnels001_cloud.growth_analytics.trial_shopping_cart_session_funnel
--where funnel_type != 'buy_your_trial'
group by all
order by 1





