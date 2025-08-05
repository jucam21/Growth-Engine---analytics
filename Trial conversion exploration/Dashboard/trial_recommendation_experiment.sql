--- Check previous cart experiment events


-------------------------------------
---- *** UNRELATED: cancel reason ***

select 
    cancel_reason,
    count(*) as total_obs,
    --min(original_timestamp) as first_event_time,
    --max(original_timestamp) as last_event_time
from CLEANSED.ZUORA.ZUORA_SUBSCRIPTIONS_BCV
group by 1
order by 2 desc


select 
    cancel_reason,
    count(*) as total_obs,
    --min(original_timestamp) as first_event_time,
    --max(original_timestamp) as last_event_time
from CLEANSED.ZUORA.ZUORA_SUBSCRIPTIONS_scd2
group by 1
order by 2 desc


select 
    cancel_reason,
    count(*) as total_obs,
    --min(original_timestamp) as first_event_time,
    --max(original_timestamp) as last_event_time
from FOUNDATIONAL.FINANCE_INTERNAL.INT_HARD_DELETED_SUBSCRIPTIONS
group by 1
order by 2 desc


--- SAAS 
select 
    cancel_reason,
    count(*) as total_obs,
    --min(original_timestamp) as first_event_time,
    --max(original_timestamp) as last_event_time
from raw.zuora.subscription
group by 1
order by 2 desc









--- Checking existing events for the previous cart experiment

, events as(

SELECT DISTINCT
INSTANCE_ACCOUNT_ID as account_id
-- 1. Modal Ingress
, count(distinct case when STANDARD_EXPERIMENT_ACCOUNT_EVENT_NAME = 'mss_modal_ingress_viewed' then account_id else null end) 
  as mss_modal_ingress_viewed
, count(distinct case when STANDARD_EXPERIMENT_ACCOUNT_EVENT_NAME = 'mss_modal_ingress_set_up_clicked' then account_id else null end) 
  as mss_modal_ingress_set_up_clicked
, count(distinct case when STANDARD_EXPERIMENT_ACCOUNT_EVENT_NAME = 'mss_modal_ingress_dismiss_clicked' then account_id else null end) 
  as mss_modal_ingress_dismiss_clicked

FROM PROPAGATED_CLEANSED.PDA.BASE_STANDARD_EXPERIMENT_ACCOUNT_EVENTS

where 
STANDARD_EXPERIMENT_NAME = 'messaging_wizard_agent_progress_tracker'
and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-06-02' //FIRSTLAUNCH

group by 1

)



select distinct
    standard_experiment_account_event_name,
    count(*) as total_accounts,
    min(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as first_event_time,
    max(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as last_event_time
from propagated_cleansed.pda.base_standard_experiment_account_events
where 

-- # 0) Entrance via a deep link / trial welcome / Lean more / Admin Center
standard_experiment_account_event_name like 'view_%compare_plans%' or
standard_experiment_account_event_name like 'click_compare_plans' or
standard_experiment_account_event_name like 'click_buy_your_trial' or
standard_experiment_account_event_name like 'click_buy_zendesk' or
standard_experiment_account_event_name like 'enter_from_deep_link' or
standard_experiment_account_event_name like 'enter_from_trial_welcome_screen' or
standard_experiment_account_event_name like 'click_learn_more_see_all_plans' or
standard_experiment_account_event_name like 'click_learn_more_get_plan_recommendation' or
standard_experiment_account_event_name like 'click_purchase_admin_center' or

-- Treatment
-- # 1) View Compare Plans or Buy Your Trial
standard_experiment_account_event_name like 'view_%compare_plans%' or
standard_experiment_account_event_name like 'view_suite_plan_compare_plans' or
standard_experiment_account_event_name like 'view_support_plan_compare_plans' or
standard_experiment_account_event_name like '%buy_your_trial%' or
standard_experiment_account_event_name like 'click_see_all_plans_buy_your_trial_payment_page' or
standard_experiment_account_event_name like 'view_support_plan' or
standard_experiment_account_event_name like 'view_suite_plan' or
standard_experiment_account_event_name like 'view_support_plan_compare_plans' or
standard_experiment_account_event_name like 'view_suite_plan_compare_plans' or
standard_experiment_account_event_name like 'view_presets%' or
standard_experiment_account_event_name like 'click_preset%' or
standard_experiment_account_event_name like 'click_preset_all_plans' or
standard_experiment_account_event_name like 'click_preset_quiz' or
standard_experiment_account_event_name like 'click_preset_trial_plan' or
standard_experiment_account_event_name like 'click_preset_suite' or
standard_experiment_account_event_name like 'click_preset_support' or
-- 2.1) Additional steps in Treatment: all plans preset
standard_experiment_account_event_name like 'view_suite_plan' or
standard_experiment_account_event_name like 'view_support_plan' or
standard_experiment_account_event_name like 'view_support_customize' or
-- 3) View Payment
standard_experiment_account_event_name like 'view_%payment%' or
standard_experiment_account_event_name = 'view_support_customize' or
standard_experiment_account_event_name like 'view_payment_buy_your_trial' or
standard_experiment_account_event_name like 'view_payment_compare_plans' or
standard_experiment_account_event_name like 'view_payment_quiz%' or
standard_experiment_account_event_name like 'view_payment_trial_plan%' or
standard_experiment_account_event_name like 'view_payment_all_plans%' or
-- Control
standard_experiment_account_event_name like 'view_support_customize' or
-- 4) Complete purchase
standard_experiment_account_event_name like 'complete_purchase%' or
standard_experiment_account_event_name like 'complete_purchase_buy_your_trial' or
standard_experiment_account_event_name like 'complete_purchase_compare_plans' or
standard_experiment_account_event_name like 'complete_purchase_quiz' or
standard_experiment_account_event_name like 'complete_purchase_trial_plan' or
standard_experiment_account_event_name like 'complete_purchase_all_plans' or
-- 5) Payment Successful
standard_experiment_account_event_name like 'payment_successful%' or
standard_experiment_account_event_name like 'payment_successful_buy_your_trial' or
standard_experiment_account_event_name like 'payment_successful_compare_plans' or
standard_experiment_account_event_name like 'payment_successful_quiz' or
standard_experiment_account_event_name like 'payment_successful_trial_plan' or
standard_experiment_account_event_name like 'payment_successful_all_plans'

group by 1
order by 2 desc





--- Previous events



# First events for critical funnel steps
, first_events as (
select account_id
, first_cart_entrance
, max(if(nth_event = 1, event_name, null)) first_event
, min(if(nth_event = 1, created_at, null)) first_event_at

## Flag accounts whos first cart entrance was through a deep link
-- , max(is_first_entrance_from_deep_link) is_first_entrance_from_deep_link
-- , max(is_first_entrance_from_trial_welcome) is_first_entrance_from_trial_welcome
-- , max(greatest(is_first_entrance_from_deep_link, is_first_entrance_from_trial_welcome)) is_first_entrance_from_deep_link_or_trial_welcome
  
# 0) Entrance via a deep link / trial welcome / Lean more / Admin Center
, min(if(event_name like 'click_compare_plans', created_at, null)) first_click_compare_plans # Treatment only
, min(if(event_name like 'click_buy_your_trial', created_at, null)) first_click_buy_your_trial # Treatment only
, min(if(event_name like 'click_buy_zendesk', created_at, null)) first_click_buy_zendesk # Control Only
, min(if(event_name like 'enter_from_deep_link', created_at, null)) first_entered_from_deep_link 
, min(if(event_name like 'enter_from_trial_welcome_screen', created_at, null)) first_entered_from_trial_welcome   
, min(if(event_name like 'click_learn_more_see_all_plans', created_at, null)) first_click_learn_more_see_all_plans 
, min(if(event_name like 'click_learn_more_get_plan_recommendation', created_at, null)) first_click_learn_more_get_plan_recommendation
, min(if(event_name like 'click_purchase_admin_center', created_at, null)) first_click_purchase_admin_center

# Treatment
# 1) View Compare Plans or Buy Your Trial
, min(if(event_name like 'view_%compare_plans%', created_at, null)) first_viewed_compare_plans
, min(if(event_name = 'view_suite_plan_compare_plans', created_at, null)) first_viewed_compare_plans_suite
, min(if(event_name = 'view_support_plan_compare_plans', created_at, null)) first_viewed_compare_plans_support
, min(if(event_name like '%buy_your_trial%', created_at, null)) first_buy_your_trial
, min(if(event_name = 'click_see_all_plans_buy_your_trial_payment_page', created_at, null)) first_buy_your_trial_see_all_plans
# Treatment and Control - Plans
, min(if(event_name IN ('view_support_plan','view_suite_plan','view_support_plan_compare_plans','view_suite_plan_compare_plans'), created_at, null)) first_viewed_plans 

# 2) View and Click A Preset - Treatment and Control
, min(if(event_name like 'view_presets%', created_at, null)) first_viewed_preset # 'view_presets' 'view_presets_v3'
, min(if(event_name like 'click_preset%', created_at, null)) first_clicked_preset # ANY preset
, min(if(event_name like 'click_preset_all_plans', created_at, null)) first_clicked_preset_all_plans  
, min(if(event_name like 'click_preset_quiz', created_at, null)) first_clicked_preset_quiz 
, min(if(event_name like 'click_preset_trial_plan', created_at, null)) first_clicked_preset_trial_plan 
, min(if(event_name like 'click_preset_suite', created_at, null)) first_clicked_preset_suite  
, min(if(event_name like 'click_preset_support', created_at, null)) first_clicked_preset_support  

# 2.1) Additional steps in  Treatment: all plans preset
### V1: All plan views, regardless of whether from All Plans preset or Quiz see all plans click
, min(if(event_name like 'view_suite_plan', created_at, null)) first_viewed_suite_plan  
, min(if(event_name like 'view_support_plan', created_at, null)) first_viewed_support_plan  
/*
### V2: Retrict to plan views directly from All Plans preset
, min(if(is_view_suite_plan_not_on_quiz = 1, created_at, null)) first_viewed_suite_plan_not_on_quiz
, min(if(is_view_support_plan_not_on_quiz = 1, created_at, null)) first_viewed_support_plan_not_on_quiz
, min(if(event_name like 'view_support_customize', created_at, null)) first_viewed_support_customize  # This is shared by both control and treatment
*/

, min(if(event_name like 'view_support_customize', created_at, null)) first_viewed_support_customize  # This is shared by both control and treatment


# 3) View Payment
, min(if(event_name like 'view_%payment%' OR event_name='view_support_customize', created_at, null)) first_viewed_payment  
# Treatment
, min(if(event_name like 'view_payment_buy_your_trial', created_at, null)) first_viewed_payment_buy_your_trial #treatment only 
, min(if(event_name like 'view_payment_compare_plans', created_at, null)) first_viewed_payment_compare_plans #treatment only

## Presets 
, min(if(event_name like 'view_payment_quiz%', created_at, null)) first_viewed_payment_quiz 
, min(if(event_name like 'view_payment_trial_plan%', created_at, null)) first_viewed_payment_trial_plan 
, min(if(event_name like 'view_payment_all_plans%', created_at, null)) first_viewed_payment_all_plans  
# Control
, min(if(event_name like 'view_support_customize', created_at, null)) first_viewed_payment_support 
# 4) Complete purchase
, min(if(event_name like 'complete_purchase%', created_at, null)) first_completed_purchase  
, min(if(event_name like 'complete_purchase_buy_your_trial', created_at, null)) first_completed_purchase_buy_your_trial #treatment only
, min(if(event_name like 'complete_purchase_compare_plans', created_at, null)) first_completed_purchase_compare_plans #treatment only
, min(if(event_name like 'complete_purchase_quiz', created_at, null)) first_completed_purchase_quiz ## <-- To check!!
, min(if(event_name like 'complete_purchase_trial_plan', created_at, null)) first_completed_purchase_trial_plan 
, min(if(event_name like 'complete_purchase_all_plans', created_at, null)) first_completed_purchase_all_plans  
# 5) Payment Successful
, min(if(event_name like 'payment_successful%', created_at, null)) first_payment_successful
, min(if(event_name like 'payment_successful_buy_your_trial', created_at, null)) first_payment_successful_buy_your_trial #treatment only
, min(if(event_name like 'payment_successful_compare_plans', created_at, null)) first_payment_successful_compare_plans #treatment only
, min(if(event_name like 'payment_successful_quiz', created_at, null)) first_payment_successful_quiz 
, min(if(event_name like 'payment_successful_trial_plan', created_at, null)) first_payment_successful_trial_plan
, min(if(event_name like 'payment_successful_all_plans', created_at, null)) first_payment_successful_all_plans 
# Latest cart event at
, max(created_at) last_cart_event

from events e
group by 1,2
)


