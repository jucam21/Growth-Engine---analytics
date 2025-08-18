--- SQL query used in previous cart experiment
--- https://docs.google.com/spreadsheets/d/1gsnxC4c82PRpYo8k22f_hg_zc8koNOl48HYjk1E9qGI/edit?gid=1690138981#gid=1690138981


--Step 1
--List of accounts enrolled in Accelerated Cart experiment
with expt as (
select distinct experiment_name
, case when variation = 'treatment' then 'V1: Variant' 
       when variation = 'control' then 'V0: Control' 
       else NULL end as variation
, account_id
, DATETIME(created_at, "America/Los_Angeles") as expt_created_at_pt
from pdw.experimentation_data
where experiment_name = 'accelerated_cart' 
  and variation in ('treatment', 'control')
  and DATETIME(created_at, "America/Los_Angeles") BETWEEN '2023-10-12' AND '2023-11-16' #FIRSTLAUNCH
),

--Step 2
--Check for duplicate assignments (account_id in more than 1 variant)
 dups as (
select account_id
, count(distinct variation) num_variations
, string_agg(distinct variation order by variation) variations
from expt
group by 1
)

--Step 3
--Organize Account and Experiment Data via Account_id
, expt2 as (
select e.*
, date(min(r.created_at)) account_created_at
, max(r.subdomain) subdomain
, max(r.derived_account_type) derived_account_type
, max(af.trial_type) trial_type
, max(af.is_direct_buy) is_direct_buy
, max(r.is_abusive) is_abusive
, max(r.region) region
, max(r.country) country
, max(r.pod_id) pod_id
--- conversion metrics ---
, max(if(af.first_shopping_cart_visit_at is not null, 1, 0)) as visited_cart
, max(if(af.first_shopping_cart_visit_at is not null, af.first_shopping_cart_visit_at, null)) as first_shopping_cart_visit_at
, max(if(af.win_dt is not null, 1, 0)) as is_won
, MAX(if(DATE_TRUNC(af.win_dt,month) = DATE_TRUNC(af.created_at,month),1,0)) as is_month0_win
, max(if(af.win_dt is not null, af.win_dt, null)) as win_date
, max(if(af.win_dt is not null and af.win_dt < date_sub(current_date(), interval 30 day), 1, 0)) as won_30d_ago
, max(if(af.win_dt is not null, af.arr_at_win, 0)) as arr_at_win
, max(if(af.win_dt is not null, af.arr_mo1, null)) as arr_at_mo1
, max(if(af.win_dt is not null, arr_band_at_win, null)) as arr_band_at_win
, max(seats_at_win) as seats_at_win
, max(af.product_mix_at_win) as product_mix_at_win
, max(af.support_plan_at_win) as support_plan_at_win
, max(af.sales_model_at_win) as sales_model_at_win
, max(currentterm_at_win) as currentterm_at_win
, max(case when w.is_suite_at_win = 1 and af.win_dt is not null then "Suite"
           when ifnull(w.is_suite_at_win,0) = 0 and af.win_dt is not null then "Support"
           else null end) as support_or_suite_win
, max(if(w.max_billing_period_at_win >= 12, 'Annual', 'Monthly')) billing_period_at_win
, max(if(w.is_suite_at_win = 1, concat('Suite ', w.spp_suite_plan_at_win), concat('Support ', w.support_plan_at_win))) support_or_suite_plan_at_win
, string_agg(distinct b.gtm_team) as gtm_team			
, string_agg(distinct b.sales_motion) as sales_motion			
, string_agg(distinct b.initiated_by) as initiated_by			
, string_agg(distinct b.purchase_method) as purchase_method	
, string_agg(distinct af.employee_range_band) as employee_range_band		 
, string_agg(distinct af.crm_employee_size_subband) as crm_employee_size_subband		 
-- experiment variations --
, max(d.num_variations) num_variations
, max(d.variations) variations # list all variants each account is in
from expt e
left join dups d
  on e.account_id = d.account_id
left join pdw.derived_account_view r
  on e.account_id = r.account_id
left join product_activation_engagement.trial_metadata t
  on e.account_id = t.account_id  
left join `edw-prod-153420.product_usage_analyst_general.yp_instance_at_win` w
  on w.account_id = e.account_id
left join `edw-prod-153420.product_usage_analyst_general.af_growth_dashboard` af
  on af.account_id = e.account_id
left join `edw-prod-153420.financials.curated_bookings` b on r.sfdc_crm_id=b.crm_account_id
and type = 'New Business'
and close_date >= '2023-10-12'
WHERE DATETIME(r.created_at, "America/Los_Angeles")BETWEEN '2023-10-12' AND '2023-11-16' #FIRSTLAUNCH
group by 1,2,3,4)


, first_cart_entry as (
select account_id
, cart_entrance as first_cart_entrance
, row_number () over (partition by account_id order by created_at) as row_num
from (
  select ev.account_id
    , CASE WHEN ev.event_name = 'click_compare_plans' THEN 'Compare Plans'
       WHEN ev.event_name = 'click_buy_your_trial' THEN 'Buy Your Trial'
       WHEN ev.event_name = 'click_buy_zendesk' THEN 'Buy Zendesk' 
       WHEN ev.event_name LIKE 'click_learn_more%' THEN 'Learn More'
       WHEN ev.event_name = 'click_purchase_admin_center' THEN 'Admin Center'
       WHEN ev.event_name = 'enter_from_deep_link' THEN 'Deep Link'
       WHEN ev.event_name = 'enter_from_preconfigured_cart' THEN 'Preconfigured Cart'
       WHEN ev.event_name = 'enter_from_expired_trial_page' THEN 'Expired Trial Page'
       ELSE null END as cart_entrance
    , DATETIME(ev.created_at, "America/Los_Angeles") as created_at
  from expt2 e
  join  product_experimentation.experimentation_account_events ev
    on e.experiment_name = ev.experiment_name
      and e.account_id = ev.account_id
  where ev.event_name IN ('click_compare_plans', 'click_buy_your_trial', 'click_buy_zendesk','click_learn_more_see_all_plans','click_learn_more_get_plan_recommendation','click_purchase_admin_center','enter_from_deep_link','enter_from_preconfigured_cart' ,'enter_from_expired_trial_page')
  order by account_id)
)
, events as (
  select e.*
, ev.event_name, DATE(ev.created_at, "America/Los_Angeles") as created_at
, row_number () over (partition by ev.account_id, ev.experiment_name order by ev.created_at) nth_event
, f.first_cart_entrance 

## NOT USING THIS. CHECK TO DELETE ## 
/* 
## Treatment Only
, if(event_name like '%buy_your_trial%',1,0)  is_click_buy_your_trial
, if(event_name like '%compare_plans%', 1, 0) is_compare_plans

## Treatment and Control - Preset
## Flags for filling in missing preset clicks. If certain events directly follows view_presets, mark it as preset click
, if(event_name like 'click_preset_trial_plan' or
     (ev.event_name = 'view_payment_trial_plan' and lag(ev.event_name) over (partition by ev.account_id order by ev.created_at) = 'view_presets_v3'), 1, 0) is_click_trial_plan
, if(event_name like 'click_preset_all_plans' or
     (ev.event_name = 'view_suite_plan' and lag(ev.event_name) over (partition by ev.account_id order by ev.created_at) = 'view_presets_v3'), 1, 0) is_click_all_plans
, if(event_name like 'click_preset_quiz' or
     (ev.event_name = 'view_quiz_channels' and lag(ev.event_name) over (partition by ev.account_id order by ev.created_at) = 'view_presets_v3'), 1, 0) is_click_quiz


## Flag Suite plan views directly from All plans preset (not through Quiz flow, click see all plans)
, if(ev.event_name = 'view_suite_plan' and # should NOT be directly preceded or followed by click_quiz_see_all_plans (timestamp order can be swapped)
       ifnull(lag(ev.event_name) over (partition by ev.account_id order by ev.created_at), '') <> 'click_quiz_see_all_plans' and
      ifnull(lead(ev.event_name) over (partition by ev.account_id order by ev.created_at), '') <> 'click_quiz_see_all_plans' , 1, 0) is_view_suite_plan_not_on_quiz 
, if(ev.event_name = 'view_support_plan' and # should NOT be directly preceded or followed by click_quiz_see_all_plans (timestamp order can be swapped)
       ifnull(lag(ev.event_name) over (partition by ev.account_id order by ev.created_at), '') <> 'click_quiz_see_all_plans' and
      ifnull(lead(ev.event_name) over (partition by ev.account_id order by ev.created_at), '') <> 'click_quiz_see_all_plans' , 1, 0) is_view_support_plan_not_on_quiz       
*/       
from expt2 e
left join product_experimentation.experimentation_account_events ev
  on e.experiment_name = ev.experiment_name
    and e.account_id = ev.account_id
left join first_cart_entry f 
  on f.account_id = e.account_id
    and row_num = 1
)


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


## Flag invalid trial/account types (DirectBuy, non-Suite Trial, tests, junk, fraud). Filter out all known testing accounts from the dataset
SELECT *,
DATE(expt_created_at_pt) expt_created_date
FROM 
(
select 
if(  ifnull(is_direct_buy,0) = 1 
  or ifnull(trial_type,'') in ('Chat', 'Sell', 'Support')
  or ifnull(is_test,0) = 1
  or ifnull(is_invalid_account_type,0) = 1
  or num_variations > 1 # multiple assignment
, 0, 1) is_valid
, * 
from (

select e.*
, if((e.subdomain like 'z3n%')
  or (e.subdomain like 'z4n%')
  , 1, 0) is_test
, if(derived_account_type not in ('Trial', 'Trial - expired', 'Customer', 'Churned', 'Unclassified', 'Freemium', 'Cancelled'), 1, 0) is_invalid_account_type
, if(ifnull(e.arr_at_win,0) < 10000, 0, 1) is_outlier_at_win
, if(ifnull(e.arr_at_mo1,0) < 10000, 0, 1) is_outlier_at_mo1
, if(ifnull(e.arr_at_win,0) < 10000 and ifnull(e.arr_at_mo1,0) < 10000, 0, 1) is_outlier


# Win attribution: Treatment
, if(first_completed_purchase_trial_plan is null, 0, is_won) is_won_from_trial_plan
, if(first_clicked_preset_trial_plan is null, 0, is_won) is_won_after_trial_plan_click
, if(first_completed_purchase_compare_plans is null, 0, is_won) is_won_after_compare_plans_click
, if(first_completed_purchase_all_plans is null, 0, is_won) is_won_from_all_plans
, if(first_clicked_preset_all_plans is null, 0, is_won) is_won_after_all_plans_click

# Win attribution: Control
--, if(first_completed_purchase_suite is null, 0, is_won) is_won_from_suite
, if(first_clicked_preset_suite is null, 0, is_won) is_won_after_suite_click
--, if(first_completed_purchase_support is null, 0, is_won) is_won_from_support
, if(first_clicked_preset_support is null, 0, is_won) is_won_after_support_click

, ev.* except (account_id)

from expt2 e
left join first_events ev
  on e.account_id = ev.account_id
) 

)
WHERE is_valid = 1
order by variation desc, account_id






