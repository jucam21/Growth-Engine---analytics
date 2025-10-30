--- Check previous cart experiment events



---------------------------------------------
---- A/B test query

--- Step 1
--- List of accounts enrolled in Accelerated Cart experiment
with expt as (
    select distinct 
        standard_experiment_name experiment_name, 
        case 
            when standard_experiment_participation_variation = 'treatment' then 'V1: Variant' 
            when standard_experiment_participation_variation = 'control' then 'V0: Control' 
            else NULL 
        end as variation, 
        instance_account_id, 
        convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as expt_created_at_pt
    from propagated_cleansed.pda.base_standard_experiment_account_participations participations
    where 
        lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
),

--- Step 2
--- Check for duplicate assignments (account_id in more than 1 variant)
dups as (
    select 
        instance_account_id, 
        count(distinct variation) as num_variations, 
        listagg(distinct variation, ', ') within group (order by variation) as variations
    from expt
    group by 1
),

--- Step 3
--- Organize Account and Experiment Data via Account_id
expt2_raw as (
    select
        e.experiment_name,
        e.variation,
        e.instance_account_id as account_id,
        e.expt_created_at_pt,
        date(dim_instance.instance_account_created_timestamp) account_created_at,
        dim_instance.instance_account_subdomain subdomain,
        dim_instance.instance_account_derived_type derived_account_type,
        trials.trial_type,
        trials.is_direct_buy,
        dim_instance.instance_account_is_abusive is_abusive,
        trials.region, --, max(r.region) region <- field not in dim_instance
        dim_instance.instance_account_address_country country,
        '--' as pod_id, --, max(r.pod_id) pod_id <- field not in dim_instance
        -- Active trial at enrollment date
        iff(datediff('day', account_created_at, date(e.expt_created_at_pt)) <= 15, 1, 0) as active_trial_at_enrollment,
        --Guardrail metrics
        '--' as first_engagement_created, --, max(if(af.first_engagement_date is not null, 1, 0)) first_engagement_created <- field not in trials yet
        iff(trials.first_ticket_created_date is not null, 1, 0) first_ticket_created,
        '--' as first_hc_article_created, -- iff(trials.hc_article_created is not null, 1, 0) first_hc_article_created // field not in trials yet
        iff(trials.agent_commented_date is not null, 1, 0) agent_commented,
        iff(trials.second_agent_added_date is not null, 1, 0) hc_second_agent,
        iff(trials.hc_created_date is not null, 1, 0) hc_created,
        --Conversion & Singposts metrics
        iff(trials.first_verified_date is not null, 1, 0) verified_trial,
        iff(trials.first_shopping_cart_visit_timestamp is not null, 1, 0) first_cart_entrance_trial_accounts,
        --- Wins 
        iff(trials.win_date is not null, 1, 0) is_won,
        iff(date_trunc('month', trials.win_date) = date_trunc('month', trials.instance_account_created_date),1,0) is_month0_win,
        --- Wins from self-service active trials (15 day timeframe)
        case 
            when 
                trials.win_date is not null 
                and datediff('day', account_created_at, trials.win_date) <= 15
                and lower(trials.sales_model_at_win) = 'self-service'
                and is_direct_buy = false
            then 1 else 0
        end as win_ss_active,
        case 
            when 
                trials.win_date is not null 
                and datediff('day', account_created_at, trials.win_date) <= 15
                and lower(trials.sales_model_at_win) = 'self-service'
                and is_direct_buy = false
            then trials.instance_account_arr_usd_at_win else 0
        end as win_ss_active_arr,
        trials.win_date,
        iff(trials.win_date is not null and trials.win_date < dateadd('day', -30, current_date()), 1, 0) won_30d_ago,
        iff(trials.win_date is not null, trials.instance_account_arr_usd_at_win, 0) arr_at_win,
        iff(trials.win_date is not null, trials.instance_account_arr_usd_at_mo1, null) arr_at_mo1,
        iff(trials.win_date is not null, trials.instance_account_arr_usd_at_mo3, null) arr_at_mo3,
        iff(trials.win_date is not null, trials.instance_account_arr_usd_at_last_snapshot, null) arr_at_refresh,
        '--' as arr_band_at_win, --, iff(trials.win_date is not null, arr_band_at_win, null) arr_band_at_win <- field not in trials yet
        case when trials.is_personal_domain= true then 0 else 1 end as qualified_trial_flag,
        trials.seats_capacity_at_win seats_at_win,
        iff(trials.seats_capacity_at_win >= 6, '6+', to_char(trials.seats_capacity_at_win)) seats_at_win_group,
        trials.product_mix_at_win,
        --, trials.core_base_plan_at_win product_mix
        trials.core_base_plan_at_win,
        case
            when trials.core_base_plan_at_win like '%Suite%' then 'Suite'
            when trials.core_base_plan_at_win like '%Support%' then 'Support'
            when trials.core_base_plan_at_win like '%Employee%' then 'Employee Service'
            else null
        end as support_plan_at_win,
        trials.sales_model_at_win sales_model_at_win,
        '--' as currentterm_at_win, --, max(currentterm_at_win) currentterm_at_win <- field not in trials
        '--' as support_or_suite_win, --, max(case when w.is_suite_at_win = 1 and af.win_dt is not null then "Suite"
        //        when ifnull(w.is_suite_at_win,0) = 0 and af.win_dt is not null then "Support"
        //        else null end) support_or_suite_win <- fields not in trials
        '--' as billing_period_at_win, --, max(if(w.max_billing_period_at_win >= 12, 'Annual', 'Monthly')) billing_period_at_win <- field not in trials
        '--' as support_or_suite_plan_at_win, --, max(if(w.is_suite_at_win = 1, concat('Suite ', w.spp_suite_plan_at_win), concat('Support ', w.support_plan_at_win))) support_or_suite_plan_at_win	
        trials.help_desk_size_grouped as crm_employee_size_band, --, string_agg(distinct af.crm_employee_size_band) crm_employee_size_band		 
        '--' as crm_employee_size_subband,  --, string_agg(distinct af.crm_employee_size_subband) crm_employee_size_subband
        --Experiment variations
        trials.help_desk_size_grouped as employee_range_band,
        d.num_variations,
        '--' as is_test ,
        '--' as is_invalid_account_type,
        --, iff(trials.first_resolved_ticket_date is not null, 1, 0) first_ticket_resolved
        --, iff(trials.go_live_date is not null, 1, 0) go_live

        --- Case to join derived account either at win or at experiment enrollment
        case when trials.win_date is not null then dateadd('day', -1, trials.win_date) else date(e.expt_created_at_pt) end as date_join_derived_account_type,
    from expt e
    left join dups d
        on e.instance_account_id = d.instance_account_id
    left join foundational.customer.dim_instance_accounts_daily_snapshot dim_instance
        on e.instance_account_id = dim_instance.instance_account_id
        and dim_instance.source_snapshot_date = (
            select max(source_snapshot_date) 
            from foundational.customer.dim_instance_accounts_daily_snapshot
        )
    left join presentation.growth_analytics.trial_accounts as trials
        on trials.instance_account_id = e.instance_account_id
),

--- Step 3.1: Add derived account type at win date or enrollment date

expt2 as (
    select 
        *,
        dim_instance_v2.instance_account_derived_type as derived_account_type_at_win_or_enrollment
    from expt2_raw expt2_raw_
    --- Joining to extract derived account type at exp enrollment
    left join foundational.customer.dim_instance_accounts_daily_snapshot dim_instance_v2
        on expt2_raw_.account_id = dim_instance_v2.instance_account_id
        and dim_instance_v2.source_snapshot_date = expt2_raw_.date_join_derived_account_type
),

--- First cart entry & source
first_cart_entry as (
    select 
        account_id, 
        cart_entrance as first_cart_entrance, 
        row_number () over (partition by account_id order by created_at) as row_num
    from (
        select 
            ev.instance_account_id account_id,
            case 
                when ev.standard_experiment_account_event_name = 'click_compare_plans' then 'Compare Plans'
                when ev.standard_experiment_account_event_name = 'click_buy_your_trial' then 'Buy Your Trial'
                else null 
            end as cart_entrance, 
            convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as created_at
        from expt2 e
        inner join propagated_cleansed.pda.base_standard_experiment_account_events ev
            on 
                e.account_id = ev.instance_account_id
                and ev.standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
        where 
            ev.standard_experiment_account_event_name in ('click_compare_plans', 'click_buy_your_trial')
    )
    qualify row_num = 1
),

events as (
    select 
        e.*, 
        ev.standard_experiment_account_event_name event_name, 
        convert_timezone('UTC', 'America/Los_Angeles', ev.created_timestamp) as created_at,
        row_number () over (partition by ev.instance_account_id order by created_at) nth_event,
        f.first_cart_entrance
    from expt2 e
    left join propagated_cleansed.pda.base_standard_experiment_account_events ev
            on 
                e.account_id = ev.instance_account_id
                and ev.standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
                and convert_timezone('UTC', 'America/Los_Angeles', ev.created_timestamp) >= '2025-08-11'
    left join first_cart_entry f 
        on f.account_id = e.account_id
),
        
events_full_list as(
    select distinct
        instance_account_id as account_id,
        --- Event flags
        count(distinct case when standard_experiment_account_event_name = 'click_compare_plans' then account_id else null end) as click_compare_plans,
        count(distinct case when standard_experiment_account_event_name = 'click_buy_your_trial' then account_id else null end) as click_buy_your_trial,
        count(distinct case when standard_experiment_account_event_name = 'trial_recommendation_modal_auto_popup' then account_id else null end) as trial_modal_load_auto_popup,
        count(distinct case when standard_experiment_account_event_name = 'trial_modal_load_compare_plans' then account_id else null end) as trial_modal_load_compare_plans,
        count(distinct case when standard_experiment_account_event_name = 'click_trial_modal_auto_popup' then account_id else null end) as click_trial_modal_auto_popup,
        count(distinct case when standard_experiment_account_event_name like '%click_trial_modal_buy_now%' then account_id else null end) as click_trial_modal_buy_now,
        count(distinct case when standard_experiment_account_event_name = 'click_trial_select_all_plans' then account_id else null end) as click_trial_select_all_plans,
        count(distinct case when standard_experiment_account_event_name = 'click_trial_modal_dismiss' then account_id else null end) as click_trial_modal_dismiss,
        count(distinct case when standard_experiment_account_event_name = 'view_suite_plan' then account_id else null end) as view_suite_plan,
        count(distinct case when standard_experiment_account_event_name = 'view_support_plan' then account_id else null end) as view_support_plan,
        count(distinct case when standard_experiment_account_event_name = 'click_buy_now_suite_Professional' then account_id else null end) as click_buy_now_suite_professional,
        count(distinct case when standard_experiment_account_event_name = 'click_buy_now_support_Professional' then account_id else null end) as click_buy_now_support_professional,
        count(distinct case when standard_experiment_account_event_name = 'click_back_button_all_plans' then account_id else null end) as click_back_button_all_plans,
        count(distinct case when standard_experiment_account_event_name = 'view_payment_modal_buy_now' then account_id else null end) as view_payment_modal_buy_now,
        count(distinct case when standard_experiment_account_event_name = 'view_payment_buy_your_trial' then account_id else null end) as view_payment_buy_your_trial,
        count(distinct case when standard_experiment_account_event_name = 'view_payment_compare_plans' then account_id else null end) as view_payment_compare_plans,
        count(distinct case when standard_experiment_account_event_name = 'view_payment_modal_auto_popup' then account_id else null end) as view_payment_modal_auto_popup,
        count(distinct case when standard_experiment_account_event_name = 'complete_purchase_buy_your_trial_trial' then account_id else null end) as complete_purchase_buy_your_trial_trial,
        count(distinct case when standard_experiment_account_event_name = 'complete_purchase_modal_buy_now_trial' then account_id else null end) as complete_purchase_modal_buy_now_trial,
        count(distinct case when standard_experiment_account_event_name = 'complete_purchase_compare_plans_trial' then account_id else null end) as complete_purchase_compare_plans_trial,
        count(distinct case when standard_experiment_account_event_name = 'complete_purchase_modal_auto_popup' then account_id else null end) as complete_purchase_modal_auto_popup,
        count(distinct case when standard_experiment_account_event_name = 'payment_successful_buy_your_trial' then account_id else null end) as payment_successful_buy_your_trial,
        count(distinct case when standard_experiment_account_event_name = 'payment_successful_modal_buy_now' then account_id else null end) as payment_successful_modal_buy_now,
        count(distinct case when standard_experiment_account_event_name = 'payment_successful_compare_plans' then account_id else null end) as payment_successful_compare_plans,
        count(distinct case when standard_experiment_account_event_name = 'payment_successful_modal_auto_popup' then account_id else null end) as payment_successful_modal_auto_popup,
    from propagated_cleansed.pda.base_standard_experiment_account_events
    where 
        standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
    group by 1
),

--- # First events for critical funnel steps
first_events as (
    select 
        e.account_id, 
        e.first_cart_entrance,
        max(iff(nth_event = 1, e.event_name, null)) first_event,
        min(iff(nth_event = 1, e.created_at, null)) first_event_at,
  
        --- First CTA click
        min(iff(event_name like 'click_compare_plans', created_at, null)) first_click_compare_plans,
        min(iff(event_name like 'click_buy_your_trial', created_at, null)) first_click_buy_your_trial,

        --- Modal recommendation load
        min(iff(event_name like 'trial_recommendation_modal_auto_popup', created_at, null)) first_load_trial_modal_load_auto_popup,
        min(iff(event_name like 'trial_modal_load_compare_plans', created_at, null)) first_load_trial_modal_load_compare_plans,
        min(iff(
            event_name like 'trial_recommendation_modal_auto_popup' or event_name like 'trial_modal_load_compare_plans', created_at, null
            )) first_load_modal_load,

        --- Modal recommendation clicks
        min(iff(event_name like 'click_trial_select_all_plans', created_at, null)) first_click_trial_select_all_plans,
        min(iff(event_name like '%click_trial_modal_buy_now%', created_at, null)) first_click_trial_modal_buy_now,
        min(iff(event_name like 'click_trial_modal_dismiss', created_at, null)) first_click_trial_modal_dismiss,
        
        --- Pricing lineup page
        min(iff(event_name like 'view_suite_plan', created_at, null)) first_view_suite_plan,
        min(iff(event_name like 'view_support_plan', created_at, null)) first_view_support_plan,
        min(iff(event_name like '%click_buy_now_%', created_at, null)) first_click_buy_now_pricing_lineup,
        min(iff(event_name like 'click_back_button_all_plans', created_at, null)) first_click_back_button_all_plans,

        --- Payment page

        --- View payment
        min(iff(event_name like 'view_payment_modal_buy_now', created_at, null)) first_view_payment_modal_buy_now,
        min(iff(event_name like 'view_payment_buy_your_trial', created_at, null)) first_view_payment_buy_your_trial,
        min(iff(event_name like 'view_payment_compare_plans', created_at, null)) first_view_payment_compare_plans,
        min(iff(event_name like 'view_payment_modal_auto_popup', created_at, null)) first_view_payment_modal_auto_popup,
        --- All payment page visits from events above
        min(iff(
            event_name like '%view_payment%' and (event_name not like '%direct%' or event_name not like '%all_plans%'), created_at, null
        )) first_view_payment,

        --- Complete purchase
        min(iff(event_name like 'complete_purchase_buy_your_trial_trial', created_at, null)) first_complete_purchase_buy_your_trial_trial,
        min(iff(event_name like 'complete_purchase_modal_buy_now_trial', created_at, null)) first_complete_purchase_modal_buy_now_trial,
        min(iff(event_name like 'complete_purchase_compare_plans_trial', created_at, null)) first_complete_purchase_compare_plans_trial,
        min(iff(event_name like 'complete_purchase_modal_auto_popup', created_at, null)) first_complete_purchase_modal_auto_popup,
        min(iff(
            event_name like '%complete_purchase%' and (event_name not like '%direct%' or event_name not like '%all_plans%'), created_at, null
        )) first_complete_purchase,

        --- Payment successful
        min(iff(event_name like 'payment_successful_buy_your_trial', created_at, null)) first_payment_successful_buy_your_trial,
        min(iff(event_name like 'payment_successful_modal_buy_now', created_at, null)) first_payment_successful_modal_buy_now,
        min(iff(event_name like 'payment_successful_compare_plans', created_at, null)) first_payment_successful_compare_plans,
        min(iff(event_name like 'payment_successful_modal_auto_popup', created_at, null)) first_payment_successful_modal_auto_popup,
        min(iff(
            event_name like '%payment_successful%' and (event_name not like '%direct%' or event_name not like '%all_plans%'), created_at, null
        )) first_payment_successful,

    from events e
    group by 1,2
),

-- ## Flag invalid trial/account types (DirectBuy, non-Suite Trial, tests, junk, fraud). Filter out all known testing accounts from the dataset
main as (
    select 
        main_.*,
        --- Wins from customers interacted modal
        case 
            when 
                main_.win_ss_active = 1 
                and events_full_list_.click_compare_plans is not null
                and events_full_list_.click_buy_your_trial is not null
            then 1 else 0 
        end as is_won_2_cta_interacted,
        case 
            when 
                main_.win_ss_active = 1 
                and events_full_list_.click_compare_plans is not null
                and events_full_list_.click_buy_your_trial is not null
            then main_.win_ss_active_arr else 0 
        end as is_won_2_cta_interacted_arr,
        date(main_.expt_created_at_pt) expt_created_date,
        events_full_list_.* exclude (account_id)
    from (
        select 
            *,
            iff(
                is_direct_buy = true
                or trial_type in ('Chat', 'Sell') 
                or derived_account_type not in ('Active Trial', 'Expired Trial', 'Paying Instance', 'Cancelled', 'Deleted', 'Suspended')
                -- // multiple assignment
                or num_variations > 1 , 0, 1
                ) is_valid
        from (
            select 
                e.*,
                iff(
                    e.subdomain like 'z3n%' or e.subdomain like 'z4n%', 1, 0
                    ) is_test,
                iff(derived_account_type not in ('Trial', 'Trial - expired', 'Customer', 'Churned', 'Unclassified', 'Freemium', 'Cancelled'), 1, 0) is_invalid_account_type,
                iff(ifnull(e.arr_at_win,0) < 10000, 0, 1) is_outlier_at_win,
                iff(ifnull(e.arr_at_mo1,0) < 10000, 0, 1) is_outlier_at_mo1,
                iff(ifnull(e.arr_at_win,0) < 10000 and ifnull(e.arr_at_mo1,0) < 10000, 0, 1) is_outlier,
                --# Wins attribution: treatment & control
                iff(first_complete_purchase_buy_your_trial_trial is null, 0, is_won) is_won_from_buy_your_trial,
                iff(first_complete_purchase_modal_buy_now_trial is null, 0, is_won) is_won_after_modal_buy_now_click,
                iff(first_complete_purchase_compare_plans_trial is null, 0, is_won) is_won_after_compare_plans_click,
                iff(first_complete_purchase_modal_auto_popup is null, 0, is_won) is_won_from_modal_auto_popup,
                ev.* exclude (account_id),
            from expt2 e
            left join first_events ev
                on e.account_id = ev.account_id
        ) 
    ) main_
    left join events_full_list events_full_list_
        on events_full_list_.account_id = main_.account_id
    where is_valid = 1
    order by variation desc, account_id
)


--- Query for Bayesian - Cart visits to win rate

select
    variation,
    count(distinct case when first_cart_entrance is not null then account_id else null end) as total_cart_visits,
    sum(is_won) as total_wins,
from main
where
    (is_direct_buy = false or is_direct_buy is null) -- Exclude direct buy accounts
    and (lower(sales_model_at_win) = 'self-service' or sales_model_at_win is null) -- Exclude accounts with sales model at win
group by 1
order by 1





--- Query for Bayesian - Win rate

select
    variation,
    count(distinct account_id) as total_accounts,
    sum(is_won) as total_wins,
from main
where
    (is_direct_buy = false or is_direct_buy is null) -- Exclude direct buy accounts
    and (lower(sales_model_at_win) = 'self-service' or sales_model_at_win is null) -- Exclude accounts with sales model at win
group by 1
order by 1






--- Main query select
select *, convert_timezone('UTC', 'America/Los_Angeles', current_timestamp) as updated_at
from main
limit 10




--- Cols for join

experiment_name,
variation,
account_id, 
account_created_at,
expt_created_at_pt,
subdomain,
DERIVED_ACCOUNT_TYPE,
DATE_JOIN_DERIVED_ACCOUNT_TYPE,
DERIVED_ACCOUNT_TYPE_AT_WIN_OR_ENROLLMENT,
CLICK_COMPARE_PLANS,
CLICK_BUY_YOUR_TRIAL,
trial_modal_load_auto_popup,
TRIAL_MODAL_LOAD_COMPARE_PLANS,
CLICK_TRIAL_MODAL_AUTO_POPUP,
CLICK_TRIAL_MODAL_BUY_NOW,
CLICK_TRIAL_SELECT_ALL_PLANS,
CLICK_TRIAL_MODAL_DISMISS,
VIEW_SUITE_PLAN,
CLICK_BUY_NOW_SUITE_PROFESSIONAL,
CLICK_BUY_NOW_SUPPORT_PROFESSIONAL,
CLICK_BACK_BUTTON_ALL_PLANS,
VIEW_PAYMENT_MODAL_BUY_NOW,
VIEW_PAYMENT_BUY_YOUR_TRIAL,
VIEW_PAYMENT_COMPARE_PLANS,
VIEW_PAYMENT_MODAL_AUTO_POPUP,
COMPLETE_PURCHASE_BUY_YOUR_TRIAL_TRIAL,
COMPLETE_PURCHASE_MODAL_BUY_NOW_TRIAL,
COMPLETE_PURCHASE_COMPARE_PLANS_TRIAL,
COMPLETE_PURCHASE_MODAL_AUTO_POPUP,
PAYMENT_SUCCESSFUL_BUY_YOUR_TRIAL,
PAYMENT_SUCCESSFUL_MODAL_BUY_NOW,
PAYMENT_SUCCESSFUL_COMPARE_PLANS,
PAYMENT_SUCCESSFUL_MODAL_AUTO_POPUP





---------------------------------------------
---- Investigate if zuora coupon is applied
--- Only tmp coupons are applied


with won_accounts as (
    select 
        convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) exp_created_at_pt,
        participations.instance_account_id,
        participations.standard_experiment_participation_variation variation,
        accounts.win_date,
        accounts.instance_account_arr_usd_at_win
    from propagated_cleansed.pda.base_standard_experiment_account_participations participations
    inner join presentation.growth_analytics.trial_accounts accounts
        on participations.instance_account_id = accounts.instance_account_id
        and accounts.win_date is not null
    where 
        lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
        and participations.instance_account_id in (
            25497201,
            25511594,
            25541455,
            25563590,
            25567844
        )
)

select 
    won_accounts_.*,
    finance.service_date,
    finance.billing_account_id,
    finance.list_price_arr_usd,
    finance.gross_arr_usd,
    finance.net_arr_usd,
    --- Discounts
    finance.temp_discount_arr_usd,
    finance.recurring_discount_arr_usd,
    finance.list_price_discount_arr_usd,
    finance.nonrecurring_discount_arr_usd
from won_accounts won_accounts_
left join foundational.customer.entity_mapping_daily_snapshot as mapping
    on 
        won_accounts_.instance_account_id = mapping.instance_account_id
        and won_accounts_.win_date = mapping.source_snapshot_date
left join foundational.finance.fact_recurring_revenue_daily_snapshot_enriched finance
    on 
        mapping.billing_account_id = finance.billing_account_id
        and finance.service_date >= dateadd('day', -5, won_accounts_.win_date)
order by won_accounts_.instance_account_id, finance.service_date







select 
    mapping.instance_account_id,
    finance.service_date,
    finance.billing_account_id,
    finance.list_price_arr_usd,
    finance.gross_arr_usd,
    finance.net_arr_usd,
    --- Discounts
    finance.temp_discount_arr_usd,
    finance.recurring_discount_arr_usd,
    finance.list_price_discount_arr_usd,
    finance.nonrecurring_discount_arr_usd
from foundational.finance.fact_recurring_revenue_daily_snapshot_enriched finance
left join foundational.customer.entity_mapping_daily_snapshot as mapping
    on 
        finance.billing_account_id = mapping.billing_account_id
        and finance.service_date = mapping.source_snapshot_date
where mapping.instance_account_id = '25827566'
order by 2
limit 10





---------
--- Check in zuora



with won_accounts as (
    select 
        convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) exp_created_at_pt,
        participations.instance_account_id,
        participations.standard_experiment_participation_variation variation,
        accounts.win_date,
        accounts.instance_account_arr_usd_at_win
    from propagated_cleansed.pda.base_standard_experiment_account_participations participations
    inner join presentation.growth_analytics.trial_accounts accounts
        on participations.instance_account_id = accounts.instance_account_id
        and accounts.win_date is not null
    where 
        lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
        and participations.instance_account_id in (
            25497201,
            25511594,
            25541455,
            25563590,
            25567844
        )
),

redeemed_zuora as (
    select 
        accounts.*,
        --mapping.instance_account_id,
        zuora.up_to_periods,
        zuora.billing_period,
        zuora.charge_model,
        tiers.currency,
        tiers.discount_amount,
        tiers.discount_percentage,
        zuora.description,
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
        inner join won_accounts accounts
            on mapping.instance_account_id = accounts.instance_account_id
    where
        zuora.is_last_segment = true
        and subscription.status in ('Active', 'Expired')
        and zuora.created_date >= '2025-08-01'
        --and LOWER(coupon_id) like '%save%'
    group by
        all
)

select *
from redeemed_zuora
order by instance_account_id









select
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as date_ok,
    *
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
    and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
    and (
        lower(standard_experiment_account_event_name) like ('%complete%') 
        or lower(standard_experiment_account_event_name) like ('%payment%')
        )
    and instance_account_id in (
        25619183,
        25617630,
        25618275,
        25618658,
        25619155,
        25614592,
        25613888,
        25614704,
        25603124,
        25598881,
        25592175,
        25592545,
        25588714,
        25591629,
        25572984,
        25572795,
        25567844,
        25567598,
        25563590,
        25550438,
        25545036,
        25519069,
        25515292,
        23528184
    )
order by date_ok
    






--- Full list of events for the accounts in the experiment
select
    events_.standard_experiment_account_event_name,
    count(distinct events_.instance_account_id) as total_accounts,
    min(convert_timezone('UTC', 'America/Los_Angeles', events_.created_timestamp)) as min_date,
    max(convert_timezone('UTC', 'America/Los_Angeles', events_.created_timestamp)) as max_date
from propagated_cleansed.pda.base_standard_experiment_account_events events_
inner join propagated_cleansed.pda.base_standard_experiment_account_participations participations_
    on events_.instance_account_id = participations_.instance_account_id
    and lower(participations_.standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and participations_.standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', participations_.created_timestamp) >= '2025-08-11'
where 
    events_.standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
    and convert_timezone('UTC', 'America/Los_Angeles', events_.created_timestamp) >= '2025-08-11'
    --and (
    --    lower(events_.standard_experiment_account_event_name) like ('%complete%') 
    --    or lower(events_.standard_experiment_account_event_name) like ('%payment_successful%')
    --    )
group by 1
order by 2 desc
    




--- Extracting all "complete purchase" and "payment successful" events for the accounts in the experiment
--- No payment or complete purchase events for these accounts
select
    convert_timezone('UTC', 'America/Los_Angeles', events_.created_timestamp) as date_ok,
    events_.*
from propagated_cleansed.pda.base_standard_experiment_account_events events_
inner join propagated_cleansed.pda.base_standard_experiment_account_participations participations_
    on events_.instance_account_id = participations_.instance_account_id
    and lower(participations_.standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and participations_.standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', participations_.created_timestamp) >= '2025-08-11'
where 
    events_.standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
    and convert_timezone('UTC', 'America/Los_Angeles', events_.created_timestamp) >= '2025-08-11'
    and (
        lower(events_.standard_experiment_account_event_name) like ('%complete%') 
        or lower(events_.standard_experiment_account_event_name) like ('%payment_successful%')
        )
order by date_ok
    







--- Extracting all "complete purchase" and "payment successful" events for the accounts in the experiment
--- No payment or complete purchase events for these accounts
select
    standard_experiment_participation_variation,
    count(distinct events_.instance_account_id) as total_accounts
from propagated_cleansed.pda.base_standard_experiment_account_events events_
inner join propagated_cleansed.pda.base_standard_experiment_account_participations participations_
    on events_.instance_account_id = participations_.instance_account_id
    and lower(participations_.standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and participations_.standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', participations_.created_timestamp) >= '2025-08-11'
where 
    events_.standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
    and convert_timezone('UTC', 'America/Los_Angeles', events_.created_timestamp) >= '2025-08-11'
    and (
        lower(events_.standard_experiment_account_event_name) like ('%click_compare_plans%') 
        or lower(events_.standard_experiment_account_event_name) like ('%click_buy_your_trial%')
        )
group by 1
    









----------------------------------------
--- Some cases with weird behavior



select 
    source_snapshot_date,
    *
from foundational.customer.dim_instance_accounts_daily_snapshot
where 
    instance_account_id = 25614592
    and source_snapshot_date >= '2025-08-08'



select distinct
    date(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) date_ok,
    instance_account_id
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    standard_experiment_name = 'billing_cart_optimization'
    and standard_experiment_account_event_name = 'click_compare_plans'
    and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-12'
limit 20



select 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) date_ok,
    *
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    --instance_account_id = 25570221
    --instance_account_id = 25207842
    --instance_account_id = 25543813 Not found after click compare plans
    instance_account_id = 25207846
    and standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
    and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
order by date_ok



select 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) date_ok,
    *
from propagated_cleansed.pda.base_standard_experiment_account_participations
where 
    --lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
    --and instance_account_id = 25570221
    --and instance_account_id = 25207842
    and instance_account_id = 25543813
order by date_ok






select 
    events.standard_experiment_name,
    events.standard_experiment_account_event_name,
    count(*)
from propagated_cleansed.pda.base_standard_experiment_account_events events
inner join propagated_cleansed.pda.base_standard_experiment_account_participations participations
    on events.instance_account_id = participations.instance_account_id
    and lower(participations.standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and participations.standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', participations.created_timestamp) >= '2025-08-11'
where 
    events.standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
    and convert_timezone('UTC', 'America/Los_Angeles', events.created_timestamp) >= '2025-08-11'
    and (events.standard_experiment_account_event_name in (
    'click_compare_plans',
    'click_buy_your_trial',
    'trial_recommendation_modal_auto_popup',
    'trial_modal_load_compare_plans',
    'click_trial_modal_auto_popup',
    'click_trial_select_all_plans',
    'click_trial_modal_dismiss',
    'view_suite_plan',
    'click_buy_now_suite_Professional',
    'click_buy_now_support_Professional',
    'click_back_button_all_plans',
    'view_payment_modal_buy_now',
    'view_payment_buy_your_trial',
    'view_payment_compare_plans',
    'view_payment_modal_auto_popup',
    'complete_purchase_buy_your_trial_trial',
    'complete_purchase_modal_buy_now_trial',
    'complete_purchase_compare_plans_trial',
    'complete_purchase_modal_auto_popup',
    'payment_successful_buy_your_trial',
    'payment_successful_modal_buy_now',
    'payment_successful_compare_plans',
    'payment_successful_modal_auto_popup'
    )
    or standard_experiment_account_event_name like '%click_trial_modal_buy_now%')
group by all
order by 1 asc,2 asc,3 desc











where 





select 
    standard_experiment_name,
    variation,
    count(*) as total_accounts,
    min(expt_created_at_pt) as first_expt_created_at_pt,
    max(expt_created_at_pt) as last_expt_created_at_pt
from expt
group by all
order by 1 desc
























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


select distinct source 
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2


select distinct
    standard_experiment_account_event_name,
    STANDARD_EXPERIMENT_NAME,
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

group by 1,2
order by 1,2





--- Joining events & event names




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












-----------------------------------------------------
--- Testing cart events
--- https://docs.google.com/spreadsheets/d/1zP7fGcfuqa3yUbxV1h7T1vfjw0SOIr-1krcR2duokbU/edit?gid=0#gid=0



trial_modal_load_compare_plans
{offer_id}_click_trial_modal_buy_now
click_trial_select_all_plans
click_trial_modal_dismiss
view_suite_plan_select_all_plans
"click_buy_now_suite_Professional
click_buy_now_support_Professional"
click_back_button_all_plans
view_payment_modal_buy_now
view_payment_buy_your_trial
view_payment_compare_plans
complete_purchase_buy_your_trial_trial
complete_purchase_modal_buy_now_cta
complete_purchase_compare_plans
payment_successful_buy_your_trial
payment_successful_modal_buy_now
payment_successful_compare_plans




select distinct
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as created_timestamp,
    standard_experiment_account_event_name,
    standard_experiment_name,
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    instance_account_id = 24224646






--- Trial: 25614011
--- Control: 25614012
  


select distinct
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as created_timestamp,
    instance_account_id,
    standard_experiment_account_event_name,
    standard_experiment_name,
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
order by created_timestamp





select distinct
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as created_timestamp,
    instance_account_id,
    standard_experiment_account_event_name,
    standard_experiment_name,
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    lower(standard_experiment_name) like '%persistent_buy%'
    and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-08'
order by created_timestamp





select 
    STANDARD_EXPERIMENT_NAME,
    count(*) as total_accounts,
    min(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as first_event_time,
    max(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as last_event_time
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
group by all
order by 3
    





select 
    STANDARD_EXPERIMENT_NAME,
    count(*) as total_accounts,
    min(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as first_event_time,
    max(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as last_event_time
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-08'
    and instance_account_id in (25614011, 25614012)
group by all
order by 3
    



select *
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-08'
    and instance_account_id in (25614011, 25614012)
order by convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)








--- New testing 25628011
--- Should have all complete purchase events
select 
    STANDARD_EXPERIMENT_NAME,
    count(*) as total_accounts,
    min(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as first_event_time,
    max(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as last_event_time
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-08'
    and instance_account_id in (25628011)
group by all
order by 3
    



select 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) date_ok,
    *
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-08'
    and instance_account_id in (25628011)
    and (standard_experiment_account_event_name like '%complete_purchase%' or 
         standard_experiment_account_event_name like '%payment_successful%')
order by convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)








select 
    standard_experiment_account_event_name,
    count(*) tot_obs
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
    and standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
    and (standard_experiment_account_event_name like '%complete_purchase%')
    and instance_account_id in (25628011)
group by 1
order by 2 desc







select 
    standard_experiment_account_event_name,
    count(*) as total_events
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-08'
    and instance_account_id in (25628011)
    and (standard_experiment_account_event_name like '%complete_purchase%' or 
         standard_experiment_account_event_name like '%payment_successful%')
group by 1






select max(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as last_event_time
from propagated_cleansed.pda.base_standard_experiment_account_events







select 
    standard_experiment_name,
    count(*) as total_accounts,
    min(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as first_event_time,
    max(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as last_event_time
from propagated_cleansed.pda.base_standard_experiment_account_participations
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
group by 1
order by 3 desc



--- Checking in account participations table
select *
from propagated_cleansed.pda.base_standard_experiment_account_participations
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-10'
    and instance_account_id in (25614011, 25614012)





--- Checking in participation table
select 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as created_timestamp_adj,
    instance_account_id,
    standard_experiment_participation_variation,
    standard_experiment_name
from propagated_cleansed.pda.base_standard_experiment_account_participations
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-10'
    and instance_account_id in (25614011, 25614012)
order by convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) desc
limit 100





--- Checking in events table
select 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as created_timestamp_adj,
    instance_account_id,
    standard_experiment_account_event_name,
    standard_experiment_name
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-10'
    and instance_account_id in (25614011, 25614012)
order by convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) desc
limit 100




--- Events fired
select 
    standard_experiment_name,
    standard_experiment_account_event_name,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as first_event_time,
    max(convert_timezone('UTC', 'America/Los_Angeles', created_timestamp)) as last_event_time
    
from propagated_cleansed.pda.base_standard_experiment_account_events
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-10'
    and instance_account_id in (25614011, 25614012)
group by all
order by 1,2
limit 100






--- Max date
select 
    max(convert_timezone('UTC', 'America/Los_Angeles', updated_timestamp)) updated_timestamp,
    max(convert_timezone('UTC', 'America/Los_Angeles', zdp_meta_l1_ingest_timestamp)) zdp_meta_l1_ingest_timestamp,
    max(convert_timezone('UTC', 'America/Los_Angeles', zdp_meta_processed_timestamp)) zdp_meta_processed_timestamp,
    max(convert_timezone('UTC', 'America/Los_Angeles', zdp_meta_header_created_timestamp)) zdp_meta_header_created_timestamp
from propagated_cleansed.pda.base_standard_experiment_account_participations
where 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-10'






-------------------------------------------------
--- Check outliers by ARR


with expt as (
    select distinct 
        standard_experiment_name experiment_name, 
        case 
            when standard_experiment_participation_variation = 'treatment' then 'V1: Variant' 
            when standard_experiment_participation_variation = 'control' then 'V0: Control' 
            else NULL 
        end as variation, 
        instance_account_id, 
        convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as expt_created_at_pt
    from propagated_cleansed.pda.base_standard_experiment_account_participations participations
    where 
        lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
),

wins as (
    select 
        date_trunc(quarter, accounts.win_date) as win_quarter,
        date_trunc(month, accounts.win_date) as win_month,
        expt_.experiment_name,
        expt_.variation,
        count(*) total_wins,
        count(distinct accounts.instance_account_id) unique_wins,
        sum(accounts.instance_account_arr_usd_at_win) total_arr_wins,
        --- Bin ARR to measure outliers
        case 
            when accounts.instance_account_arr_usd_at_win <= 500 then '1. Sub 0.5K'
            when accounts.instance_account_arr_usd_at_win <= 1000 then '2. 0.5K - 1K'
            when accounts.instance_account_arr_usd_at_win <= 5000 then '3. 1K - 5K'
            when accounts.instance_account_arr_usd_at_win <= 10000 then '4. 5K - 10K'
            when accounts.instance_account_arr_usd_at_win <= 15000 then '5. 10K - 15K'
            when accounts.instance_account_arr_usd_at_win <= 20000 then '6. 15K - 20K'
            when accounts.instance_account_arr_usd_at_win <= 25000 then '7. 20K - 25K'
            when accounts.instance_account_arr_usd_at_win > 25000 then '8. Above 25K'
            else 'Unknown'
        end as arr_band
    from presentation.growth_analytics.trial_accounts accounts
    left join expt expt_
        on accounts.instance_account_id = expt_.instance_account_id
    left join foundational.customer.dim_instance_accounts_daily_snapshot dim_instance
        on accounts.instance_account_id = dim_instance.instance_account_id
        and dim_instance.source_snapshot_date = dateadd(day, 1, win_date) --day after win
    where 
        accounts.instance_account_id != 25628656 -- To ensure data will match with main analysis
        and accounts.is_direct_buy = False
        and accounts.sales_model_at_win = 'Self-service'
        and accounts.win_date >= '2025-01-01'
        --- Using the same conditions as the experiment to clasify wins
        and (expt_.variation in ('V0: Control', 'V1: Variant')
        and (
            accounts.trial_type not in ('Chat', 'Sell') 
            and (
                dim_instance.instance_account_derived_type in ('Active Trial', 'Expired Trial', 'Paying Instance', 'Cancelled', 'Deleted', 'Suspended')
                or dim_instance.instance_account_derived_type is null) -- to include new accounts not yet classified)
            ) 
            or expt_.variation is null
            ) -- to include wins from accounts not in the experiment
    group by all
),

wins_list as (
    select distinct accounts.instance_account_id
    from presentation.growth_analytics.trial_accounts accounts
    left join expt expt_
        on accounts.instance_account_id = expt_.instance_account_id
    left join foundational.customer.dim_instance_accounts_daily_snapshot dim_instance
        on accounts.instance_account_id = dim_instance.instance_account_id
        and dim_instance.source_snapshot_date = dateadd(day, 1, win_date) --day after win
    where 
        accounts.instance_account_id != 25628656 -- To ensure data will match with main analysis
        and accounts.is_direct_buy = False
        and accounts.sales_model_at_win = 'Self-service'
        and accounts.win_date >= '2025-01-01'
        and accounts.trial_type not in ('Chat', 'Sell') 
        and (dim_instance.instance_account_derived_type in ('Active Trial', 'Expired Trial', 'Paying Instance', 'Cancelled', 'Deleted', 'Suspended')
             or dim_instance.instance_account_derived_type is null) -- to include new accounts not yet classified
        and expt_.variation = 'V0: Control'
)


select 
    variation, 
    sum(total_wins) as total_wins,
    sum(unique_wins) as unique_wins
from wins
group by 1
order by 1 desc



select *, convert_timezone('UTC', 'America/Los_Angeles', current_timestamp) as updated_at
from main
limit 10









------------------------------------------------------------
--- Zuora coupons


with expt as (
    select distinct 
        standard_experiment_name experiment_name, 
        case 
            when standard_experiment_participation_variation = 'treatment' then 'V1: Variant' 
            when standard_experiment_participation_variation = 'control' then 'V0: Control' 
            else NULL 
        end as variation, 
        instance_account_id, 
        convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as expt_created_at_pt
    from propagated_cleansed.pda.base_standard_experiment_account_participations participations
    where 
        lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and standard_experiment_participation_variation in ('treatment')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
),

wins as (
    select 
        expt_.*,
        accounts.win_date,
        accounts.instance_account_arr_usd_at_win,
        accounts.sales_model_at_win,
        accounts.is_direct_buy
    from expt expt_
    left join presentation.growth_analytics.trial_accounts accounts
        on accounts.instance_account_id = expt_.instance_account_id
),

redeemed_zuora as (
    select 
        mapping.instance_account_id,
        min(zuora.created_date) as coupon_applied_date,
        zuora.up_to_periods,
        zuora.billing_period,
        zuora.charge_model,
        tiers.currency,
        tiers.discount_amount,
        tiers.discount_percentage,
        zuora.description,
        wins_.variation,
        wins_.instance_account_arr_usd_at_win,
        wins_.sales_model_at_win,
        wins_.win_date
    from cleansed.zuora.zuora_rate_plan_charges_bcv as zuora
    left join
        foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on zuora.account_id = mapping.billing_account_id
    left join cleansed.zuora.zuora_subscriptions_bcv as subscription
        on zuora.subscription_id = subscription.id
    left join cleansed.zuora.zuora_rate_plan_charge_tiers_bcv as tiers
        on zuora.id = tiers.rate_plan_charge_id
    inner join wins wins_
        on 
            mapping.instance_account_id = wins_.instance_account_id
            --- Coupon applied within 15 days of win
            and zuora.created_date <= dateadd(day, 15, wins_.win_date)
            and zuora.created_date >= dateadd(day, -15, wins_.win_date) 
    where
        zuora.is_last_segment = true
        and subscription.status in ('Active', 'Expired')
        and zuora.created_date >= '2025-08-01'
        --and lower(zuora.description) like '%get%'
    group by all
)


select 
    description,
    count(*) as total_records,
    count(distinct instance_account_id) as unique_accounts,
from redeemed_zuora
group by 1
order by 3 desc



select *, convert_timezone('UTC', 'America/Los_Angeles', current_timestamp) as updated_at
from redeemed_zuora
order by 1





--- Manually validate discount cases


with modal_load as (
    select
        account_id,
        offer_id,
        plan_name,
        preview_state,
        source,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
    group by all
),

wins as (
    select 
        modal_load_.*,
        accounts.win_date,
        accounts.instance_account_arr_usd_at_win,
        accounts.sales_model_at_win,
        accounts.core_base_plan_at_win
    from modal_load modal_load_
    left join presentation.growth_analytics.trial_accounts accounts
        on modal_load_.account_id = accounts.instance_account_id
    where 
        accounts.win_date >= '2025-01-01'
        and accounts.is_direct_buy = False
        and accounts.sales_model_at_win = 'Self-service'
)

select *
from wins
where 
    date >= '2025-08-11'
    and offer_id = '01JYH0M68B9VVK05W81XAJ6PMJ'
    and win_date is not null
    and core_base_plan_at_win = 'Support Team'





select 
    sales_model_at_win,
    count(*) as total_records,
    count(distinct instance_account_id) as unique_accounts,
from redeemed_zuora
group by 1
order by 2 desc



-- 25674979





select 
    finance.service_date,
    finance.billing_account_id,
    finance.list_price_arr_usd,
    finance.gross_arr_usd,
    finance.net_arr_usd,
    --- Discounts
    finance.temp_discount_arr_usd,
    finance.recurring_discount_arr_usd,
    finance.list_price_discount_arr_usd,
    finance.nonrecurring_discount_arr_usd
from foundational.customer.entity_mapping_daily_snapshot as mapping
left join foundational.finance.fact_recurring_revenue_daily_snapshot_enriched finance
    on 
        mapping.billing_account_id = finance.billing_account_id
        and finance.service_date >= date('2025-08-01')
where 
    --mapping.instance_account_id = 25623567
    --mapping.instance_account_id = 25599833
    --mapping.instance_account_id = 25572984
    --mapping.instance_account_id = 25641812
    --mapping.instance_account_id = 25674979
    --mapping.instance_account_id = 25673658
    --mapping.instance_account_id = 25623567
    mapping.instance_account_id = 25588201
order by mapping.instance_account_id, finance.service_date





select max(win_date)
from presentation.growth_analytics.trial_accounts




----------------------------------------------
--- Sizing new populations - rules




select 
    predictions.predicted_sku_1,
    predictions.predicted_plan_1,
    count(*)
from functional.growth_engine.dim_growth_engine_customer_accounts accounts
left join functional.growth_engine.dim_growth_engine_trial_expansion_predictions predictions
    on accounts.zendesk_account_id = predictions.instance_account_id
where 
    is_trial = TRUE
    and trial_age >= 6
    and trial_employee_count_range in ('1-9', '10-49')
    and predictions.predicted_conversion_probability < 0.7
    and lower(predictions.predicted_sku_1) = 'zendesk suite'
    --and lower(predictions.predicted_plan_1) = 'team'
group by all
order by 3 desc






select distinct trial_employee_count_range
from functional.growth_engine.dim_growth_engine_customer_accounts


select min(source_snapshot_date), max(source_snapshot_date)
from functional.growth_engine.dim_growth_engine_trial_expansion_predictions


select 
    instance_account_id,
    count(*)
from functional.growth_engine.dim_growth_engine_trial_expansion_predictions
group by 1
order by 2 desc
limit 10





-----------------------------------------------------
--- Check new recommendation wins




--- Wins

select
    win_date,
    count(*) as total_wins,
from presentation.growth_analytics.trial_accounts
where 
    win_date >= '2025-09-01'
    and is_direct_buy = False
    and sales_model_at_win = 'Self-service'
group by 1
order by 1




--- Verified trials
select
    first_verified_date,
    count(*) as total_wins,
from presentation.growth_analytics.trial_accounts
where 
    first_verified_date >= '2025-07-01'
group by 1
order by 1




--- Created trials
select
    INSTANCE_ACCOUNT_CREATED_DATE,
    count(*) as total_wins,
from presentation.growth_analytics.trial_accounts
where 
    INSTANCE_ACCOUNT_CREATED_DATE >= '2025-07-01'
group by 1
order by 1







with expt as (
    select distinct 
        standard_experiment_name experiment_name, 
        case 
            when standard_experiment_participation_variation = 'treatment' then 'V1: Variant' 
            when standard_experiment_participation_variation = 'control' then 'V0: Control' 
            else NULL 
        end as variation, 
        instance_account_id, 
        convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as expt_created_at_pt
    from propagated_cleansed.pda.base_standard_experiment_account_participations participations
    where 
        lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-09-09'
)

select 
    expt_.variation,
    count(*) as total_accounts,
    --count(distinct expt_.instance_account_id) as unique_accounts,
    count(distinct accounts.instance_account_id) as accounts_found,
    count(case when accounts.win_date is not null then accounts.instance_account_id end) as accounts_with_wins
from expt expt_
left join presentation.growth_analytics.trial_accounts accounts
    on expt_.instance_account_id = accounts.instance_account_id
group by 1







--- Validate last enrolled accounts & events


--- Still seeing enrolled accounts
select 
    convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) expt_created_at_pt,
    instance_account_id,
    standard_experiment_participation_variation,
from propagated_cleansed.pda.base_standard_experiment_account_participations participations
where 
    lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
    and standard_experiment_participation_variation in ('treatment', 'control')
    --- After launch date. Using date to remove testing accounts
    and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-09-30'
order by 1 desc





--- Problem would be for control customers not seeing the modal

with control as (
    select 
        convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) expt_created_at_pt,
        instance_account_id account_id,
        standard_experiment_participation_variation,
    from propagated_cleansed.pda.base_standard_experiment_account_participations participations
    where 
        lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-10-01'
        and standard_experiment_participation_variation = 'control'
),

event_list as (
    select *
    from control control_
    left join propagated_cleansed.pda.base_standard_experiment_account_events ev
        on
            control_.account_id = ev.instance_account_id
            and ev.standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
)

select
    standard_experiment_account_event_name,
    count(*) tot_obs,
    count(distinct instance_account_id) instance_account_id
from event_list
group by 1
order by 2 desc














----------------------------------------------------------------
--- Expansion/Churn ARR query





---------------------------------------------
---- A/B test query

--- Step 1
--- List of accounts enrolled in Accelerated Cart experiment
with expt as (
    select distinct 
        standard_experiment_name experiment_name, 
        case 
            when standard_experiment_participation_variation = 'treatment' then 'V1: Variant' 
            when standard_experiment_participation_variation = 'control' then 'V0: Control' 
            else NULL 
        end as variation, 
        instance_account_id, 
        convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as expt_created_at_pt
    from propagated_cleansed.pda.base_standard_experiment_account_participations participations
    where 
        lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
        and standard_experiment_participation_variation in ('treatment', 'control')
        --- After launch date. Using date to remove testing accounts
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
),

--- Step 2
--- Check for duplicate assignments (account_id in more than 1 variant)
dups as (
    select 
        instance_account_id, 
        count(distinct variation) as num_variations, 
        listagg(distinct variation, ', ') within group (order by variation) as variations
    from expt
    group by 1
),

--- Step 3
--- Organize Account and Experiment Data via Account_id
expt2_raw as (
    select
        e.experiment_name,
        e.variation,
        e.instance_account_id as account_id,
        e.expt_created_at_pt,
        date(dim_instance.instance_account_created_timestamp) account_created_at,
        dim_instance.instance_account_subdomain subdomain,
        dim_instance.instance_account_derived_type derived_account_type,
        trials.trial_type,
        trials.is_direct_buy,
        dim_instance.instance_account_is_abusive is_abusive,
        '--' as region, --, max(r.region) region <- field not in dim_instance
        dim_instance.instance_account_address_country country,
        '--' as pod_id, --, max(r.pod_id) pod_id <- field not in dim_instance
        -- Active trial at enrollment date
        iff(datediff('day', account_created_at, date(e.expt_created_at_pt)) <= 15, 1, 0) as active_trial_at_enrollment,
        --Guardrail metrics
        '--' as first_engagement_created, --, max(if(af.first_engagement_date is not null, 1, 0)) first_engagement_created <- field not in trials yet
        iff(trials.first_ticket_created_date is not null, 1, 0) first_ticket_created,
        '--' as first_hc_article_created, -- iff(trials.hc_article_created is not null, 1, 0) first_hc_article_created // field not in trials yet
        iff(trials.agent_commented_date is not null, 1, 0) agent_commented,
        iff(trials.second_agent_added_date is not null, 1, 0) hc_second_agent,
        iff(trials.hc_created_date is not null, 1, 0) hc_created,
        --Conversion & Singposts metrics
        iff(trials.first_verified_date is not null, 1, 0) verified_trial,
        iff(trials.first_shopping_cart_visit_timestamp is not null, 1, 0) first_cart_entrance_trial_accounts,
        --- Wins 
        iff(trials.win_date is not null, 1, 0) is_won,
        iff(date_trunc('month', trials.win_date) = date_trunc('month', trials.instance_account_created_date),1,0) is_month0_win,
        --- Wins from self-service active trials (15 day timeframe)
        case 
            when 
                trials.win_date is not null 
                and datediff('day', account_created_at, trials.win_date) <= 15
                and lower(trials.sales_model_at_win) = 'self-service'
                and is_direct_buy = false
            then 1 else 0
        end as win_ss_active,
        case 
            when 
                trials.win_date is not null 
                and datediff('day', account_created_at, trials.win_date) <= 15
                and lower(trials.sales_model_at_win) = 'self-service'
                and is_direct_buy = false
            then trials.instance_account_arr_usd_at_win else 0
        end as win_ss_active_arr,
        trials.win_date,
        iff(trials.win_date is not null and trials.win_date < dateadd('day', -30, current_date()), 1, 0) won_30d_ago,
        iff(trials.win_date is not null, trials.instance_account_arr_usd_at_win, 0) arr_at_win,
        iff(trials.win_date is not null, trials.instance_account_arr_usd_at_mo1, null) arr_at_mo1,
        iff(trials.win_date is not null, trials.instance_account_arr_usd_at_mo3, null) arr_at_mo3,
        iff(trials.win_date is not null, trials.instance_account_arr_usd_at_last_snapshot, null) arr_at_refresh,
        '--' as arr_band_at_win, --, iff(trials.win_date is not null, arr_band_at_win, null) arr_band_at_win <- field not in trials yet
        case when trials.is_personal_domain= true then 0 else 1 end as qualified_trial_flag,
        trials.seats_capacity_at_win seats_at_win,
        iff(trials.seats_capacity_at_win >= 6, '6+', to_char(trials.seats_capacity_at_win)) seats_at_win_group,
        trials.product_mix_at_win,
        --, trials.core_base_plan_at_win product_mix
        trials.core_base_plan_at_win,
        case
            when trials.core_base_plan_at_win like '%Suite%' then 'Suite'
            when trials.core_base_plan_at_win like '%Support%' then 'Support'
            when trials.core_base_plan_at_win like '%Employee%' then 'Employee Service'
            else null
        end as support_plan_at_win,
        trials.sales_model_at_win sales_model_at_win,
        '--' as currentterm_at_win, --, max(currentterm_at_win) currentterm_at_win <- field not in trials
        '--' as support_or_suite_win, --, max(case when w.is_suite_at_win = 1 and af.win_dt is not null then "Suite"
        //        when ifnull(w.is_suite_at_win,0) = 0 and af.win_dt is not null then "Support"
        //        else null end) support_or_suite_win <- fields not in trials
        '--' as billing_period_at_win, --, max(if(w.max_billing_period_at_win >= 12, 'Annual', 'Monthly')) billing_period_at_win <- field not in trials
        '--' as support_or_suite_plan_at_win, --, max(if(w.is_suite_at_win = 1, concat('Suite ', w.spp_suite_plan_at_win), concat('Support ', w.support_plan_at_win))) support_or_suite_plan_at_win	
        trials.help_desk_size_grouped as crm_employee_size_band, --, string_agg(distinct af.crm_employee_size_band) crm_employee_size_band		 
        '--' as crm_employee_size_subband,  --, string_agg(distinct af.crm_employee_size_subband) crm_employee_size_subband
        --Experiment variations
        trials.help_desk_size_grouped as employee_range_band,
        d.num_variations,
        '--' as is_test ,
        '--' as is_invalid_account_type,
        --, iff(trials.first_resolved_ticket_date is not null, 1, 0) first_ticket_resolved
        --, iff(trials.go_live_date is not null, 1, 0) go_live

        --- Case to join derived account either at win or at experiment enrollment
        case when trials.win_date is not null then dateadd('day', -1, trials.win_date) else date(e.expt_created_at_pt) end as date_join_derived_account_type,
        is_shopping_cart_visit_1hour
    from expt e
    left join dups d
        on e.instance_account_id = d.instance_account_id
    left join foundational.customer.dim_instance_accounts_daily_snapshot dim_instance
        on e.instance_account_id = dim_instance.instance_account_id
        and dim_instance.source_snapshot_date = (
            select max(source_snapshot_date) 
            from foundational.customer.dim_instance_accounts_daily_snapshot
        )
    left join presentation.growth_analytics.trial_accounts as trials
        on trials.instance_account_id = e.instance_account_id
),

--- Step 3.1: Add derived account type at win date or enrollment date

expt2 as (
    select 
        *,
        dim_instance_v2.instance_account_derived_type as derived_account_type_at_win_or_enrollment
    from expt2_raw expt2_raw_
    --- Joining to extract derived account type at exp enrollment
    left join foundational.customer.dim_instance_accounts_daily_snapshot dim_instance_v2
        on expt2_raw_.account_id = dim_instance_v2.instance_account_id
        and dim_instance_v2.source_snapshot_date = expt2_raw_.date_join_derived_account_type
),

--- First cart entry & source
first_cart_entry as (
    select 
        account_id, 
        cart_entrance as first_cart_entrance, 
        row_number () over (partition by account_id order by created_at) as row_num
    from (
        select 
            ev.instance_account_id account_id,
            case 
                when ev.standard_experiment_account_event_name = 'click_compare_plans' then 'Compare Plans'
                when ev.standard_experiment_account_event_name = 'click_buy_your_trial' then 'Buy Your Trial'
                else null 
            end as cart_entrance, 
            convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) as created_at
        from expt2 e
        inner join propagated_cleansed.pda.base_standard_experiment_account_events ev
            on 
                e.account_id = ev.instance_account_id
                and ev.standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
        where 
            ev.standard_experiment_account_event_name in ('click_compare_plans', 'click_buy_your_trial')
    )
    qualify row_num = 1
),

events as (
    select 
        e.*, 
        ev.standard_experiment_account_event_name event_name, 
        convert_timezone('UTC', 'America/Los_Angeles', ev.created_timestamp) as created_at,
        row_number () over (partition by ev.instance_account_id order by created_at) nth_event,
        f.first_cart_entrance
    from expt2 e
    left join propagated_cleansed.pda.base_standard_experiment_account_events ev
            on 
                e.account_id = ev.instance_account_id
                and ev.standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
                and convert_timezone('UTC', 'America/Los_Angeles', ev.created_timestamp) >= '2025-08-11'
    left join first_cart_entry f 
        on f.account_id = e.account_id
),
        
events_full_list as(
    select distinct
        instance_account_id as account_id,
        --- Event flags
        count(distinct case when standard_experiment_account_event_name = 'click_compare_plans' then account_id else null end) as click_compare_plans,
        count(distinct case when standard_experiment_account_event_name = 'click_buy_your_trial' then account_id else null end) as click_buy_your_trial,
        count(distinct case when standard_experiment_account_event_name = 'trial_recommendation_modal_auto_popup' then account_id else null end) as trial_modal_load_auto_popup,
        count(distinct case when standard_experiment_account_event_name = 'trial_modal_load_compare_plans' then account_id else null end) as trial_modal_load_compare_plans,
        count(distinct case when standard_experiment_account_event_name = 'click_trial_modal_auto_popup' then account_id else null end) as click_trial_modal_auto_popup,
        count(distinct case when standard_experiment_account_event_name like '%click_trial_modal_buy_now%' then account_id else null end) as click_trial_modal_buy_now,
        count(distinct case when standard_experiment_account_event_name = 'click_trial_select_all_plans' then account_id else null end) as click_trial_select_all_plans,
        count(distinct case when standard_experiment_account_event_name = 'click_trial_modal_dismiss' then account_id else null end) as click_trial_modal_dismiss,
        count(distinct case when standard_experiment_account_event_name = 'view_suite_plan' then account_id else null end) as view_suite_plan,
        count(distinct case when standard_experiment_account_event_name = 'view_support_plan' then account_id else null end) as view_support_plan,
        count(distinct case when standard_experiment_account_event_name = 'click_buy_now_suite_Professional' then account_id else null end) as click_buy_now_suite_professional,
        count(distinct case when standard_experiment_account_event_name = 'click_buy_now_support_Professional' then account_id else null end) as click_buy_now_support_professional,
        count(distinct case when standard_experiment_account_event_name = 'click_back_button_all_plans' then account_id else null end) as click_back_button_all_plans,
        count(distinct case when standard_experiment_account_event_name = 'view_payment_modal_buy_now' then account_id else null end) as view_payment_modal_buy_now,
        count(distinct case when standard_experiment_account_event_name = 'view_payment_buy_your_trial' then account_id else null end) as view_payment_buy_your_trial,
        count(distinct case when standard_experiment_account_event_name = 'view_payment_compare_plans' then account_id else null end) as view_payment_compare_plans,
        count(distinct case when standard_experiment_account_event_name = 'view_payment_modal_auto_popup' then account_id else null end) as view_payment_modal_auto_popup,
        count(distinct case when standard_experiment_account_event_name = 'complete_purchase_buy_your_trial_trial' then account_id else null end) as complete_purchase_buy_your_trial_trial,
        count(distinct case when standard_experiment_account_event_name = 'complete_purchase_modal_buy_now_trial' then account_id else null end) as complete_purchase_modal_buy_now_trial,
        count(distinct case when standard_experiment_account_event_name = 'complete_purchase_compare_plans_trial' then account_id else null end) as complete_purchase_compare_plans_trial,
        count(distinct case when standard_experiment_account_event_name = 'complete_purchase_modal_auto_popup' then account_id else null end) as complete_purchase_modal_auto_popup,
        count(distinct case when standard_experiment_account_event_name = 'payment_successful_buy_your_trial' then account_id else null end) as payment_successful_buy_your_trial,
        count(distinct case when standard_experiment_account_event_name = 'payment_successful_modal_buy_now' then account_id else null end) as payment_successful_modal_buy_now,
        count(distinct case when standard_experiment_account_event_name = 'payment_successful_compare_plans' then account_id else null end) as payment_successful_compare_plans,
        count(distinct case when standard_experiment_account_event_name = 'payment_successful_modal_auto_popup' then account_id else null end) as payment_successful_modal_auto_popup,
    from propagated_cleansed.pda.base_standard_experiment_account_events
    where 
        standard_experiment_name in ('billing_cart_optimization', 'persistent_buy_plan_recommendations')
        and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'
    group by 1
),

--- # First events for critical funnel steps
first_events as (
    select 
        e.account_id, 
        e.first_cart_entrance,
        max(iff(nth_event = 1, e.event_name, null)) first_event,
        min(iff(nth_event = 1, e.created_at, null)) first_event_at,
  
        --- First CTA click
        min(iff(event_name like 'click_compare_plans', created_at, null)) first_click_compare_plans,
        min(iff(event_name like 'click_buy_your_trial', created_at, null)) first_click_buy_your_trial,

        --- Modal recommendation load
        min(iff(event_name like 'trial_recommendation_modal_auto_popup', created_at, null)) first_load_trial_modal_load_auto_popup,
        min(iff(event_name like 'trial_modal_load_compare_plans', created_at, null)) first_load_trial_modal_load_compare_plans,
        min(iff(
            event_name like 'trial_recommendation_modal_auto_popup' or event_name like 'trial_modal_load_compare_plans', created_at, null
            )) first_load_modal_load,

        --- Modal recommendation clicks
        min(iff(event_name like 'click_trial_select_all_plans', created_at, null)) first_click_trial_select_all_plans,
        min(iff(event_name like '%click_trial_modal_buy_now%', created_at, null)) first_click_trial_modal_buy_now,
        min(iff(event_name like 'click_trial_modal_dismiss', created_at, null)) first_click_trial_modal_dismiss,
        
        --- Pricing lineup page
        min(iff(event_name like 'view_suite_plan', created_at, null)) first_view_suite_plan,
        min(iff(event_name like 'view_support_plan', created_at, null)) first_view_support_plan,
        min(iff(event_name like '%click_buy_now_%', created_at, null)) first_click_buy_now_pricing_lineup,
        min(iff(event_name like 'click_back_button_all_plans', created_at, null)) first_click_back_button_all_plans,

        --- Payment page

        --- View payment
        min(iff(event_name like 'view_payment_modal_buy_now', created_at, null)) first_view_payment_modal_buy_now,
        min(iff(event_name like 'view_payment_buy_your_trial', created_at, null)) first_view_payment_buy_your_trial,
        min(iff(event_name like 'view_payment_compare_plans', created_at, null)) first_view_payment_compare_plans,
        min(iff(event_name like 'view_payment_modal_auto_popup', created_at, null)) first_view_payment_modal_auto_popup,
        --- All payment page visits from events above
        min(iff(
            event_name like '%view_payment%' and (event_name not like '%direct%' or event_name not like '%all_plans%'), created_at, null
        )) first_view_payment,

        --- Complete purchase
        min(iff(event_name like 'complete_purchase_buy_your_trial_trial', created_at, null)) first_complete_purchase_buy_your_trial_trial,
        min(iff(event_name like 'complete_purchase_modal_buy_now_trial', created_at, null)) first_complete_purchase_modal_buy_now_trial,
        min(iff(event_name like 'complete_purchase_compare_plans_trial', created_at, null)) first_complete_purchase_compare_plans_trial,
        min(iff(event_name like 'complete_purchase_modal_auto_popup', created_at, null)) first_complete_purchase_modal_auto_popup,
        min(iff(
            event_name like '%complete_purchase%' and (event_name not like '%direct%' or event_name not like '%all_plans%'), created_at, null
        )) first_complete_purchase,

        --- Payment successful
        min(iff(event_name like 'payment_successful_buy_your_trial', created_at, null)) first_payment_successful_buy_your_trial,
        min(iff(event_name like 'payment_successful_modal_buy_now', created_at, null)) first_payment_successful_modal_buy_now,
        min(iff(event_name like 'payment_successful_compare_plans', created_at, null)) first_payment_successful_compare_plans,
        min(iff(event_name like 'payment_successful_modal_auto_popup', created_at, null)) first_payment_successful_modal_auto_popup,
        min(iff(
            event_name like '%payment_successful%' and (event_name not like '%direct%' or event_name not like '%all_plans%'), created_at, null
        )) first_payment_successful,

    from events e
    group by 1,2
),

-- ## Flag invalid trial/account types (DirectBuy, non-Suite Trial, tests, junk, fraud). Filter out all known testing accounts from the dataset
main as (
    select 
        main_.*,
        --- Wins from customers interacted modal
        case 
            when 
                main_.win_ss_active = 1 
                and events_full_list_.click_compare_plans is not null
                and events_full_list_.click_buy_your_trial is not null
            then 1 else 0 
        end as is_won_2_cta_interacted,
        case 
            when 
                main_.win_ss_active = 1 
                and events_full_list_.click_compare_plans is not null
                and events_full_list_.click_buy_your_trial is not null
            then main_.win_ss_active_arr else 0 
        end as is_won_2_cta_interacted_arr,
        date(main_.expt_created_at_pt) expt_created_date,
        events_full_list_.* exclude (account_id)
    from (
        select 
            *,
            iff(
                is_direct_buy = true
                or trial_type in ('Chat', 'Sell') 
                or derived_account_type not in ('Active Trial', 'Expired Trial', 'Paying Instance', 'Cancelled', 'Deleted', 'Suspended')
                -- // multiple assignment
                or num_variations > 1 , 0, 1
                ) is_valid
        from (
            select 
                e.*,
                iff(
                    e.subdomain like 'z3n%' or e.subdomain like 'z4n%', 1, 0
                    ) is_test,
                iff(derived_account_type not in ('Trial', 'Trial - expired', 'Customer', 'Churned', 'Unclassified', 'Freemium', 'Cancelled'), 1, 0) is_invalid_account_type,
                iff(ifnull(e.arr_at_win,0) < 10000, 0, 1) is_outlier_at_win,
                iff(ifnull(e.arr_at_mo1,0) < 10000, 0, 1) is_outlier_at_mo1,
                iff(ifnull(e.arr_at_win,0) < 10000 and ifnull(e.arr_at_mo1,0) < 10000, 0, 1) is_outlier,
                --# Wins attribution: treatment & control
                iff(first_complete_purchase_buy_your_trial_trial is null, 0, is_won) is_won_from_buy_your_trial,
                iff(first_complete_purchase_modal_buy_now_trial is null, 0, is_won) is_won_after_modal_buy_now_click,
                iff(first_complete_purchase_compare_plans_trial is null, 0, is_won) is_won_after_compare_plans_click,
                iff(first_complete_purchase_modal_auto_popup is null, 0, is_won) is_won_from_modal_auto_popup,
                ev.* exclude (account_id),
            from expt2 e
            left join first_events ev
                on e.account_id = ev.account_id
        ) 
    ) main_
    left join events_full_list events_full_list_
        on events_full_list_.account_id = main_.account_id
    where is_valid = 1
    order by variation desc, account_id
)

select *, convert_timezone('UTC', 'America/Los_Angeles', current_timestamp) as updated_at
from main
where 
  convert_timezone('UTC', 'America/Los_Angeles', expt_created_at_pt) <= '2025-09-03'
  and (win_date is null or win_date <= '2025-09-03')
--limit 10






select max(source_snapshot_date)
from presentation.product_analytics.paid_customer_universe_daily_snapshot


select *
from presentation.product_analytics.paid_customer_universe_daily_snapshot
where instance_account_id = 25435870
order by source_snapshot_date




select *
from presentation.product_analytics.paid_customer_universe_daily_snapshot
where instance_account_id = 25510572
order by source_snapshot_date




select *
from presentation.product_analytics.paid_customer_universe_daily_snapshot
where instance_account_id = 25572770
order by source_snapshot_date



select finance.*
from FOUNDATIONAL.FINANCE.FACT_RECURRING_REVENUE_DAILY_SNAPSHOT_ENRICHED finance
left join foundational.customer.entity_mapping_daily_snapshot as mapping
    on finance.billing_account_id = mapping.billing_account_id
    and finance.service_date = mapping.source_snapshot_date
where mapping.instance_account_id = 25572770
order by finance.service_date








select finance.*
from FOUNDATIONAL.FINANCE.FACT_RECURRING_REVENUE_DAILY_SNAPSHOT_ENRICHED finance
inner join foundational.customer.entity_mapping_daily_snapshot as mapping
    on finance.billing_account_id = mapping.billing_account_id
    and finance.service_date = mapping.source_snapshot_date
    and mapping.instance_account_id = 25510572
order by finance.service_date




select *
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25510572





select *
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25572770





select 
    instance_account_id,
    instance_account_name,
    source_snapshot_date,
    instance_account_derived_type
from foundational.customer.dim_instance_accounts_daily_snapshot
--where instance_account_id = 25510572
where instance_account_id = 25572770
order by source_snapshot_date 




select *
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25615371


--- Search churned accounts in experiment enrollments table


select *
from propagated_cleansed.pda.base_standard_experiment_account_participations participations
where 
    lower(standard_experiment_name) like '%persistent_buy_plan_recommendations%'
    and standard_experiment_participation_variation in ('treatment', 'control')
    and instance_account_id in (25510572, 25572770)
    --- After launch date. Using date to remove testing accounts
    and convert_timezone('UTC', 'America/Los_Angeles', created_timestamp) >= '2025-08-11'








-------------------------------------------------------------
--- Query expansion/churn - Aug11 - Sep03


with control_ as (
    select column1 as account_id
    from values 
        (25700949),
        (25699387),
        (25696466),
        (25695896),
        (25694660),
        (25691047),
        (25686550),
        (25684999),
        (25684702),
        (25682725),
        (25682173),
        (25491878),
        (25681036),
        (25679260),
        (25598567),
        (25674428),
        (25674305),
        (25673936),
        (25481789),
        (25673627),
        (25671509),
        (25670851),
        (25668337),
        (25658616),
        (25658390),
        (25658118),
        (25542760),
        (25655238),
        (25654309),
        (25653710),
        (25518009),
        (25653309),
        (25652411),
        (25652138),
        (25651392),
        (25539020),
        (25645657),
        (25647287),
        (25646756),
        (25646051),
        (25548036),
        (25569090),
        (25641931),
        (25641780),
        (25598090),
        (25641007),
        (25513470),
        (25584449),
        (25639433),
        (25590994),
        (25605991),
        (25634046),
        (25634006),
        (25633205),
        (25607097),
        (25632380),
        (25631902),
        (25631641),
        (25631474),
        (25605328),
        (25628656),
        (25577406),
        (25627193),
        (25607473),
        (25626944),
        (25626693),
        (25303044),
        (25625606),
        (25566528),
        (25567064),
        (24964213),
        (25623342),
        (25576942),
        (25622885),
        (25622577),
        (25510572),
        (25572770),
        (25563476),
        (25621895),
        (25596311),
        (25470418),
        (25620998),
        (25587991),
        (25515292),
        (25619155),
        (25522505),
        (25618658),
        (25599266),
        (25520124),
        (25574727),
        (25572795),
        (25617844),
        (25598882),
        (25617630),
        (25617649),
        (25546767),
        (25567598),
        (25617057),
        (25585240),
        (23528184),
        (25539403),
        (25611911),
        (25040080),
        (25559829),
        (25612336),
        (25545036),
        (25590897),
        (25596122),
        (25597447),
        (25599447),
        (25573352),
        (25443633),
        (25551619),
        (25601365),
        (25519069),
        (25573794),
        (25582803),
        (25536630),
        (25614606),
        (25575927),
        (25613888)
),

variant_ as (
    select column1 as account_id
    from values 
        (25700587),
        (25696485),
        (25695081),
        (25690898),
        (25688138),
        (25683841),
        (25683268),
        (25682747),
        (25680945),
        (25679184),
        (25677701),
        (25535588),
        (25677354),
        (25387942),
        (25676407),
        (25674979),
        (25673784),
        (25673658),
        (25673559),
        (25668308),
        (25661688),
        (25661185),
        (25660760),
        (25519854),
        (25658277),
        (25658218),
        (25658138),
        (25655024),
        (25653940),
        (25598514),
        (25653473),
        (25613995),
        (25094736),
        (25564504),
        (25648488),
        (25524384),
        (25647554),
        (25646392),
        (25646132),
        (25646121),
        (25590126),
        (25645941),
        (25645492),
        (25645174),
        (25642693),
        (25590010),
        (25642123),
        (25641812),
        (25641684),
        (25640778),
        (25640676),
        (25640317),
        (25639745),
        (25584803),
        (25584782),
        (25584729),
        (25584365),
        (25584340),
        (25638074),
        (25637281),
        (25636373),
        (25632570),
        (25632440),
        (25632415),
        (25594531),
        (25631957),
        (25631597),
        (25630537),
        (25630132),
        (25596939),
        (25629639),
        (25569449),
        (25628258),
        (25627264),
        (25572605),
        (25626709),
        (25580014),
        (25599833),
        (25625208),
        (25625187),
        (25538581),
        (25541266),
        (25511594),
        (25623586),
        (25623567),
        (25497201),
        (25587299),
        (25599245),
        (25525771),
        (25574154),
        (25598373),
        (25622360),
        (25547206),
        (25621939),
        (25621376),
        (25585351),
        (25435870),
        (25441648),
        (25510972),
        (25619183),
        (25603610),
        (25618884),
        (25434615),
        (25618300),
        (25618160),
        (25572984),
        (25380892),
        (25613404),
        (25594393),
        (25613620),
        (25567844),
        (25616534),
        (25525662),
        (25566975),
        (25563590),
        (25566119),
        (25611971),
        (25598881),
        (25592545),
        (25588201),
        (25588714),
        (25592175),
        (25577364),
        (25612548),
        (25614592),
        (25612801),
        (25563635),
        (25579209),
        (25607865),
        (25603124),
        (25544432),
        (25537024),
        (25606315),
        (25595486),
        (25591629),
        (25587466)
),

expt_population as (
    select 
        'V0: Control' as variation,
        account_id
    from control_
    union all
    select 
        'V1: Variant' as variation,
        account_id
    from variant_
),

main as (
    select 
        trial_accounts_.instance_account_id,
        expt_population_.variation,
        trial_accounts_.last_snapshot_date,
        trial_accounts_.win_date,
        trial_accounts_.instance_account_arr_usd_at_win arr_at_win,
        trial_accounts_.instance_account_arr_usd_at_mo1 arr_at_mo1,
        trial_accounts_.instance_account_arr_usd_at_mo2 arr_at_mo2,
        trial_accounts_.instance_account_arr_usd_at_mo3 arr_at_mo3,
        trial_accounts_.instance_account_arr_usd_at_last_snapshot arr_at_latest,
        --- Flags to indicate data availability at each timepoint
        case 
            when trial_accounts_.mo1_date <= trial_accounts_.last_snapshot_date then 1 else 0
        end as has_data_mo1,
        case 
            when trial_accounts_.mo2_date <= trial_accounts_.last_snapshot_date then 1 else 0
        end as has_data_mo2,
        case 
            when trial_accounts_.mo3_date <= trial_accounts_.last_snapshot_date then 1 else 0
        end as has_data_mo3,
        case 
            when has_data_mo1 = 1 then trial_accounts_.instance_account_arr_usd_at_win else 0 
        end as has_data_arr_mo1,
        case 
            when has_data_mo2 = 1 then trial_accounts_.instance_account_arr_usd_at_win else 0 
        end as has_data_arr_mo2,
        case 
            when has_data_mo3 = 1 then trial_accounts_.instance_account_arr_usd_at_win else 0 
        end as has_data_arr_mo3,
        --- Expansion & churn metrics
        --- Expansion flags
        case 
            when 
                trial_accounts_.mo1_date <= trial_accounts_.last_snapshot_date
                and trial_accounts_.instance_account_arr_usd_at_mo1 > trial_accounts_.instance_account_arr_usd_at_win
            then 1 else 0
        end as is_expanded_mo1,
        case 
            when 
                trial_accounts_.mo2_date <= trial_accounts_.last_snapshot_date
                and trial_accounts_.instance_account_arr_usd_at_mo2 > trial_accounts_.instance_account_arr_usd_at_win
            then 1 else 0
        end as is_expanded_mo2,
        case 
            when 
                trial_accounts_.mo3_date <= trial_accounts_.last_snapshot_date
                and trial_accounts_.instance_account_arr_usd_at_mo3 > trial_accounts_.instance_account_arr_usd_at_win
            then 1 else 0
        end as is_expanded_mo3,
        case 
            when 
                trial_accounts_.instance_account_arr_usd_at_last_snapshot > trial_accounts_.instance_account_arr_usd_at_win
            then 1 else 0
        end as is_expanded_latest,
        --- Expansion amounts
        case 
            when is_expanded_mo1 = 1 
            then trial_accounts_.instance_account_arr_usd_at_mo1 - trial_accounts_.instance_account_arr_usd_at_win 
            else 0 
        end as expansion_amount_mo1,
        case 
            when is_expanded_mo2 = 1 
            then trial_accounts_.instance_account_arr_usd_at_mo2 - trial_accounts_.instance_account_arr_usd_at_win 
            else 0 
        end as expansion_amount_mo2,
        case 
            when is_expanded_mo3 = 1 
            then trial_accounts_.instance_account_arr_usd_at_mo3 - trial_accounts_.instance_account_arr_usd_at_win 
            else 0 
        end as expansion_amount_mo3,
        case 
            when is_expanded_latest = 1 
            then trial_accounts_.instance_account_arr_usd_at_last_snapshot - trial_accounts_.instance_account_arr_usd_at_win 
            else 0 
        end as expansion_amount_latest,
        --- Churn flags
        case 
            when 
                trial_accounts_.mo1_date <= trial_accounts_.last_snapshot_date
                and (trial_accounts_.instance_account_arr_usd_at_mo1 = 0 or trial_accounts_.instance_account_arr_usd_at_mo1 is null)
            then 1 else 0
        end as is_churned_mo1,
        case 
            when 
                (trial_accounts_.mo2_date <= trial_accounts_.last_snapshot_date
                and (trial_accounts_.instance_account_arr_usd_at_mo2 = 0 or trial_accounts_.instance_account_arr_usd_at_mo2 is null))
                or is_churned_mo1 = 1
            then 1 else 0
        end as is_churned_mo2,
        case 
            when 
                (trial_accounts_.mo3_date <= trial_accounts_.last_snapshot_date
                and (trial_accounts_.instance_account_arr_usd_at_mo3 = 0 or trial_accounts_.instance_account_arr_usd_at_mo3 is null))
                or is_churned_mo2 = 1
            then 1 else 0
        end as is_churned_mo3,
        case 
            when 
                trial_accounts_.instance_account_arr_usd_at_last_snapshot = 0 or trial_accounts_.instance_account_arr_usd_at_last_snapshot is null
            then 1 else 0
        end as is_churned_latest,
        --- Churn amounts
        case 
            when is_churned_mo1 = 1
            then trial_accounts_.instance_account_arr_usd_at_win
            else 0
        end as churn_amount_mo1,
        case 
            when is_churned_mo2 = 1
            then trial_accounts_.instance_account_arr_usd_at_win
            else 0
        end as churn_amount_mo2,
        case 
            when is_churned_mo3 = 1
            then trial_accounts_.instance_account_arr_usd_at_win    
            else 0
        end as churn_amount_mo3,
        case 
            when is_churned_latest = 1
            then trial_accounts_.instance_account_arr_usd_at_win
            else 0
        end as churn_amount_latest
    from presentation.growth_analytics.trial_accounts trial_accounts_
    inner join expt_population expt_population_
        on trial_accounts_.instance_account_id = expt_population_.account_id
)


select *, convert_timezone('UTC', 'America/Los_Angeles', current_timestamp) as updated_at
from main



select *
from main
where is_churned_latest = 1
and variation = 'variant'





select 
    variation,
    count(*) tot_obs,
    count(distinct instance_account_id) instance_account_id,
    count(distinct case when win_date is not null then instance_account_id else null end) as won_accounts,
    sum(is_expanded_mo1) as expanded_mo1_accounts,
    sum(is_expanded_mo2) as expanded_mo2_accounts,
    sum(is_expanded_mo3) as expanded_mo3_accounts,
    sum(is_expanded_latest) as expanded_latest_accounts,
    sum(is_churned_mo1) as churned_mo1_accounts,
    sum(is_churned_mo2) as churned_mo2_accounts,
    sum(is_churned_mo3) as churned_mo3_accounts,
    sum(is_churned_latest) as churned_latest_accounts,
from main
group by 1
order by 2 desc





--- Customer expanded and then churned
select *
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25641812



















-------------------------------------------------------------
--- Query expansion/churn - Sep09 - Sep30


with control_ as (
    select column1 as account_id
    from values 
        (25726135),
        (25726783),
        (25726913),
        (25727189),
        (25465466),
        (25728039),
        (25729243),
        (25729477),
        (25550799),
        (25729833),
        (25730180),
        (25730660),
        (25731917),
        (25732347),
        (25732438),
        (25732561),
        (25733269),
        (25733543),
        (25734149),
        (25734537),
        (25735499),
        (25735950),
        (25736534),
        (25737980),
        (25738230),
        (25738642),
        (25740685),
        (25741709),
        (25742992),
        (25743200),
        (25743224),
        (25743284),
        (25743377),
        (25743464),
        (25744099),
        (25744109),
        (25744932),
        (25745305),
        (25746407),
        (25746420),
        (25747373),
        (25747605),
        (25748423),
        (25750027),
        (25750259),
        (25752591),
        (25752733),
        (25756035),
        (25757164),
        (25758320),
        (25762038),
        (25763028),
        (25765749),
        (25766302),
        (25767265),
        (25767463),
        (25768379),
        (25770726),
        (25771544),
        (25773298),
        (25773295),
        (25778956)
),

variant_ as (
    select column1 as account_id
    from values 
        (25726122),
        (25726432),
        (25727961),
        (25727969),
        (25729467),
        (25729854),
        (25730019),
        (25730068),
        (25730156),
        (25730917),
        (25730996),
        (25732234),
        (24295640),
        (25732575),
        (25733074),
        (25735109),
        (25735685),
        (25736328),
        (25737946),
        (25738384),
        (25739305),
        (25740180),
        (25740809),
        (25742830),
        (25743141),
        (25743280),
        (25743374),
        (25742369),
        (25745244),
        (25745365),
        (25745836),
        (25747059),
        (25747079),
        (25747425),
        (25748647),
        (25748649),
        (25749552),
        (25392477),
        (25751765),
        (25752669),
        (25753075),
        (25754308),
        (25754682),
        (25754744),
        (25754827),
        (25755108),
        (25756850),
        (25757114),
        (25757522),
        (25763709),
        (25765030),
        (25765545),
        (25765698),
        (25511818),
        (25767049),
        (25767613),
        (25767940),
        (25768041),
        (25770257),
        (25770982),
        (25771645),
        (25773607),
        (25774343),
        (25778003),
        (25779834),
        (25780116),
        (25780490)
),

expt_population as (
    select 
        'V0: Control' as variation,
        account_id
    from control_
    union all
    select 
        'V1: Variant' as variation,
        account_id
    from variant_
),

main as (
    select 
        trial_accounts_.instance_account_id,
        expt_population_.variation,
        trial_accounts_.last_snapshot_date,
        trial_accounts_.win_date,
        trial_accounts_.instance_account_arr_usd_at_win arr_at_win,
        trial_accounts_.instance_account_arr_usd_at_mo1 arr_at_mo1,
        trial_accounts_.instance_account_arr_usd_at_mo2 arr_at_mo2,
        trial_accounts_.instance_account_arr_usd_at_mo3 arr_at_mo3,
        trial_accounts_.instance_account_arr_usd_at_last_snapshot arr_at_latest,
        --- Flags to indicate data availability at each timepoint
        case 
            when trial_accounts_.mo1_date <= trial_accounts_.last_snapshot_date then 1 else 0
        end as has_data_mo1,
        case 
            when trial_accounts_.mo2_date <= trial_accounts_.last_snapshot_date then 1 else 0
        end as has_data_mo2,
        case 
            when trial_accounts_.mo3_date <= trial_accounts_.last_snapshot_date then 1 else 0
        end as has_data_mo3,
        case 
            when has_data_mo1 = 1 then trial_accounts_.instance_account_arr_usd_at_win else 0 
        end as has_data_arr_mo1,
        case 
            when has_data_mo2 = 1 then trial_accounts_.instance_account_arr_usd_at_win else 0 
        end as has_data_arr_mo2,
        case 
            when has_data_mo3 = 1 then trial_accounts_.instance_account_arr_usd_at_win else 0 
        end as has_data_arr_mo3,
        --- Expansion & churn metrics
        --- Expansion flags
        case 
            when 
                trial_accounts_.mo1_date <= trial_accounts_.last_snapshot_date
                and trial_accounts_.instance_account_arr_usd_at_mo1 > trial_accounts_.instance_account_arr_usd_at_win
            then 1 else 0
        end as is_expanded_mo1,
        case 
            when 
                trial_accounts_.mo2_date <= trial_accounts_.last_snapshot_date
                and trial_accounts_.instance_account_arr_usd_at_mo2 > trial_accounts_.instance_account_arr_usd_at_win
            then 1 else 0
        end as is_expanded_mo2,
        case 
            when 
                trial_accounts_.mo3_date <= trial_accounts_.last_snapshot_date
                and trial_accounts_.instance_account_arr_usd_at_mo3 > trial_accounts_.instance_account_arr_usd_at_win
            then 1 else 0
        end as is_expanded_mo3,
        case 
            when 
                trial_accounts_.instance_account_arr_usd_at_last_snapshot > trial_accounts_.instance_account_arr_usd_at_win
            then 1 else 0
        end as is_expanded_latest,
        --- Expansion amounts
        case 
            when is_expanded_mo1 = 1 
            then trial_accounts_.instance_account_arr_usd_at_mo1 - trial_accounts_.instance_account_arr_usd_at_win 
            else 0 
        end as expansion_amount_mo1,
        case 
            when is_expanded_mo2 = 1 
            then trial_accounts_.instance_account_arr_usd_at_mo2 - trial_accounts_.instance_account_arr_usd_at_win 
            else 0 
        end as expansion_amount_mo2,
        case 
            when is_expanded_mo3 = 1 
            then trial_accounts_.instance_account_arr_usd_at_mo3 - trial_accounts_.instance_account_arr_usd_at_win 
            else 0 
        end as expansion_amount_mo3,
        case 
            when is_expanded_latest = 1 
            then trial_accounts_.instance_account_arr_usd_at_last_snapshot - trial_accounts_.instance_account_arr_usd_at_win 
            else 0 
        end as expansion_amount_latest,
        --- Churn flags
        case 
            when 
                trial_accounts_.mo1_date <= trial_accounts_.last_snapshot_date
                and (trial_accounts_.instance_account_arr_usd_at_mo1 = 0 or trial_accounts_.instance_account_arr_usd_at_mo1 is null)
            then 1 else 0
        end as is_churned_mo1,
        case 
            when 
                (trial_accounts_.mo2_date <= trial_accounts_.last_snapshot_date
                and (trial_accounts_.instance_account_arr_usd_at_mo2 = 0 or trial_accounts_.instance_account_arr_usd_at_mo2 is null))
                or is_churned_mo1 = 1
            then 1 else 0
        end as is_churned_mo2,
        case 
            when 
                (trial_accounts_.mo3_date <= trial_accounts_.last_snapshot_date
                and (trial_accounts_.instance_account_arr_usd_at_mo3 = 0 or trial_accounts_.instance_account_arr_usd_at_mo3 is null))
                or is_churned_mo2 = 1
            then 1 else 0
        end as is_churned_mo3,
        case 
            when 
                trial_accounts_.instance_account_arr_usd_at_last_snapshot = 0 or trial_accounts_.instance_account_arr_usd_at_last_snapshot is null
            then 1 else 0
        end as is_churned_latest,
        --- Churn amounts
        case 
            when is_churned_mo1 = 1
            then trial_accounts_.instance_account_arr_usd_at_win
            else 0
        end as churn_amount_mo1,
        case 
            when is_churned_mo2 = 1
            then trial_accounts_.instance_account_arr_usd_at_win
            else 0
        end as churn_amount_mo2,
        case 
            when is_churned_mo3 = 1
            then trial_accounts_.instance_account_arr_usd_at_win    
            else 0
        end as churn_amount_mo3,
        case 
            when is_churned_latest = 1
            then trial_accounts_.instance_account_arr_usd_at_win
            else 0
        end as churn_amount_latest
    from presentation.growth_analytics.trial_accounts trial_accounts_
    inner join expt_population expt_population_
        on trial_accounts_.instance_account_id = expt_population_.account_id
)


select *, convert_timezone('UTC', 'America/Los_Angeles', current_timestamp) as updated_at
from main





