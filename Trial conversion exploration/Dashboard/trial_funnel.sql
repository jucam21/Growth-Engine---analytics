------------------------------------
--- Query that will populate the trial recommendation dashboard
--- Requirements: https://docs.google.com/document/d/1_mvJ3R6S7e3O5Hy7U0OUaBG0QfSiHeE3S3z-pXecHb4/edit?tab=t.0#heading=h.yoq1yjfhflrr


----------------------------------------------------
--- 1.0: Uncohorted funnel - clicks


--- Step 0: filtering by trial accounts
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        trial_accounts.crm_account_id,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
),
--- Step 1: count interactions with each modal step
prompt_load as (
    select
        prompt_click.account_id,
        prompt_click.trial_type,
        prompt_click.account_id as unique_count,
        date_trunc('day', prompt_click.original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_scd2 prompt_click
    inner join accounts a
        on prompt_click.account_id = a.instance_account_id
    group by all
),

modal_load as (
    select
        account_id,
        a.crm_account_id,
        offer_id,
        plan_name,
        preview_state,
        source,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
    inner join accounts a
        on load.account_id = a.instance_account_id
    group by all
),

modal_dismiss as (
    select
        account_id,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_dismiss_offer_scd2
    group by all
),

modal_buy_now as (
    select
        account_id,
        agent_count,
        billing_cycle,
        offer_id,
        plan,
        plan_name,
        product,
        promo_code,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2
    group by all
),

modal_agent_increase as (
    select
        account_id,
        agent_count,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_agent_increase_scd2
    group by all
),

modal_agent_decrease as (
    select
        account_id,
        agent_count,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_agent_decrease_scd2
    group by all
),

modal_billing_cycle as (
    select
        account_id,
        billing_cycle,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_billing_cycle_change_scd2
    group by all
),

modal_see_all_plans as (
    select
        account_id,
        offer_id,
        plan_name,
        account_id as unique_count,
        date_trunc('day', original_timestamp) as date,
        count(*) as total_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
    group by all
),

--- Step 2: Join relevant events
--- Decided to not use agent increase/decrease & billing cycle change events,
--- since they have duplicates and require additional logic

segment_events_all_tmp as (
    select
        modal_load.date as loaded_date,
        modal_load.account_id,
        modal_load.crm_account_id,
        case when modal_load.offer_id is null then 'No Offer' else modal_load.offer_id end as offer_id,
        modal_load.plan_name,
        modal_load.preview_state,
        modal_load.source,
        --- Fields relevant to buy now modal
        modal_buy_now.agent_count as buy_now_agent_count,
        modal_buy_now.billing_cycle as buy_now_billing_cycle,
        modal_buy_now.offer_id as buy_now_offer_id,
        modal_buy_now.plan as buy_now_plan,
        modal_buy_now.plan_name as buy_now_plan_name,
        modal_buy_now.product as buy_now_product,
        modal_buy_now.promo_code as buy_now_promo_code,
        --- Counts per each modal
        prompt_load.total_count as total_count_prompt_load,
        prompt_load.unique_count as unique_count_prompt_load,
        modal_load.total_count as total_count_modal_loads,
        modal_load.unique_count as unique_count_modal_loads,
        case when modal_load.source = 'CTA' then total_count_modal_loads end as total_count_modal_loads_cta,
        case when modal_load.source = 'auto_trigger' then total_count_modal_loads end as total_count_modal_loads_auto_trigger,
        case when modal_load.source = 'CTA' then unique_count_modal_loads end as unique_count_modal_loads_cta,
        case when modal_load.source = 'auto_trigger' then unique_count_modal_loads end as unique_count_modal_loads_auto_trigger,
        modal_dismiss.total_count as total_count_modal_dismiss,
        modal_dismiss.unique_count as unique_count_modal_dismiss,
        modal_buy_now.total_count as total_count_modal_buy_now,
        modal_buy_now.unique_count as unique_count_modal_buy_now,
        modal_see_all_plans.total_count as total_count_modal_see_all_plans,
        modal_see_all_plans.unique_count as unique_count_modal_see_all_plans
    from modal_load 
    left join modal_dismiss
        on modal_load.account_id = modal_dismiss.account_id
        and modal_load.date = modal_dismiss.date
    left join modal_buy_now
        on modal_load.account_id = modal_buy_now.account_id
        and modal_load.date = modal_buy_now.date
    left join prompt_load
        on modal_load.account_id = prompt_load.account_id
        and modal_load.date = prompt_load.date
    left join modal_see_all_plans
        on modal_load.account_id = modal_see_all_plans.account_id
        and modal_load.date = modal_see_all_plans.date
),

--- Step 3: Join win date data

sub_term as (
    select distinct
        finance.service_date,
        snapshot.instance_account_id,
        finance.subscription_term_start_date,
        finance.subscription_term_end_date
    from foundational.finance.fact_recurring_revenue_daily_snapshot_enriched as finance
    inner join foundational.customer.entity_mapping_daily_snapshot as snapshot
        on finance.billing_account_id = snapshot.billing_account_id
        and finance.service_date = snapshot.source_snapshot_date
    where finance.service_date >= '2025-06-01'
),

--- Including wins here but they will be cohorted by cart load.
--- The correct # of uncohorted wins will be measured in a different query.

wins as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.win_date,
        trial_accounts.core_base_plan_at_win,
        datediff(day, sub_term_.subscription_term_start_date::date, sub_term_.subscription_term_end_date::date) as subscription_term_days,
        case 
            when subscription_term_days >= 0 and subscription_term_days <= 40 then 'monthly'
            when subscription_term_days >= 360 and subscription_term_days <= 370 then 'annually'
            else 'other'
        end as billing_cycle,
        trial_accounts.instance_account_arr_usd_at_win
    from presentation.growth_analytics.trial_accounts trial_accounts 
    left join sub_term sub_term_
        on trial_accounts.instance_account_id = sub_term_.instance_account_id
        and trial_accounts.win_date = sub_term_.service_date
    where 
        trial_accounts.win_date is not null
        --and trial_accounts.sales_model_at_win <> 'Assisted'
        --and trial_accounts.is_direct_buy = FALSE  
        and trial_accounts.win_date >= '2025-06-01'
),

segment_events_all as (
    select
        segment.*,
        crms.crm_account_name,
        crms.pro_forma_market_segment,
        wins.win_date,
        wins.instance_account_arr_usd_at_win,
        datediff(day, segment.loaded_date::date, wins.win_date::date) as days_to_win,
        case when wins.win_date is not null then 1 else null end as is_won_all,
        case when wins.win_date is not null then segment.account_id else null end as is_won_unique,
        case 
            when 
                wins.win_date is not null 
                and unique_count_modal_buy_now is not null 
                and unique_count_modal_see_all_plans is null 
                then segment.account_id else null 
        end as wins_just_buy_now,
        case 
            when 
                wins.win_date is not null 
                and unique_count_modal_buy_now is null 
                and unique_count_modal_see_all_plans is not null 
                then segment.account_id else null 
        end as wins_just_see_all_plans,
        case 
            when 
                wins.win_date is not null 
                and unique_count_modal_buy_now is not null 
                and unique_count_modal_see_all_plans is not null 
                then segment.account_id else null 
        end as wins_both,
        case 
            when 
                wins.win_date is not null 
                and unique_count_modal_buy_now is null 
                and unique_count_modal_see_all_plans is null 
                then segment.account_id else null 
        end as wins_none,
        wins.core_base_plan_at_win,
        wins.subscription_term_days,
        wins.billing_cycle
    from segment_events_all_tmp segment
    left join wins
        on segment.account_id = wins.instance_account_id
        and date(segment.loaded_date) = date(wins.win_date)
    left join foundational.customer.dim_crm_accounts_daily_snapshot crms
        on 
            segment.crm_account_id = crms.crm_account_id
            and date(segment.loaded_date) = crms.source_snapshot_date

)

select 
    count(distinct account_id) as total_accounts,
    sum(is_won_all) as total_wins,
    count(distinct is_won_unique) as unique_wins,
    count(distinct wins_none) as wins_none,
    count(distinct case when wins_just_buy_now is not null or wins_just_see_all_plans is not null then account_id else null end) as wins_with_cta
from segment_events_all









select *
from segment_events_all
where account_id in (25724028,
25551618,
25558494,
25565272,
25646392,
25753530,
25587636,
25550799,
25521005,
25627718,
25624623,
25572456,
25559829,
25631450,
25637281,
25621376,
25524384,
25579209,
25563635,
25438424,
25622218,
25493646,
25605462,
25697205,
25565781,
25158681,
25540218,
25570135,
25658218,
25526586,
25509266,
25539202,
25642123,
25590342,
25755293,
25689772,
25677354,
25524177,
25712230,
25572605,
25771645,
25497785,
25595728,
25443633,
25649127,
25730996,
25539756,
25537713,
25745244,
25596939,
25747079,
25548036,
25713313,
25570878,
25590010,
25598999,
25485948,
25751765,
25653940,
25577364,
25488922,
25501076,
25561181,
25402279,
25742570,
25640676,
25548654,
25089868,
25552150,
25658277,
25042437,
25585240,
25545358,
25640317,
25668308,
25544716,
25547038,
25598090,
25555715,
25768604,
25534337,
25706061,
25435870,
25790822,
25757522,
25491454,
25569449,
25683841,
25585351,
25747425,
25539916,
25619025,
25594393,
25565237,
25645492,
25522890,
25510972,
25567736,
25713369,
25658138,
25626709,
25546622,
25724261,
25629639,
25559583,
25547037,
25630154,
25732575,
25547995,
25519330,
25724626,
25613617,
25646121,
25506219,
25600147,
25732619,
25750328,
25538297,
25559924,
25730068,
25622360,
25765187,
25647554,
25752153,
25757470,
25565886,
25537024,
25546471,
25603014,
25712733,
25484750,
25527365,
25767893,
25763285,
25613888,
25590126,
25743374,
25603610,
25551619,
25511624,
25566975,
25706176,
25677023,
25783280,
25646432,
25760450,
25714925,
25439892,
25566847,
25745836,
25552491,
25705165,
25487420,
25548440,
25740809,
25545848,
25773377,
25520977,
25515135,
25560533,
25753075,
25739305,
25746619,
25763290,
25551369,
25525781,
25570844,
25526267,
25555505,
25748649,
25501837,
25692009,
25573352,
25547521,
25094736,
25602954,
25755129,
25715527,
25605328,
25541266,
25544063,
25632440,
25598514,
25582803,
25639745,
25785666,
25511005,
25661804,
25790594,
25645174,
25532600,
25567844,
25736328,
25704279,
25679330,
25589837,
25656931,
25480613,
25563745,
25584588,
25711347,
25660760,
25700970,
25585766,
25677701,
25150458,
25648488,
25437408,
25599447,
25621939,
25595753,
25508661,
25588622,
25619183,
25598881,
25208603,
25517942,
25721069,
24295640,
25612801,
25467286,
25638074,
25535148,
25747059,
25526023,
25761581,
25611971,
25484577,
25387942,
25575284,
25655024,
25767212,
25537709,
25618300,
25656158,
25630349,
25613404,
25661185,
25765983,
25522260,
25613170,
25592175,
25735109,
25645941,
25653473,
25506535,
25598352,
25558014,
25603124,
25703788,
25568297,
25673559,
25678417,
25597447,
25565662,
25630537,
25543902,
25551194,
25534484,
25588851,
25768041,
25680945,
25683309,
25524073,
25512024,
25441648,
25632909,
25560287,
25520124,
25519284,
25593317,
25743280,
25770820,
25625208,
25714279,
25554602,
24646878,
25212203
)





select 
    count(distinct account_id) as total_accounts,
    sum(is_won_all) as total_wins,
    count(distinct is_won_unique) as unique_wins,
    count(distinct wins_none) as wins_none
from segment_events_all





select 
    count(distinct load.account_id) as total_accounts,
    min(load.original_timestamp) as first_load,
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
inner join presentation.growth_analytics.trial_accounts a
        on load.account_id = a.instance_account_id;




select *, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) as updated_at
from segment_events_all;




------------------------------------------------
--- 1.1 SMB list of accounts

--- Define columns

select distinct
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) as updated_at,
    loaded_timestamp,
    date(loaded_date) loaded_day,
    crm_account_id,
    crm_account_name,
    account_id as instance_account_id,
    pro_forma_market_segment, 
    offer_id, 
    plan_name, 
    preview_state, 
    source,
    BUY_NOW_AGENT_COUNT, BUY_NOW_BILLING_CYCLE, BUY_NOW_OFFER_ID, BUY_NOW_PLAN, BUY_NOW_PLAN_NAME, BUY_NOW_PRODUCT, BUY_NOW_PROMO_CODE, 
    UNIQUE_COUNT_PROMPT_LOAD as is_cta_loaded,
    UNIQUE_COUNT_MODAL_LOADS as is_modal_loaded,
    UNIQUE_COUNT_MODAL_LOADS_CTA as is_modal_loaded_cta,
    UNIQUE_COUNT_MODAL_LOADS_AUTO_TRIGGER as is_modal_loaded_auto_trigger,
    UNIQUE_COUNT_MODAL_DISMISS as is_modal_dismissed,
    UNIQUE_COUNT_MODAL_BUY_NOW as is_modal_buy_now_clicked,
    UNIQUE_COUNT_MODAL_SEE_ALL_PLANS as is_modal_see_all_plans_clicked,
    win_date,
    INSTANCE_ACCOUNT_ARR_USD_AT_WIN,
from segment_events_all
where pro_forma_market_segment = 'SMB';




select * --sales_model_at_win, is_direct_buy
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25588201



select distinct pro_forma_market_segment
from foundational.customer.dim_crm_accounts_daily_snapshot
where source_snapshot_date = '2025-08-01'



select *
from foundational.customer.dim_crm_accounts_daily_snapshot
where 
    source_snapshot_date >= '2025-08-01'



select *
from foundational.customer.dim_instance_accounts_daily_snapshot
where 
    source_snapshot_date >= '2025-08-01'
    and instance_account_id = 25611419



select *
from foundational.customer.entity_mapping_daily_snapshot 
where 
    source_snapshot_date >= '2025-07-01'
    and instance_account_id = 25611419




with wins as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.win_date,
        trial_accounts.core_base_plan_at_win,
        trial_accounts.instance_account_arr_usd_at_win
    from presentation.growth_analytics.trial_accounts trial_accounts 
    where 
        trial_accounts.win_date is not null
        and trial_accounts.sales_model_at_win <> 'Assisted'
        and trial_accounts.is_direct_buy = FALSE  
        and trial_accounts.win_date >= '2025-06-01'
)

select *
from wins
where instance_account_id = 25588201




--- Validating results
select 
    loaded_date,
    --- Total counts
    sum(total_count_prompt_load) as total_count_prompt_load,
    sum(total_count_modal_loads) as total_count_modal_loads,
    sum(total_count_modal_loads_cta) as total_count_modal_loads_cta,
    sum(total_count_modal_loads_auto_trigger) as total_count_modal_loads_auto_trigger,

    --- Unique counts
    count(distinct unique_count_prompt_load) as unique_count_prompt_load,
    count(distinct unique_count_modal_loads) as unique_count_modal_loads,
    count(distinct unique_count_modal_loads_cta) as unique_count_modal_loads_cta,
    count(distinct unique_count_modal_loads_auto_trigger) as unique_count_modal_loads_auto_trigger
from segment_events_all
group by loaded_date
order by loaded_date 


select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where 
    source = 'auto_trigger'
    and date(original_timestamp) = '2025-08-03'
order by original_timestamp desc
limit 20


select 
    count(*) as total_count,
    count(distinct account_id) as unique_count,
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 a
inner join presentation.growth_analytics.trial_accounts trial_accounts 
    on a.account_id = trial_accounts.instance_account_id
where 
    source = 'auto_trigger'
    and date(original_timestamp) = '2025-08-03'


--- Users without prompt load, no auto-trigger 

select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 a
inner join presentation.growth_analytics.trial_accounts trial_accounts 
    on a.account_id = trial_accounts.instance_account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_scd2 prompt_load
    on a.account_id = prompt_load.account_id
    and date(a.original_timestamp) = date(prompt_load.original_timestamp)
where 
    date(a.original_timestamp) = '2025-07-25'
    and prompt_load.account_id is null


--- Examples of those accounts. Probably are testing created by zendesk employees
--- Amy's account: https://monitor.zende.sk/accounts/25489303/overview
select *
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where account_id in (25550478, 25489303)


select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where account_id in (25550478, 25489303)








----------------------------------------------------
--- 1.1: Uncohorted funnel - wins


--- Adjusted wins to measure if a user ever clicked on "buy now" or "see all plans" modals
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
),

--- User ever clicked on "buy now" or "see all plans" modals in the past
modal_buy_now as (
    select
        account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 buy_now
    inner join accounts 
        on 
            accounts.instance_account_id = buy_now.account_id
            and date(buy_now.original_timestamp) <= accounts.win_date
    group by all
),

modal_see_all_plans as (
    select
        account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2 see_all_plans
    inner join accounts 
        on 
            accounts.instance_account_id = see_all_plans.account_id
            and date(see_all_plans.original_timestamp) <= accounts.win_date
    group by all
),

--- Join wins vs segment funnel
wins_daily as (
    select
        accounts_.win_date,
        count(*) as total_wins,
        count(distinct accounts_.instance_account_id) as unique_wins,
        sum(is_won_arr) as total_wins_arr,
        --- Wins count
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is null 
                then accounts_.instance_account_id else null 
        end) as wins_just_buy_now,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is not null 
                then accounts_.instance_account_id else null 
        end) as wins_just_see_all_plans,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is not null 
                then accounts_.instance_account_id else null 
        end) as wins_both,
        count(distinct case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is null 
                then accounts_.instance_account_id else null 
        end) as wins_none,
        --- Wins arr
        sum(case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is null 
                then accounts_.is_won_arr else null 
        end) as wins_just_buy_now_arr,
        sum(case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is not null 
                then accounts_.is_won_arr else null 
        end) as wins_just_see_all_plans_arr,
        sum(case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is not null 
                then accounts_.is_won_arr else null 
        end) as wins_both_arr,
        sum(case 
            when 
                accounts_.win_date is not null 
                and modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is null 
                then accounts_.is_won_arr else null 
        end) as wins_none_arr,
    from accounts accounts_
    left join modal_buy_now modal_buy_now_ 
        on accounts_.instance_account_id = modal_buy_now_.account_id
    left join modal_see_all_plans modal_see_all_plans_ 
        on accounts_.instance_account_id = modal_see_all_plans_.account_id
    where accounts_.is_won = 1
    group by 1
),


--- Check admin center loads & billing cart loaded event


admin_center as (
    select
        account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_billing.segment_billing_subscription_viewed_bcv admin_center
    inner join accounts 
        on 
            accounts.instance_account_id = admin_center.account_id
            -- Admin center logins last 15 days before win date
            and date(admin_center.original_timestamp) <= accounts.win_date
            and date(admin_center.original_timestamp) >= dateadd('day', -15, accounts.win_date)
    group by all
),

billing_cart_loaded as (
    select
        account_id,
        cart_screen,
        cart_step,
        cart_version,
        origin,
        cart_type,
        date(original_timestamp) as max_date
    from 
        cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing_cart_loaded
    inner join accounts 
        on 
            accounts.instance_account_id = billing_cart_loaded.account_id
            -- Admin center logins last 15 days before win date
            and date(billing_cart_loaded.original_timestamp) <= accounts.win_date
            and date(billing_cart_loaded.original_timestamp) >= dateadd('day', -15, accounts.win_date)
    where paid_customer = FALSE
    qualify row_number() over (partition by account_id order by original_timestamp desc) = 1
)

select 
    accounts_.*,
    case 
        when modal_buy_now_.account_id is not null or modal_see_all_plans_.account_id is not null 
        then 'buy_now/see_all_plans_clicked' else 'no_cta_clicked'
    end as modal_clicked,
    modal_buy_now_.max_date as max_date_modal_buy_now,
    modal_see_all_plans_.max_date as max_date_modal_see_all_plans,
    admin_center_.max_date as max_date_admin_center,
    billing_cart_loaded_.* exclude (account_id),
        case when billing_cart_loaded_.account_id is null then 1 else 0 end as cart_null
from accounts accounts_
left join modal_buy_now modal_buy_now_ 
    on accounts_.instance_account_id = modal_buy_now_.account_id
left join modal_see_all_plans modal_see_all_plans_ 
    on accounts_.instance_account_id = modal_see_all_plans_.account_id
left join admin_center admin_center_ 
    on accounts_.instance_account_id = admin_center_.account_id
left join billing_cart_loaded billing_cart_loaded_ 
    on accounts_.instance_account_id = billing_cart_loaded_.account_id
where 
    accounts_.is_won = 1
    and win_date >= '2025-05-01'
order by win_date




---- Sharing query with EDA

--- All trial accounts
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_arr
    from presentation.growth_analytics.trial_accounts trial_accounts 
),

--- User ever clicked on "buy now" or "see all plans" modals in the past
modal_buy_now as (
    select
        account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 buy_now
    inner join accounts 
        on 
            accounts.instance_account_id = buy_now.account_id
            and date(buy_now.original_timestamp) <= accounts.win_date
    group by all
),

modal_see_all_plans as (
    select
        account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2 see_all_plans
    inner join accounts 
        on 
            accounts.instance_account_id = see_all_plans.account_id
            and date(see_all_plans.original_timestamp) <= accounts.win_date
    group by all
),

--- User fired billing cart loaded in the last 15 days
billing_cart_loaded as (
    select
        account_id,
        cart_screen,
        cart_step,
        cart_version,
        origin,
        cart_type,
        date(original_timestamp) as max_date
    from 
        cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing_cart_loaded
    inner join accounts 
        on 
            accounts.instance_account_id = billing_cart_loaded.account_id
            -- 15 day timeframe last 15 days before win date
            and date(billing_cart_loaded.original_timestamp) <= accounts.win_date
            and date(billing_cart_loaded.original_timestamp) >= dateadd('day', -15, accounts.win_date)
    where paid_customer = FALSE
    qualify row_number() over (partition by account_id order by original_timestamp desc) = 1
)

select 
    accounts_.*,
    case 
        when modal_buy_now_.account_id is not null or modal_see_all_plans_.account_id is not null 
        then 'buy_now/see_all_plans_clicked' else 'no_cta_clicked'
    end as modal_clicked,
    modal_buy_now_.max_date as max_date_modal_buy_now,
    modal_see_all_plans_.max_date as max_date_modal_see_all_plans,
    billing_cart_loaded_.* exclude (account_id),
        case when billing_cart_loaded_.account_id is null then 1 else 0 end as cart_null
from accounts accounts_
left join modal_buy_now modal_buy_now_ 
    on accounts_.instance_account_id = modal_buy_now_.account_id
left join modal_see_all_plans modal_see_all_plans_ 
    on accounts_.instance_account_id = modal_see_all_plans_.account_id
left join billing_cart_loaded billing_cart_loaded_ 
    on accounts_.instance_account_id = billing_cart_loaded_.account_id
where 
    accounts_.is_won = 1
    --- Selecting only wins from Jul29
    and win_date = '2025-07-29'
order by win_date





select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 a
where account_id = 24079522




--- Checking online dashboard query

--- Numbers match.
--- Have to determine opportunity id to account id mapping
select 
    close_year,
    year_month,
    count(*) as total_count,
    sum(total_booking_arr) as total_booking_arr
from presentation.growth_analytics.growth_analytics_online_dashboard_curated_bookings
where 
    close_date >= '2025-04-01'
    and sales_motion = 'Online'
group by all
order by 1,2







--- Checking account id to CRM mapping from modal loads
--- Using onlywins, since for not all accounts will be found in the table

with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.win_date,
        trial_accounts.instance_account_arr_usd_at_win
    from presentation.growth_analytics.trial_accounts trial_accounts 
    where 
        trial_accounts.win_date is not null 
        and trial_accounts.sales_model_at_win <> 'Assisted'
        and trial_accounts.is_direct_buy = FALSE
)


select 
    accounts_.instance_account_id,
    mapping.crm_account_id,
    accounts_.win_date,
    accounts_.instance_account_arr_usd_at_win
from accounts accounts_
left join foundational.customer.entity_mapping_daily_snapshot as mapping
    on accounts_.instance_account_id = mapping.instance_account_id
    and accounts_.win_date = mapping.source_snapshot_date
where 
    mapping.crm_account_id = '0016R000036b0nmQAA'
    and accounts_.win_date >= '2025-05-01'



select 
    mapping.crm_account_id,
    count(*) as total_count,
    count(distinct accounts_.instance_account_id) as unique_crm_accounts
from accounts accounts_
left join foundational.customer.entity_mapping_daily_snapshot as mapping
    on accounts_.instance_account_id = mapping.instance_account_id
    and accounts_.win_date = mapping.source_snapshot_date
where accounts_.win_date >= '2025-05-01'
group by 1
order by 3 desc
limit 10


select
    count(*) as total_count,
    count(distinct accounts_.instance_account_id) as unique_accounts,
    count(distinct mapping.crm_account_id) as unique_crm_accounts,
    count(case when mapping.crm_account_id is null then 1 else null end) as null_crms
from accounts accounts_
left join foundational.customer.entity_mapping_daily_snapshot as mapping
    on accounts_.instance_account_id = mapping.instance_account_id
    and accounts_.win_date = mapping.source_snapshot_date
where accounts_.win_date >= '2025-05-01'










select 
    win_date,
    count(*) as total_count,
    sum(instance_account_arr_usd_at_win) as total_arr
from accounts
where win_date >= '2025-07-01'
group by all
order by win_date






select *
from foundational.customer.entity_mapping_daily_snapshot --._bcv
where instance_account_id = 25500148
limit 10





select *
from foundational.customer.entity_mapping_daily_snapshot --._bcv
where instance_account_id = 25500148
limit 10






select *
from functional.finance.sfa_crm_bookings_current
where 
    crm_account_id = '0016R000036b0nmQAA'
    --and pro_forma_signature_date >= '2025-05-01'
order by pro_forma_signature_date desc







select 
    date_trunc('month', pro_forma_signature_date) pro_forma_signature_date,
    count(*) total_obs,
    count(distinct crm_account_id) crm_account_id,
    sum(total_booking_arr_usd) total_booking_arr_usd
from functional.finance.sfa_crm_bookings_current
where
    pro_forma_market_segment_at_close_date = 'Digital'
    and sales_motion = 'Online'
    and type = 'New Business'
    and pro_forma_signature_date >= '2025-03-01'
group by 1
order by 1







select 
    crm_account_id,
    count(*) total_obs,
    count(distinct crm_account_id) crm_account_id,
    sum(total_booking_arr_usd) total_booking_arr_usd
from functional.finance.sfa_crm_bookings_current
where
    pro_forma_market_segment_at_close_date = 'Digital'
    and sales_motion = 'Online'
    and type = 'New Business'
    and pro_forma_signature_date >= '2025-03-01'
group by 1
order by 2 desc
limit 10



select *
from functional.finance.sfa_crm_bookings_current
where
    pro_forma_market_segment_at_close_date = 'Digital'
    and crm_account_id = '001PC00000RsL9aYAF'





--- Checking bookings vs instance accounts

with bookings as (
    select 
        date_trunc('month', pro_forma_signature_date) month_closed,
        crm_account_id,
        sum(total_booking_arr_usd) as total_booking_arr_usd
    from functional.finance.sfa_crm_bookings_current
    where
        pro_forma_market_segment_at_close_date = 'Digital'
        and sales_motion = 'Online'
        and type = 'New Business'
        and pro_forma_signature_date >= '2025-05-01'
    group by 1,2
),

mapping as (
    select
        bookings_.*,
        mapping.instance_account_id
    from bookings bookings_
    left join foundational.customer.entity_mapping_daily_snapshot mapping
        on bookings_.crm_account_id = mapping.crm_account_id
        and bookings_.month_closed = mapping.source_snapshot_date
)


select 
    month_closed,
    crm_account_id,
    count(*) as total_obs,
    count(distinct instance_account_id) as instance_account_id
from mapping
group by 1, 2
order by 3 desc
limit 10




select *
from bookings
where crm_account_id = '0018000000vWQTcAAO'




select *
from mapping
where crm_account_id = '0018000000vWQTcAAO'








----------------------------------------------------
--- 1.1.1: Uncohorted funnel - bookings


--- Adjusting query to use bookings

--- All trial accounts

with bookings as (
    select 
        pro_forma_signature_date,
        crm_account_id,
        crm_opportunity_id,
        total_booking_arr_usd
        --sum(total_booking_arr_usd) as total_booking_arr_usd
    from functional.finance.sfa_crm_bookings_current
    where
        pro_forma_market_segment_at_close_date = 'Digital'
        and sales_motion = 'Online'
        and type = 'New Business'
        --and pro_forma_signature_date >= '2025-05-01'
    --group by 1,2
),

--- User ever clicked on "buy now" or "see all plans" modals in the past
modal_buy_now as (
    select
        mapping.crm_account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 buy_now
    --- Remove testing accounts
    inner join presentation.growth_analytics.trial_accounts trial_accounts  
        on 
            trial_accounts.instance_account_id = buy_now.account_id
            and date(buy_now.original_timestamp) <= trial_accounts.win_date
    --- Join CRM
    left join foundational.customer.entity_mapping_daily_snapshot mapping
        on 
            buy_now.account_id = mapping.instance_account_id
            and date(buy_now.original_timestamp) = mapping.source_snapshot_date
    group by all
),

modal_see_all_plans as (
    select
        mapping.crm_account_id,
        max(date(original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2 see_all_plans
    --- Remove testing accounts
    inner join presentation.growth_analytics.trial_accounts trial_accounts
        on
            trial_accounts.instance_account_id = see_all_plans.account_id
            and date(see_all_plans.original_timestamp) <= trial_accounts.win_date
    --- Join CRM
    left join foundational.customer.entity_mapping_daily_snapshot mapping
        on 
            see_all_plans.account_id = mapping.instance_account_id
            and date(see_all_plans.original_timestamp) = mapping.source_snapshot_date
    group by all
),

--- Last billing cart loaded event
billing_cart_loaded as (
    select
        mapping.crm_account_id,
        cart_screen,
        cart_step,
        cart_version,
        origin,
        cart_type,
        date(original_timestamp) as max_date
    from 
        cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing_cart_loaded
    --- Remove testing accounts
    inner join presentation.growth_analytics.trial_accounts trial_accounts  
        on 
            trial_accounts.instance_account_id = billing_cart_loaded.account_id
            and date(billing_cart_loaded.original_timestamp) <= trial_accounts.win_date
            and date(billing_cart_loaded.original_timestamp) >= dateadd('day', -15, trial_accounts.win_date)
    --- CRM
    left join foundational.customer.entity_mapping_daily_snapshot mapping
        on 
            billing_cart_loaded.account_id = mapping.instance_account_id
            and date(billing_cart_loaded.original_timestamp) = mapping.source_snapshot_date
    where paid_customer = FALSE
    qualify row_number() over (partition by mapping.crm_account_id order by billing_cart_loaded.original_timestamp desc) = 1
),

joined as (
    select
        bookings_.crm_account_id,
        bookings_.pro_forma_signature_date,
        bookings_.total_booking_arr_usd,
        --- Cases for modal interactions
        case 
            when modal_buy_now_.crm_account_id is not null and modal_see_all_plans_.crm_account_id is null 
            then 'buy_now_clicked'
            when modal_buy_now_.crm_account_id is null and modal_see_all_plans_.crm_account_id is not null 
            then 'see_all_plans_clicked'
            when modal_buy_now_.crm_account_id is not null and modal_see_all_plans_.crm_account_id is not null 
            then 'both_clicked'
            when modal_buy_now_.crm_account_id is null and modal_see_all_plans_.crm_account_id is null 
            then 'no_cta_clicked'
            else 'unknown'
        end as modal_clicked,
        billing_cart_loaded_.cart_screen,
        billing_cart_loaded_.cart_step,
        billing_cart_loaded_.cart_version,
        billing_cart_loaded_.origin,
        billing_cart_loaded_.cart_type,
        billing_cart_loaded_.max_date as max_date_billing_cart_loaded

    from bookings bookings_
    left join modal_buy_now modal_buy_now_
        on bookings_.crm_account_id = modal_buy_now_.crm_account_id
    left join modal_see_all_plans modal_see_all_plans_
        on bookings_.crm_account_id = modal_see_all_plans_.crm_account_id
    left join billing_cart_loaded billing_cart_loaded_
        on bookings_.crm_account_id = billing_cart_loaded_.crm_account_id
    where
        bookings_.pro_forma_signature_date >= '2025-05-01'
)

select *
from joined
where 
    origin is null
limit 10


-- Bookings no cart event
/* 
001PC00000TVUfRYAX
001PC00000RmnZzYAJ
001PC00000TAXn7YAH
001PC00000RXpImYAL
001PC00000RxujRYAR
*/


--- Search for bookings with no cart event

select *
from foundational.customer.entity_mapping_daily_snapshot_bcv
where crm_account_id in (
    '001PC00000TVUfRYAX',
    '001PC00000RmnZzYAJ',
    '001PC00000TAXn7YAH',
    '001PC00000RXpImYAL',
    '001PC00000RxujRYAR'
    )

select *
from cleansed.segment_billing.segment_billing_cart_loaded_scd2
where 
    account_id in (
        25534919, 
        25410185, 
        25305955, 
        25429949, 
        25386939
        )
    and paid_customer = False





select
    pro_forma_signature_date,
    count(*) as total_wins,
    count(distinct crm_account_id) as unique_crm_wins,
    sum(case when origin is null then 1 else null end) as no_cart_event
from joined
group by 1
order by 1 desc



------ Previous Null query


select
    --date_trunc(week, win_date) as week,
    date_trunc('day', win_date) as day,
    case
        when first_shopping_cart_visit_timestamp is null then '0. no_cart_visit'
        when datediff(day, first_shopping_cart_visit_timestamp::date, win_date) <= 7 then '1. less_5'
        when datediff(day, first_shopping_cart_visit_timestamp::date, win_date) <= 15 then '2. 7-15'
        when datediff(day, first_shopping_cart_visit_timestamp::date, win_date) <= 30 then '3. 15-30'
        when datediff(day, first_shopping_cart_visit_timestamp::date, win_date) <= 100 then '4. 30-100'
        when datediff(day, first_shopping_cart_visit_timestamp::date, win_date) > 100 then '5. 100+'
        else 'Edge case'
    end as cart_visit_diff,
    count(*) tot_obs,
    count(distinct instance_account_id) as total_wins,
    count(distinct iff(first_shopping_cart_visit_timestamp is not null, instance_account_id, null)) cart_visit

from presentation.growth_analytics.trial_accounts
where win_date is not null
    and win_date >= '2025-07-01'
    and sales_model_at_win = 'Self-service'
group by all
order by 1, 2







select
    date_trunc(week, win_date) as week,
    sum(case when first_shopping_cart_visit_timestamp is null then 1 else 0 end) as no_cart_visit,
    sum(case when datediff(day, first_shopping_cart_visit_timestamp::date, win_date) <= 100 then 1 else 0 end) as cart_visit_less_100d,
    sum(case when datediff(day, first_shopping_cart_visit_timestamp::date, win_date) > 100 then 1 else 0 end) as cart_visit_plus_100d,
    count(*) tot_obs,
    count(distinct instance_account_id) as total_wins,
    sum(case when is_startup_program then 1 else 0 end) as is_startup_program,

    --- Percentages per time period
    no_cart_visit / tot_obs as pct_no_cart_visit,
    cart_visit_less_100d / tot_obs as pct_cart_visit_less_100d,
    cart_visit_plus_100d / tot_obs as pct_cart_visit_plus_100d,
    (no_cart_visit + cart_visit_plus_100d) / tot_obs as pct_cart_visit_no_100plus

from presentation.growth_analytics.trial_accounts
where win_date is not null
    and win_date >= '2024-01-01'
    and sales_model_at_win = 'Self-service'
    and is_startup_program = False
group by all
order by 1, 2










select
    date_trunc(week, win_date) as week,
    sum(case when datediff(day, instance_account_created_date::date, win_date) <= 30 then 1 else 0 end) as wins_less_30d,
    sum(case when datediff(day, instance_account_created_date::date, win_date) > 30 then 1 else 0 end) as wins_plus_30d,
    count(*) tot_obs,
    count(distinct instance_account_id) as total_wins,
    sum(case when is_startup_program then 1 else 0 end) as is_startup_program,

    --- Percentages per time period
    wins_less_30d / tot_obs as pct_wins_less_30d,
    wins_plus_30d / tot_obs as pct_wins_plus_30d,
    (wins_less_30d + wins_plus_30d) / tot_obs as pct_wins_no_30plus

from presentation.growth_analytics.trial_accounts
where win_date is not null
    and win_date >= '2025-01-01'
    and sales_model_at_win = 'Self-service'
    and is_startup_program = False
    and is_direct_buy = False
group by all
order by 1, 2






select
    date_trunc(week, win_date) as week,
    --- Case for all possible types of wins
    case 
        when is_startup_program then '0. Startup program'
        when is_direct_buy then '1. Direct buy'
        when datediff(day, instance_account_created_date::date, win_date) <= 15 then '2. Less than 15 days'
        when datediff(day, instance_account_created_date::date, win_date) <= 30 then '3. 15-30 days'
        when datediff(day, instance_account_created_date::date, win_date) <= 100 then '4. 30-100 days'
        when datediff(day, instance_account_created_date::date, win_date) > 100 then '5. More than 100 days'   
        else '6. Other'
    end as win_type,

    count(*) tot_obs,
    count(distinct instance_account_id) as total_wins,
    sum(instance_account_arr_usd_at_win) as total_wins_arr

from presentation.growth_analytics.trial_accounts
where win_date is not null
    and win_date >= '2025-01-01'
    and sales_model_at_win = 'Self-service'
    --and is_startup_program = False
    --and is_direct_buy = False
group by all
order by 1, 2








-----------------------------------------
--- Bookings vs wins

with bookings as (
    select 
        date_trunc(month, pro_forma_signature_date) as month,
        count(*) as total_obs,
        count(distinct crm_account_id) as unique_crm_accounts,
        sum(total_booking_arr_usd) as total_booking_arr_usd
    from functional.finance.sfa_crm_bookings_current
    where
        pro_forma_market_segment_at_close_date = 'Digital'
        and sales_motion = 'Online'
        and type = 'New Business'
        and pro_forma_signature_date >= '2024-01-01'
    group by all
),

wins as (
    select 
        date_trunc(month, win_date) as month,
        count(*) as total_obs,
        count(distinct instance_account_id) as unique_wins,
        sum(instance_account_arr_usd_at_win) as total_wins_arr
    from presentation.growth_analytics.trial_accounts
    where 
        win_date is not null
        and sales_model_at_win = 'Self-service'
        and is_direct_buy = False
        and win_date >= '2024-01-01'
    group by 1
)

select 
    bookings_.month,
    bookings_.total_obs as total_bookings,
    bookings_.unique_crm_accounts as unique_crm_bookings,
    bookings_.total_booking_arr_usd as total_bookings_arr,
    wins_.total_obs as total_wins,
    wins_.unique_wins as unique_wins,
    wins_.total_wins_arr as total_wins_arr
from bookings bookings_
left join wins wins_
    on bookings_.month = wins_.month
order by 1







-----------------------------------------
--- Calculate total cohorts

with deleted_accounts as (
    select 
        instance_account_id,
        min(source_snapshot_date) as first_deleted_date
    from foundational.customer.dim_instance_accounts_daily_snapshot
    where instance_account_is_deleted = True
    group by 1
),

dates as (
    select distinct
        last_day_of_month as month
    from foundational.finance.dim_date
    where 
        the_date >= '2024-01-01'
        and the_date <= current_date
),

panel as (
    select 
        dates_.month,
        accounts.instance_account_id,
        accounts.instance_account_created_date,
        accounts.win_date,
        accounts.is_startup_program,
        accounts.is_direct_buy,
        accounts.sales_model_at_win,
        deleted_accounts_.first_deleted_date
    from dates dates_
    cross join presentation.growth_analytics.trial_accounts accounts
    left join deleted_accounts deleted_accounts_
        on accounts.instance_account_id = deleted_accounts_.instance_account_id
    where 
        date_trunc(month, accounts.instance_account_created_date) <= dates_.month
        --- Not converted or converted in same period/future periods
        and (
             accounts.win_date is null
             or (date_trunc(month, win_date) >= date_trunc(month, dates_.month)
                 and accounts.sales_model_at_win = 'Self-service')
            )
        -- Non deleted or deleted in the future
        and (
            deleted_accounts_.first_deleted_date is null
            or deleted_accounts_.first_deleted_date > dates_.month
        )
        and is_direct_buy = False
),

conditions as (
    select
        month,
        --is_deleted,
        case 
            when date_trunc(month, win_date) = date_trunc(month, month) and is_startup_program then '0. Startup program'
            when date_trunc(month, win_date) = date_trunc(month, month) and datediff(day, instance_account_created_date::date, win_date) <= 15 then '1. 0 - 15 days'
            when date_trunc(month, win_date) = date_trunc(month, month) and datediff(day, instance_account_created_date::date, win_date) <= 30 then '2. 15 - 30 days'
            when date_trunc(month, win_date) = date_trunc(month, month) and datediff(day, instance_account_created_date::date, win_date) <= 100 then '3. 30 - 100 days'
            when date_trunc(month, win_date) = date_trunc(month, month) and datediff(day, instance_account_created_date::date, win_date) > 100 then '4. More than 100 days'
            when date_trunc(month, win_date) > date_trunc(month, month) then '5. Win in future'
            when win_date is null then null
            else '6. Other'
        end as win_type,
        case 
            when win_type is not null and win_type not in ('5. Win in future', '6. Other') then win_type
            when is_startup_program then '0. Startup program'
            when datediff(day, instance_account_created_date, month) <= 15 then '1. 0 - 15 days'
            when datediff(day, instance_account_created_date, month) <= 30 then '2. 15 - 30 days'
            when datediff(day, instance_account_created_date, month) <= 100 then '3. 30 - 100 days'
            when datediff(day, instance_account_created_date, month) > 100 then '4. More than 100 days'
            else 'Other'
        end as cohort,
        count(*) tot_obs,
        count(distinct instance_account_id) as unique_obs,
        count(distinct 
                case 
                    when 
                        date_trunc(month, win_date) = date_trunc(month, month) 
                    then instance_account_id 
                end) as wins
    from panel
    group by all
)


select *
from conditions
where month >= '2024-01-01'
order by 1, 2, 3






select 
    month,
    cohort,
    win_type,
    instance_account_id,
    instance_account_created_date,
    win_date,
    date_trunc('month', instance_account_created_date) as instance_account_created_date_month,
    date_trunc('month', win_date) as win_date_month
from conditions
where 
    is_deleted = True
    and win_date is not null
    --and month = '2025-07-31'




select *
from foundational.customer.DIM_INSTANCE_ACCOUNTS_DAILY_SNAPSHOT
where 
    INSTANCE_ACCOUNT_IS_DELETED = True
    and instance_account_id = 18002457
order by source_snapshot_date
limit 10

































--- Join all data
bookings_daily as (
    select
        bookings_.pro_forma_signature_date,
        billing_cart_loaded_.cart_screen,
        billing_cart_loaded_.cart_step,
        billing_cart_loaded_.cart_version,
        case 
            when billing_cart_loaded_.origin is null then 'No cart event' 
            when billing_cart_loaded_.origin in ('trial_welcome_screen', 'direct', 'expired-trial', 'central_admin') then billing_cart_loaded_.origin
            else 'other origin' 
        end as origin,
        billing_cart_loaded_.cart_type,
        billing_cart_loaded_.max_date as max_date_billing_cart_loaded,
        count(*) as total_wins,
        count(distinct bookings_.crm_account_id) as unique_crm_wins,
        sum(bookings_.total_booking_arr_usd) as total_booking_arr_usd,
        
        --- Total wins count
        --- At the opportunity level
        count(case
            when
                modal_buy_now_.crm_account_id is not null
                and modal_see_all_plans_.crm_account_id is null
                then bookings_.crm_account_id else null
        end) as total_bookings_just_buy_now,
        count(case
            when
                modal_buy_now_.crm_account_id is null
                and modal_see_all_plans_.crm_account_id is not null
                then bookings_.crm_account_id else null
        end) as total_bookings_just_see_all_plans,
        count(case
            when
                modal_buy_now_.crm_account_id is not null
                and modal_see_all_plans_.crm_account_id is not null
                then bookings_.crm_account_id else null
        end) as total_bookings_both,
        count(case
            when
                modal_buy_now_.crm_account_id is null
                and modal_see_all_plans_.crm_account_id is null
                then bookings_.crm_account_id else null
        end) as total_bookings_none,

        --- Bookings ARR
        sum(case
            when
                modal_buy_now_.crm_account_id is not null
                and modal_see_all_plans_.crm_account_id is null
                then bookings_.total_booking_arr_usd else null
        end) as total_bookings_just_buy_now_arr,
        sum(case
            when
                modal_buy_now_.crm_account_id is null
                and modal_see_all_plans_.crm_account_id is not null
                then bookings_.total_booking_arr_usd else null
        end) as total_bookings_just_see_all_plans_arr,
        sum(case
            when
                modal_buy_now_.crm_account_id is not null
                and modal_see_all_plans_.crm_account_id is not null
                then bookings_.total_booking_arr_usd else null
        end) as total_bookings_both_arr,
        sum(case
            when
                modal_buy_now_.crm_account_id is null
                and modal_see_all_plans_.crm_account_id is null
                then bookings_.total_booking_arr_usd else null
        end) as total_bookings_none_arr
        
    from bookings bookings_
    left join modal_buy_now modal_buy_now_
        on bookings_.crm_account_id = modal_buy_now_.crm_account_id
    left join modal_see_all_plans modal_see_all_plans_
        on bookings_.crm_account_id = modal_see_all_plans_.crm_account_id
    left join billing_cart_loaded billing_cart_loaded_
        on bookings_.crm_account_id = billing_cart_loaded_.crm_account_id
    where
        bookings_.pro_forma_signature_date >= '2025-05-01'
    group by all
)

--- Validate results

select
    pro_forma_signature_date,
    sum(total_wins) as total_wins,
    sum(unique_crm_wins) as unique_crm_wins,
    sum(total_bookings_just_buy_now) as total_bookings_just_buy_now,
    sum(total_bookings_just_see_all_plans) as total_bookings_just_see_all_plans,
    sum(total_bookings_both) as total_bookings_both,
    sum(total_bookings_none) as total_bookings_none
from bookings_daily
group by 1
order by 1 desc








joined as (
    select
        bookings_.crm_account_id,
        bookings_.pro_forma_signature_date,
        bookings_.total_booking_arr_usd,
        --- Cases for modal interactions
        case 
            when modal_buy_now_.crm_account_id is not null and modal_see_all_plans_.crm_account_id is null 
            then 'buy_now_clicked'
            when modal_buy_now_.crm_account_id is null and modal_see_all_plans_.crm_account_id is not null 
            then 'see_all_plans_clicked'
            when modal_buy_now_.crm_account_id is not null and modal_see_all_plans_.crm_account_id is not null 
            then 'both_clicked'
            when modal_buy_now_.crm_account_id is null and modal_see_all_plans_.crm_account_id is null 
            then 'no_cta_clicked'
            else 'unknown'
        end as modal_clicked,
        billing_cart_loaded_.cart_screen,
        billing_cart_loaded_.cart_step,
        billing_cart_loaded_.cart_version,
        billing_cart_loaded_.origin,
        billing_cart_loaded_.cart_type,
        billing_cart_loaded_.max_date as max_date_billing_cart_loaded

    from bookings bookings_
    left join modal_buy_now modal_buy_now_
        on bookings_.crm_account_id = modal_buy_now_.crm_account_id
    left join modal_see_all_plans modal_see_all_plans_
        on bookings_.crm_account_id = modal_see_all_plans_.crm_account_id
    left join billing_cart_loaded billing_cart_loaded_
        on bookings_.crm_account_id = billing_cart_loaded_.crm_account_id
    where
        bookings_.pro_forma_signature_date >= '2025-05-01'
)

select 
    pro_forma_signature_date,
    origin,
    count(*) as total_wins,
    count(distinct crm_account_id) as unique_crm_wins,
    sum(total_booking_arr_usd) as total_booking_arr_usd,
from joined
where modal_clicked = 'no_cta_clicked'
group by 1, 2
order by 1 desc, 2;











--- Testing counts
--- Numbers match online dashboard
--- https://prod-useast-a.online.tableau.com/#/site/zendesktableau/views/OnlineBusinessDashboardZDP/TotalBookings?:iid=1

select
    --date_trunc('month', month_closed) as month_closed,

    month_closed,
    count(*) as total_obs,
    count(distinct bookings_.crm_account_id) as unique_crm_accounts,
    sum(case when modal_buy_now_.crm_account_id is not null then 1 end) as buy_now_clicked,
    sum(case when modal_see_all_plans_.crm_account_id is not null then 1 end) as see_all_plans_clicked,
    buy_now_clicked + see_all_plans_clicked as total_cta_clicked,
    sum(total_booking_arr_usd) as total_booking_arr_usd
from bookings bookings_
left join modal_buy_now modal_buy_now_
    on bookings_.crm_account_id = modal_buy_now_.crm_account_id
left join modal_see_all_plans modal_see_all_plans_
    on bookings_.crm_account_id = modal_see_all_plans_.crm_account_id
where
    bookings_.month_closed >= '2025-05-01'
group by 1
order by 1 desc






----------------------------------------------------
--- 1.2: Uncohorted funnel - payment page visits

--- Adjusting query to use payment page visits

--- Counting all payment page visits

select 
    date(original_timestamp) date_loaded,
    count(*) as total_count,
    count(distinct account_id) as unique_accounts
from cleansed.segment_billing.segment_billing_payment_loaded_scd2 payment_loaded
where paid_customer = FALSE
group by 1
order by 1 desc





--- Payment page funnel
with payment_loaded as (
    select
        date(original_timestamp) as date_loaded,
        account_id
    from cleansed.segment_billing.segment_billing_payment_loaded_scd2 payment_loaded
    where
        date(original_timestamp) >= '2025-05-01'
),

--- User ever clicked on "buy now" or "see all plans" modals in the past
modal_buy_now as (
    select
        buy_now.account_id,
        max(date(buy_now.original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 buy_now
    inner join presentation.growth_analytics.trial_accounts accounts 
        on 
            accounts.instance_account_id = buy_now.account_id
    inner join payment_loaded 
        on 
            payment_loaded.account_id = buy_now.account_id
            and date(buy_now.original_timestamp) <= payment_loaded.date_loaded
    group by all
),

modal_see_all_plans as (
    select
        see_all_plans.account_id,
        max(date(see_all_plans.original_timestamp)) as max_date
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2 see_all_plans
    inner join presentation.growth_analytics.trial_accounts accounts 
        on 
            accounts.instance_account_id = see_all_plans.account_id
    inner join payment_loaded 
        on 
            payment_loaded.account_id = see_all_plans.account_id
            and date(see_all_plans.original_timestamp) <= payment_loaded.date_loaded
    group by all
),

--- Join payment page visits & segment funnel
payment_visits_daily as (
    select
        payment_loaded_.date_loaded,
        count(*) as total_payment_visits,
        count(distinct payment_loaded_.account_id) as unique_payment_visits,
        
        --- Total visits count
        count(case 
            when 
                modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is null 
                then payment_loaded_.account_id else null 
        end) as total_payment_visits_just_buy_now,
        count(case 
            when 
                modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is not null 
                then payment_loaded_.account_id else null 
        end) as total_payment_visits_just_see_all_plans,
        count(case 
            when 
                modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is not null 
                then payment_loaded_.account_id else null 
        end) as total_payment_visits_both,
        count(case 
            when 
                modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is null 
                then payment_loaded_.account_id else null 
        end) as total_payment_visits_none,
        
        --- Unique visits count
        count(distinct case 
            when 
                modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is null 
                then payment_loaded_.account_id else null 
        end) as unique_payment_visits_just_buy_now,
        count(distinct case 
            when 
                modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is not null 
                then payment_loaded_.account_id else null 
        end) as unique_payment_visits_just_see_all_plans,
        count(distinct case 
            when 
                modal_buy_now_.account_id is not null 
                and modal_see_all_plans_.account_id is not null 
                then payment_loaded_.account_id else null 
        end) as unique_payment_visits_both,
        count(distinct case 
            when 
                modal_buy_now_.account_id is null 
                and modal_see_all_plans_.account_id is null 
                then payment_loaded_.account_id else null 
        end) as unique_payment_visits_none
        
    from payment_loaded payment_loaded_
    left join modal_buy_now modal_buy_now_ 
        on payment_loaded_.account_id = modal_buy_now_.account_id
    left join modal_see_all_plans modal_see_all_plans_ 
        on payment_loaded_.account_id = modal_see_all_plans_.account_id
    group by 1
)

--- Validate results

select
    date_loaded,
    --- Total counts
    sum(total_payment_visits) as total_payment_visits,
    sum(total_payment_visits_just_buy_now) as total_payment_visits_just_buy_now,
    sum(total_payment_visits_just_see_all_plans) as total_payment_visits_just_see_all_plans,
    sum(total_payment_visits_both) as total_payment_visits_both,
    sum(total_payment_visits_none) as total_payment_visits_none,

    --- Unique counts
    sum(unique_payment_visits) as unique_payment_visits,
    sum(unique_payment_visits_just_buy_now) as unique_payment_visits_just_buy_now,
    sum(unique_payment_visits_just_see_all_plans) as unique_payment_visits_just_see_all_plans,
    sum(unique_payment_visits_both) as unique_payment_visits_both,
    sum(unique_payment_visits_none) as unique_payment_visits_none
from payment_visits_daily
group by date_loaded
order by date_loaded desc;












--- User fired billing cart loaded in the last 15 days
billing_cart_loaded as (
    select
        account_id,
        cart_screen,
        cart_step,
        cart_version,
        origin,
        cart_type,
        date(original_timestamp) as max_date
    from 
        cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing_cart_loaded
    inner join accounts 
        on 
            accounts.instance_account_id = billing_cart_loaded.account_id
            -- 15 day timeframe last 15 days before win date
            and date(billing_cart_loaded.original_timestamp) <= accounts.win_date
            and date(billing_cart_loaded.original_timestamp) >= dateadd('day', -15, accounts.win_date)
    where paid_customer = FALSE
    qualify row_number() over (partition by account_id order by original_timestamp desc) = 1
)

select 
    accounts_.*,
    case 
        when modal_buy_now_.account_id is not null or modal_see_all_plans_.account_id is not null 
        then 'buy_now/see_all_plans_clicked' else 'no_cta_clicked'
    end as modal_clicked,
    modal_buy_now_.max_date as max_date_modal_buy_now,
    modal_see_all_plans_.max_date as max_date_modal_see_all_plans,
    billing_cart_loaded_.* exclude (account_id),
        case when billing_cart_loaded_.account_id is null then 1 else 0 end as cart_null
from accounts accounts_
left join modal_buy_now modal_buy_now_ 
    on accounts_.instance_account_id = modal_buy_now_.account_id
left join modal_see_all_plans modal_see_all_plans_ 
    on accounts_.instance_account_id = modal_see_all_plans_.account_id
left join billing_cart_loaded billing_cart_loaded_ 
    on accounts_.instance_account_id = billing_cart_loaded_.account_id
where 
    accounts_.is_won = 1
    --- Selecting only wins from Jul29
    and win_date = '2025-07-29'
order by win_date












---------------------------------------------
--- 2.0 Cohorted funnel

---- Auto-trigger loads do not happen within the first 2 days after account creation


--- Minimum 3 days before showing auto pop up
select 
    date(original_timestamp) as date_loaded,
    count(*) as total_auto_trigger_loads,
    count(distinct account_id) as unique_auto_trigger_loads,
    --- Max & min date diff between account creation and auto-trigger load
    min(datediff('day', date(instance_account_created_date), date(original_timestamp))) as minimum_age_days,
    max(datediff('day', date(instance_account_created_date), date(original_timestamp))) as max_age_days
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 loads
inner join presentation.growth_analytics.trial_accounts trial_accounts
    on loads.account_id = trial_accounts.instance_account_id
where loads.source = 'auto_trigger'
group by 1
order by 1



select distinct
    trial_accounts.instance_account_created_date,
    account_id
    --loads.*
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 loads
inner join presentation.growth_analytics.trial_accounts trial_accounts
    on loads.account_id = trial_accounts.instance_account_id
where datediff('day', date(instance_account_created_date), date(original_timestamp)) >= 200


--- Using dim instance
--- Minimum 3 days before showing auto pop up
select 
    date(original_timestamp) as date_loaded,
    count(*) as total_auto_trigger_loads,
    count(distinct account_id) as unique_auto_trigger_loads,
    --- Max & min date diff between account creation and auto-trigger load
    min(datediff('day', date(instance_account_created_timestamp), date(original_timestamp))) as minimum_age_days,
    max(datediff('day', date(instance_account_created_timestamp), date(original_timestamp))) as max_age_days
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 loads
inner join foundational.customer.dim_instance_accounts_daily_snapshot_bcv accounts
    on loads.account_id = accounts.instance_account_id
where 
    loads.source = 'auto_trigger'
    and instance_account_derived_type = 'Active Trial'
group by 1
order by 1








--- Main query

with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        --- Verified date next 2 days after account creation
        case 
            when
                trial_accounts.first_verified_date is not null
                and trial_accounts.first_verified_date <= dateadd('day', 2, date(trial_accounts.instance_account_created_date)) 
            then 1 else null
        end as verified_flag,
        --- Wins next 2 days after account creation
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE  
                and trial_accounts.win_date <= dateadd('day', 2, date(trial_accounts.instance_account_created_date)) 
            then 1 else null
        end as win_flag,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE  
                and trial_accounts.win_date <= dateadd('day', 2, date(trial_accounts.instance_account_created_date)) 
            then instance_account_arr_usd_at_win else null
        end as win_flag_arr,
    from presentation.growth_analytics.trial_accounts trial_accounts 
    where 
        trial_accounts.instance_account_created_date >= '2025-07-17'
),

--------------
--- Actions:
--- Will measure all actions performed in the next 2 days after account creation


--- Segment funnel

prompt_load as (
    select
        prompt_click.account_id,
        count(*) as total_events,
        count(distinct prompt_click.account_id) as unique_events
    from
        cleansed.segment_support.growth_engine_trial_cta_1_scd2 prompt_click
    inner join accounts trial_accounts
        on 
            prompt_click.account_id = trial_accounts.instance_account_id
            and prompt_click.original_timestamp >= trial_accounts.instance_account_created_date
            and prompt_click.original_timestamp <= dateadd('day', 2, date(trial_accounts.instance_account_created_date))
    group by all
),

modal_load as (
    select
        load.account_id,
        count(*) as total_events,
        count(distinct load.account_id) as unique_events,
        -- CTA
        sum(case when load.source = 'CTA' then 1 else 0 end) as total_cta_count,
        count(distinct case when load.source = 'CTA' then load.account_id else null end) as unique_cta_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
    inner join accounts trial_accounts
        on 
            load.account_id = trial_accounts.instance_account_id
            and load.original_timestamp >= trial_accounts.instance_account_created_date
            and load.original_timestamp <= dateadd('day', 2, date(trial_accounts.instance_account_created_date))
    group by all
),

--- Separated auto trigger loads to change timeframe (5 days)
modal_load_auto_trigger as (
    select
        load.account_id,
        count(*) as total_auto_trigger_count,
        count(distinct load.account_id) as unique_auto_trigger_count
    from
        cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 load
    inner join accounts trial_accounts
        on 
            load.account_id = trial_accounts.instance_account_id
            and load.original_timestamp >= trial_accounts.instance_account_created_date
            and load.original_timestamp <= dateadd('day', 5, date(trial_accounts.instance_account_created_date))
    where load.source = 'auto_trigger'
    group by all
),

modal_dismiss as (
    select
        dismiss.account_id,
        count(*) as total_events,
        count(distinct dismiss.account_id) as unique_events
    from
        cleansed.segment_support.growth_engine_trial_cta_1_dismiss_offer_scd2 dismiss
    inner join accounts trial_accounts
        on 
            dismiss.account_id = trial_accounts.instance_account_id
            and dismiss.original_timestamp >= trial_accounts.instance_account_created_date
            and dismiss.original_timestamp <= dateadd('day', 2, date(trial_accounts.instance_account_created_date))
    group by all
),

modal_buy_now as (
    select
        buy_now.account_id,
        count(*) as total_events,
        count(distinct buy_now.account_id) as unique_events
    from
        cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 buy_now
    inner join accounts trial_accounts
        on 
            buy_now.account_id = trial_accounts.instance_account_id
            and buy_now.original_timestamp >= trial_accounts.instance_account_created_date
            and buy_now.original_timestamp <= dateadd('day', 2, date(trial_accounts.instance_account_created_date))
    group by all
),

modal_see_all_plans as (
    select
        see_all_plans.account_id,
        count(*) as total_events,
        count(distinct see_all_plans.account_id) as unique_events
    from
        cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2 see_all_plans
    inner join accounts trial_accounts
        on 
            see_all_plans.account_id = trial_accounts.instance_account_id
            and see_all_plans.original_timestamp >= trial_accounts.instance_account_created_date
            and see_all_plans.original_timestamp <= dateadd('day', 2, date(trial_accounts.instance_account_created_date))
    group by all
),


--- Additional actions: logins & payment page visits

--- Logins
logins as (
    select  
        emails.instance_account_id,
        count(distinct emails.agent_last_login_timestamp) as total_logins,
        case when total_logins = 1 then max(emails.instance_account_id) else null end as login_1_flag,
        case when total_logins >= 2 then max(emails.instance_account_id) else null end as login_2_flag,
        date(max(emails.agent_last_login_timestamp)) as last_login_date
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv emails
    inner join accounts trial_accounts 
        on 
            emails.instance_account_id = trial_accounts.instance_account_id
            and emails.agent_last_login_timestamp >= trial_accounts.instance_account_created_date
            and emails.agent_last_login_timestamp <= dateadd('day', 2, date(trial_accounts.instance_account_created_date))
    where 
        emails.agent_role in ('Admin', 'Billing Admin')
        or emails.agent_is_owner = True
    group by emails.instance_account_id
),

--- Payment page visits
payment_page_visits as (
    select
        payment.account_id,
        count(*) as total_events,
        count(distinct payment.account_id) as unique_events
    from cleansed.segment_billing.segment_billing_payment_loaded_scd2 payment
    inner join accounts trial_accounts
        on 
            payment.account_id = trial_accounts.instance_account_id
            and payment.original_timestamp >= trial_accounts.instance_account_created_date
            and payment.original_timestamp <= dateadd('day', 2, date(trial_accounts.instance_account_created_date))
    where payment.paid_customer = FALSE
    group by all
),

--- Joining all data together & count events

cohorted_funnel as (
    select
        date(trial_accounts.instance_account_created_date) created_date,
        count(*) as total_created_accounts,
        count(distinct trial_accounts.instance_account_id) as unique_created_accounts,
        sum(trial_accounts.verified_flag) as total_verified_accounts,

        --- Segment funnel actions
        ---- Total counts
        sum(prompt_load.total_events) as total_prompt_loads,
        sum(modal_load.total_events) as total_modal_loads,
        sum(modal_load_auto_trigger.total_auto_trigger_count) as total_modal_loads_auto_trigger,
        sum(modal_load.total_cta_count) as total_modal_loads_cta,
        sum(modal_dismiss.total_events) as total_modal_dismisses,
        sum(modal_buy_now.total_events) as total_modal_buy_now,
        sum(modal_see_all_plans.total_events) as total_modal_see_all_plans,
        ---- Unique counts
        sum(prompt_load.unique_events) as unique_prompt_loads,
        sum(modal_load.unique_events) as unique_modal_loads,
        sum(modal_load_auto_trigger.unique_auto_trigger_count) as unique_modal_loads_auto_trigger,
        sum(modal_load.unique_cta_count) as unique_modal_loads_cta,
        sum(modal_dismiss.unique_events) as unique_modal_dismisses,
        sum(modal_buy_now.unique_events) as unique_modal_buy_now,
        sum(modal_see_all_plans.unique_events) as unique_modal_see_all_plans,

        --- Additional actions
        
        ---- Logins
        sum(logins.total_logins) as total_logins,
        count(distinct logins.login_1_flag) as unique_login_1,
        count(distinct logins.login_2_flag) as unique_login_2,
        
        --- Payment page visits
        sum(payment_page_visits.total_events) as total_payment_page_visits,
        sum(payment_page_visits.unique_events) as unique_payment_page_visits,
        ---- Case query for payment visits & modal interactions
        ---- Only unique events
        sum(
            case 
                when 
                    modal_buy_now.unique_events is not null
                    and modal_see_all_plans.unique_events is null
                then payment_page_visits.unique_events
            end) as unique_payment_page_visits_just_buy_now,
        sum(
            case 
                when 
                    modal_buy_now.unique_events is null
                    and modal_see_all_plans.unique_events is not null
                then payment_page_visits.unique_events
            end) as unique_payment_page_visits_just_see_all_plans,
        sum(
            case 
                when 
                    modal_buy_now.unique_events is not null
                    and modal_see_all_plans.unique_events is not null
                then payment_page_visits.unique_events
            end) as unique_payment_page_visits_both,
        sum(
            case 
                when 
                    modal_buy_now.unique_events is null
                    and modal_see_all_plans.unique_events is null
                then payment_page_visits.unique_events
            end) as unique_payment_page_visits_none,

        --- Wins
        sum(trial_accounts.win_flag) as total_wins,
        --- Testing wins uniqueness
        count(distinct case when trial_accounts.win_flag = 1 then trial_accounts.instance_account_id else null end) as unique_wins,
        sum(win_flag_arr) as total_wins_arr,

        --- By modal interactions
        count(distinct 
            case 
                when 
                    modal_buy_now.unique_events is not null
                    and modal_see_all_plans.unique_events is null
                    and trial_accounts.win_flag = 1
                then trial_accounts.instance_account_id
            end) as unique_wins_just_buy_now,
        count(distinct 
            case 
                when 
                    modal_buy_now.unique_events is null
                    and modal_see_all_plans.unique_events is not null
                    and trial_accounts.win_flag = 1
                then trial_accounts.instance_account_id
            end) as unique_wins_just_see_all_plans,
        count(distinct 
            case 
                when 
                    modal_buy_now.unique_events is not null
                    and modal_see_all_plans.unique_events is not null
                    and trial_accounts.win_flag = 1
                then trial_accounts.instance_account_id
            end) as unique_wins_both,
        count(distinct 
            case 
                when 
                    modal_buy_now.unique_events is null
                    and modal_see_all_plans.unique_events is null
                    and trial_accounts.win_flag = 1
                then trial_accounts.instance_account_id
            end) as unique_wins_none,
        
        --- By modal interactions & wins ARR
        sum(case
                when 
                    modal_buy_now.unique_events is not null
                    and modal_see_all_plans.unique_events is null
                    and trial_accounts.win_flag = 1
                then trial_accounts.win_flag_arr
            end) as total_wins_just_buy_now_arr,
        sum(case
                when 
                    modal_buy_now.unique_events is null
                    and modal_see_all_plans.unique_events is not null
                    and trial_accounts.win_flag = 1
                then trial_accounts.win_flag_arr
            end) as total_wins_just_see_all_plans_arr,
        sum(case
                when 
                    modal_buy_now.unique_events is not null
                    and modal_see_all_plans.unique_events is not null
                    and trial_accounts.win_flag = 1
                then trial_accounts.win_flag_arr
            end) as total_wins_both_arr,
        sum(case
                when 
                    modal_buy_now.unique_events is null
                    and modal_see_all_plans.unique_events is null
                    and trial_accounts.win_flag = 1
                then trial_accounts.win_flag_arr
            end) as total_wins_none_arr

    from accounts trial_accounts
    left join prompt_load on trial_accounts.instance_account_id = prompt_load.account_id
    left join modal_load on trial_accounts.instance_account_id = modal_load.account_id
    left join modal_load_auto_trigger on trial_accounts.instance_account_id = modal_load_auto_trigger.account_id
    left join modal_dismiss on trial_accounts.instance_account_id = modal_dismiss.account_id
    left join modal_buy_now on trial_accounts.instance_account_id = modal_buy_now.account_id
    left join modal_see_all_plans on trial_accounts.instance_account_id = modal_see_all_plans.account_id
    left join logins on trial_accounts.instance_account_id = logins.instance_account_id
    left join payment_page_visits on trial_accounts.instance_account_id = payment_page_visits.account_id

    group by all
    
)

select *, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CURRENT_TIMESTAMP) as updated_at
from cohorted_funnel
order by created_date 







--- 

joined as (
    select
        trial_accounts.instance_account_id,
        trial_accounts.instance_account_created_date,
        trial_accounts.first_verified_date,
        logins.login_1,
        logins.login_2,
        logins.last_login_date,
        first_modal.id as first_modal_id,
        first_modal.account_id as first_modal_account_id,
        modal_load.id as modal_load_id,
        modal_load.account_id as modal_load_account_id,
        modal_load.source
    from presentation.growth_analytics.trial_accounts trial_accounts
    left join logins on trial_accounts.instance_account_id = logins.instance_account_id
    left join cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 first_modal 
        on trial_accounts.instance_account_id = first_modal.account_id
    left join cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal_load 
        on trial_accounts.instance_account_id = modal_load.account_id
    where 
        trial_accounts.instance_account_created_date >= '2025-07-17'
)








select
    trial_accounts.instance_account_created_date,
    count(*) as total_accounts,
    count(distinct trial_accounts.instance_account_id) as unique_accounts,
    count(distinct case when trial_accounts.first_verified_date is not null then trial_accounts.instance_account_id else null end) as unique_verified_accounts,
    
    --------------------------
    --- Logins:
    count(distinct case when logins_.last_login_date is null and trial_accounts.first_verified_date is not null then trial_accounts.instance_account_id end) as unique_login_null,
    count(distinct case when trial_accounts.first_verified_date is not null then logins_.login_1 end) as unique_login_1,
    count(distinct case when trial_accounts.first_verified_date is not null then logins_.login_2 end) as unique_login_2,

    --------------------------
    --- First modal loads:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    count(distinct case when first_modal.account_id is not null then first_modal.id end) as total_first_modal,
    count(distinct case when first_modal.account_id is not null then trial_accounts.instance_account_id end) as unique_first_modal,
    count(distinct case when first_modal.account_id is not null and trial_accounts.first_verified_date is not null then first_modal.id end) as total_first_modal_verified,
    count(distinct case when first_modal.account_id is not null and trial_accounts.first_verified_date is not null then trial_accounts.instance_account_id end) as unique_first_modal_verified,
    count(distinct case
            when
                first_modal.account_id is not null
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then first_modal.id
        end) as total_first_modal_timeboxed,
    count(distinct case
            when
                first_modal.account_id is not null
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_first_modal_timeboxed,
   
    --- Total/unique by auto trigger
    count(distinct case when modal_load.source = 'auto_trigger' and first_modal.account_id is not null then first_modal.id end) as total_first_modal_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and first_modal.account_id is not null then trial_accounts.instance_account_id end) as unique_first_modal_auto_trigger,
    count(distinct case
            when
                first_modal.account_id is not null
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then first_modal.id
        end) as total_first_modal_timeboxed_auto_trigger,
    count(distinct case
            when
                first_modal.account_id is not null
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_first_modal_timeboxed_auto_trigger,
   
    --- Total/unique by CTA click
    count(distinct case when modal_load.source = 'CTA' and first_modal.account_id is not null then first_modal.id end) as total_first_modal_cta,
    count(distinct case when modal_load.source = 'CTA' and first_modal.account_id is not null then trial_accounts.instance_account_id end) as unique_first_modal_cta,
    count(distinct case
            when
                first_modal.account_id is not null
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then first_modal.id
        end) as total_first_modal_timeboxed_cta,
    count(distinct case
            when
                first_modal.account_id is not null
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', first_modal.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_first_modal_timeboxed_cta,

    --------------------------
    --- Modal loads:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    count(distinct case when modal_load.account_id is not null then modal_load.id end) as total_modal_loads,
    count(distinct case when modal_load.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_loads,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then modal_load.id
        end) as total_modal_loads_timeboxed,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_loads_timeboxed,
    
    --- Total/unique by auto trigger
    count(distinct case when modal_load.source = 'auto_trigger' and modal_load.account_id is not null then modal_load.id end) as total_modal_loads_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and modal_load.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_loads_auto_trigger,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then modal_load.id
        end) as total_modal_loads_timeboxed_auto_trigger,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_loads_timeboxed_auto_trigger,
    
    --- Total/unique by CTA click
    count(distinct case when modal_load.source = 'CTA' and modal_load.account_id is not null then modal_load.id end) as total_modal_loads_cta,
    count(distinct case when modal_load.source = 'CTA' and modal_load.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_loads_cta,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then modal_load.id
        end) as total_modal_loads_timeboxed_cta,
    count(distinct case 
            when 
                modal_load.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_loads_timeboxed_cta,

    --------------------------
    --- Modal buy now:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    count(distinct case when modal_buy_now.account_id is not null then modal_buy_now.id end) as total_modal_buy_now,
    count(distinct case when modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_buy_now,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then modal_buy_now.id
        end) as total_modal_buy_now_timeboxed,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_buy_now_timeboxed,
    
    --- Total/unique by auto trigger
    count(distinct case when modal_load.source = 'auto_trigger' and modal_buy_now.account_id is not null then modal_buy_now.id end) as total_modal_buy_now_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_buy_now_auto_trigger,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then modal_buy_now.id
        end) as total_modal_buy_now_timeboxed_auto_trigger,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_buy_now_timeboxed_auto_trigger,
    
    --- Total/unique by CTA click
    count(distinct case when modal_load.source = 'CTA' and modal_buy_now.account_id is not null then modal_buy_now.id end) as total_modal_buy_now_cta,
    count(distinct case when modal_load.source = 'CTA' and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_modal_buy_now_cta,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then modal_buy_now.id
        end) as total_modal_buy_now_timeboxed_cta,
    count(distinct case 
            when 
                modal_buy_now.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', modal_buy_now.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_modal_buy_now_timeboxed_cta,

    --------------------------
    --- Cart loaded:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    count(distinct case when cart_load.account_id is not null then cart_load.id end) as total_cart_loaded_all,
    count(distinct case when cart_load.account_id is not null then trial_accounts.instance_account_id end) as unique_cart_loaded_all,
    count(distinct case when cart_load.account_id is not null and modal_buy_now.account_id is not null then cart_load.id end) as total_cart_loaded_buy_now,
    count(distinct case when cart_load.account_id is not null and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_cart_loaded_buy_now,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then cart_load.id
        end) as total_cart_loaded_timeboxed,
    count(distinct case 
            when 
                cart_load.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_cart_loaded_timeboxed_all,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_cart_loaded_timeboxed_buy_now,
    
    --- Total/unique by auto trigger
    count(distinct case when modal_load.source = 'auto_trigger' and cart_load.account_id is not null and modal_buy_now.account_id is not null then cart_load.id end) as total_cart_loaded_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and cart_load.account_id is not null and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_cart_loaded_auto_trigger,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then cart_load.id
        end) as total_cart_loaded_timeboxed_auto_trigger,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_cart_loaded_timeboxed_auto_trigger,
    
    --- Total/unique by CTA click
    count(distinct case when modal_load.source = 'CTA' and cart_load.account_id is not null and modal_buy_now.account_id is not null then cart_load.id end) as total_cart_loaded_cta,
    count(distinct case when modal_load.source = 'CTA' and cart_load.account_id is not null and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_cart_loaded_cta,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then cart_load.id
        end) as total_cart_loaded_timeboxed_cta,
    count(distinct case 
            when 
                cart_load.account_id is not null and modal_buy_now.account_id is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', cart_load.original_timestamp)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_cart_loaded_timeboxed_cta,


    --------------------------
    --- Wins:
    
    --- Total/unique modal loads & timeboxed
    --- Seems majority of modal loads happen at the same day as account creation
    sum(case when trial_accounts.win_date is not null then 1 else 0 end) as total_wins,
    count(distinct case when trial_accounts.win_date is not null then trial_accounts.instance_account_id end) as unique_wins,
    sum(case when trial_accounts.win_date is not null and first_modal.account_id is not null then 1 else 0 end) as total_wins_first_modal,
    count(distinct case when trial_accounts.win_date is not null and first_modal.account_id is not null then trial_accounts.instance_account_id end) as unique_wins_first_modal,
    sum(case when trial_accounts.win_date is not null and modal_buy_now.account_id is not null then 1 else 0 end) as total_wins_buy_now,
    count(distinct case when trial_accounts.win_date is not null and modal_buy_now.account_id is not null then trial_accounts.instance_account_id end) as unique_wins_buy_now,
    sum(case when trial_accounts.win_date is not null and cart_load.account_id is not null then 1 else 0 end) as total_wins_cart_loaded,
    count(distinct case when trial_accounts.win_date is not null and cart_load.account_id is not null then trial_accounts.instance_account_id end) as unique_wins_cart_loaded,
    sum(case 
            when 
                trial_accounts.win_date is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then 1
            else 0
        end) as total_wins_timeboxed,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_timeboxed,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and first_modal.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_first_modal_timeboxed,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and modal_buy_now.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_modal_buy_now_timeboxed,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and cart_load.account_id is not null 
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_cart_load_timeboxed,
    
    --- Total/unique by auto trigger
    sum(case when modal_load.source = 'auto_trigger' and trial_accounts.win_date is not null then 1 else 0 end) as total_wins_auto_trigger,
    count(distinct case when modal_load.source = 'auto_trigger' and trial_accounts.win_date is not null then trial_accounts.instance_account_id end) as unique_wins_auto_trigger,
    sum(case 
            when 
                trial_accounts.win_date is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then 1
            else 0
        end) as total_wins_timeboxed_auto_trigger,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and modal_load.source = 'auto_trigger'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_timeboxed_auto_trigger,
    
    --- Total/unique by CTA click
    sum(case when modal_load.source = 'CTA' and trial_accounts.win_date is not null then 1 else 0 end) as total_wins_cta,
    count(distinct case when modal_load.source = 'CTA' and trial_accounts.win_date is not null then trial_accounts.instance_account_id end) as unique_wins_cta,
    sum(case 
            when 
                trial_accounts.win_date is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then 1
            else 0
        end) as total_wins_timeboxed_cta,
    count(distinct case 
            when 
                trial_accounts.win_date is not null 
                and modal_load.source = 'CTA'
                and datediff(day, trial_accounts.instance_account_created_date, date_trunc('day', trial_accounts.win_date)) <= 2
            then trial_accounts.instance_account_id
        end) as unique_wins_timeboxed_cta,

from presentation.growth_analytics.trial_accounts trial_accounts 
left join cleansed.segment_support.growth_engine_trial_cta_1_scd2 first_modal
    on trial_accounts.instance_account_id = first_modal.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal_load
    on trial_accounts.instance_account_id = modal_load.account_id
left join cleansed.segment_support.growth_engine_trial_cta_1_buy_now_scd2 modal_buy_now
    on trial_accounts.instance_account_id = modal_buy_now.account_id
left join logins logins_
    on trial_accounts.instance_account_id = logins_.instance_account_id
left join ( --- Count only cart loads from buy trial CTAs
    select *
    from cleansed.segment_billing.segment_billing_cart_loaded_scd2
    where 
        cart_screen in ('preset_trial_plan', 'buy_your_trial', 'buy_trial_plan')
        and cart_type = 'spp_self_service'
        and paid_customer = False
    ) cart_load
    on trial_accounts.instance_account_id = cart_load.account_id
where 
    trial_accounts.instance_account_created_date >= '2025-07-17'
group by all
order by 1





--------------------------------------------
--- Add verified trials & active logins per day


with verified_trials as (
    select distinct
        trial_create_date, 
        count(distinct account_id) as total_v_trials
    from presentation.growth_analytics.trial_shopping_cart_funnel
    where 
        first_verified_date is not null
        and trial_create_date >= '2025-07-15'
        --and employee_range_band in ('1-9','10-49')
    group by 1
),

--- Logins (only Admins, Billing Admins, Owners)
--- Used a nested CTE because when agents do not
--- login for consecutive days, field agent_last_login_timestamp
--- will be the same for multiple days, therefore the need of
--- deduplicating first.
logins as (
    select  
        date_trunc('day', emails.agent_last_login_timestamp) as login_date,
        count(*) as total_logins,
        count(distinct emails.instance_account_id) as unique_logins
    from (
        select distinct 
            emails.agent_last_login_timestamp,
            emails.instance_account_id
        from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot emails
        inner join presentation.growth_analytics.trial_accounts trial_accounts_ 
            on emails.instance_account_id = trial_accounts_.instance_account_id
        where 
            (
                emails.agent_role in ('Admin', 'Billing Admin')
                or emails.agent_is_owner = True
            )
            and emails.agent_last_login_timestamp >= '2025-07-15'
        ) emails
    group by all
),

merged as (
    select *
    from verified_trials trial_accounts
    left join logins logins_
        on trial_accounts.trial_create_date = logins_.login_date
)

select *
from merged
order by 1






select *
from propagated_foundational.product_agent_info.dim_agent_emails_bcv
where agent_last_login_timestamp >= '2025-08-01'
order by agent_last_login_timestamp desc
limit 10




select *
from propagated_foundational.product_agent_info.dim_agent_emails_bcv
where 
    instance_account_id = 17931397
    and agent_name = 'Zecoya'
--order by source_snapshot_date
--limit 10




select *
from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot
where 
    instance_account_id = 17931397
    and agent_name = 'Zecoya'
    and source_snapshot_date >= '2025-07-01'
order by source_snapshot_date
--limit 10





--------------------------------------------
--- Testing if events are firing ok


select 
    convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) date_adj,
    *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
--where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-26 14:30'
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-16'
order by 1





select 
    count(*)
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-26 14:30'
order by 1


select *
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
order by convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) desc
limit 10


select convert_timezone('UTC', 'America/Los_Angeles', max(dbt_updated_at)) 
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2




select convert_timezone('UTC', 'America/Los_Angeles', max(original_timestamp)) 
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where date(original_timestamp) <= '2025-08-31'



select 
    count(*) as tot_obs,
    count(distinct account_id) as unique_accounts
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
--where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-26 14:30'
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-16'



select 
    count(*) as tot_obs,
    count(distinct account_id) as unique_accounts
from cleansed.segment_support.growth_engine_trial_cta_1_see_all_plans_scd2
--where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-26 14:30'
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-16'




select 
    count(*) as tot_obs,
    count(distinct account_id) as unique_accounts
from cleansed.segment_support.growth_engine_trial_cta_variant_b_see_all_plans_scd2
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-26 14:30'



select 
    source,
    count(*) as tot_obs,
    count(distinct account_id) as unique_accounts
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-08-26 14:30'
group by 1




select max(win_date)
from presentation.growth_analytics.trial_accounts




--- Max update date GE trial recommendation tables

select convert_timezone('UTC', 'America/Los_Angeles', max(dbt_updated_at)) 
from cleansed.segment_support.growth_engine_trial_cta_1_scd2



select convert_timezone('UTC', 'America/Los_Angeles', max(dbt_updated_at)) 
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2





select 
    account_id,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as first_event,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as last_event
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-17'
group by 1
order by 2 desc




select 
    account_id,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as first_event,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as last_event
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-18'
group by 1
order by 2 desc






select 
    account_id,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as first_event,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as last_event
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where original_timestamp >= '2025-09-17'
group by 1
order by 2 desc





select 
    account_id,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as first_event,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as last_event
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where original_timestamp >= '2025-09-18'
group by 1
order by 2 desc







select 
    date_trunc('day', original_timestamp) as event_date,
    count(*) as total_events,
    count(distinct account_id) as unique_accounts,
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where original_timestamp >= '2025-08-01'
group by 1
order by 1





select 
    date_trunc('day', original_timestamp) as event_date,
    count(*) as total_events,
    count(distinct account_id) as unique_accounts,
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where original_timestamp >= '2025-08-01'
group by 1
order by 1




select *
from propagated_cleansed.segment_support.growth_engine_couponmodal_work_modal_2_apply_offer_click_1_scd2
order by convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) desc




select 
    account_id,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as first_event,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as last_event
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where original_timestamp >= '2025-09-17'
group by 1
order by 2 desc





select 
    offer_id,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as first_event,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as last_event
from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-10'
group by 1
order by 2 desc





select 
    account_id,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as first_event,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as last_event
from cleansed.segment_support.growth_engine_trial_cta_1_scd2
where date_trunc('day', convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) = '2025-09-11'
group by 1
order by 2 desc









select 
    account_id,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as first_event,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as last_event
from RAW.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_CTA_1
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-16'
group by 1
order by 2 desc




select 
    account_id,
    count(*) as total_events,
    min(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as first_event,
    max(convert_timezone('UTC', 'America/Los_Angeles', original_timestamp)) as last_event
from RAW.SEGMENT_SUPPORT.GROWTH_ENGINE_TRIAL_cta_1_modal_load
where convert_timezone('UTC', 'America/Los_Angeles', original_timestamp) >= '2025-09-16'
group by 1
order by 2 desc




