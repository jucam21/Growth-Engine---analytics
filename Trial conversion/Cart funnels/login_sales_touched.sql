-----------------------------------------------------
--- Query to determine login and sales touchpoints in the cart funnel




--- First, explorating easy to buy populations to base the query on it
select 
    date_trunc('month', date) as month_,
    sum(total_v_trials) total_v_trials,
    sum(cart_visits_passive) total_cart_visits_passive,
    sum(cart_visits) total_cart_visits,
    sum(wins) wins
from presentation.growth_analytics.trial_shopping_cart_traffic
where 
    date >= '2025-05-01'
    and (sales_model_at_win is null or sales_model_at_win = 'Self-service')
    and employee_range_band in ('1-9', '10-49')
group by 1
order by 1 





select 
    date_trunc('month', win_date) as month_,
    sum(case when win_date is not null then 1 else 0 end) wins,
    sum(case when first_cart_visit is not null then 1 else 0 end) cart_visits,
    sum(case when date_trunc('month', first_cart_visit) = date_trunc('month', win_date) then 1 else 0 end) cart_visits_month_win
from presentation.growth_analytics.trial_shopping_cart_funnel
where 
    win_date >= '2025-05-01'
    and (sales_model_at_win is null or sales_model_at_win = 'Self-service')
    and employee_range_band in ('1-9', '10-49')
    and startup_flag = false
group by 1
order by 1 








-------------------------------------------------------
--- Create logins query
--- Include agent logins, as well as some segment events
--- Decided to add all SS wins, to match E2B cart visit logic
--- Ensuring events are from trialists only
--- Therefore, only counting events before win date, or if win date is null

with agent_logins as (
    select distinct
        agents.instance_account_id account_id,
        agents.agent_last_login_timestamp as login_timestamp,
        'agent_login' as login_type,
    from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot agents
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            agents.instance_account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', agents.agent_last_login_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        agent_last_login_timestamp >= '2024-01-01'
        and (agent_role in ('Admin', 'Billing Admin') or agent_is_owner = True)
),

--- Segment events

--- Modal load event
modal_load as (
    select distinct
        modal.account_id,
        modal.original_timestamp as login_timestamp,
        'modal_load' as login_type
    from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            modal.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', modal.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        modal.original_timestamp >= '2024-01-01'
),

--- Billing cart loaded event
billing_cart_loaded as (
    select distinct
        billing.account_id,
        billing.original_timestamp as login_timestamp,
        'billing_cart_loaded' as login_type
    from cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            billing.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', billing.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        billing.original_timestamp >= '2024-01-01'
        and not billing.paid_customer
),
--- Billing payment loaded event
billing_payment_loaded as (
    select distinct
        billing.account_id,
        billing.original_timestamp as login_timestamp,
        'billing_payment_loaded' as login_type
    from cleansed.segment_billing.segment_billing_payment_loaded_scd2 billing
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            billing.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', billing.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        billing.original_timestamp >= '2024-01-01'
),

--- Simple setup event
simple_setup as (
    select distinct
        agents.instance_account_id as account_id,
        simple_setup.original_timestamp as login_timestamp,
        'simple_setup' as login_type
    from cleansed.segment_support.simple_setup_scd2 simple_setup
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv agents
        on simple_setup.user_id = agents.agent_id
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            agents.instance_account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', simple_setup.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        simple_setup.original_timestamp >= '2024-01-01'
),

--- Central admin navigation event
central_admin_navigation as (
    select distinct
        nav.account_id,
        nav.original_timestamp as login_timestamp,
        'central_admin_navigation' as login_type
    from cleansed.segment_central_admin.central_admin_navigation_search_scd2 nav
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            nav.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', nav.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        nav.original_timestamp >= '2024-01-01'
),

--- Group table
group_table as (
    select distinct
        group_events.group_id account_id,
        group_events.received_at as login_timestamp,
        'group_event' as login_type
    from cleansed.segment_support.groups_scd2 group_events
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            group_events.group_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', group_events.received_at) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        group_events.received_at >= '2024-01-01'
),

all_wins as (
    select 
        instance_account_id account_id,
        instance_account_created_date login_timestamp,
        'create_date' as login_type
    from presentation.growth_analytics.trial_accounts
    where 
        win_date >= '2024-01-01'
        and not is_direct_buy
        and is_abusive = false
    union all
    select 
        instance_account_id account_id,
        win_date login_timestamp,
        'win' as login_type
    from presentation.growth_analytics.trial_accounts
    where 
        win_date >= '2024-01-01'
        and not is_direct_buy
        and is_abusive = false
),

--- Union all logins
logins_union as (
    select * from agent_logins
    union all
    select * from modal_load
    union all
    select * from billing_cart_loaded
    union all
    select * from billing_payment_loaded
    union all
    select * from simple_setup
    union all
    select * from central_admin_navigation
    union all
    select * from group_table
    union all
    select * from all_wins
)

select
    date(date_trunc('month', login_timestamp)) as month_,
    count(*) as total_logins,
    count(distinct account_id) as total_logins
from logins_union
where 
    date_trunc('month', login_timestamp) >= '2025-01-01'
group by 1
order by 1





select
    date(date_trunc('month', login_timestamp)) as month_,
    count(*) as total_logins,
    count(distinct account_id) as total_logins
from logins_union
where 
    date_trunc('month', login_timestamp) >= '2025-01-01'
    and login_type in ('group_event')
group by 1
order by 1
















------------------------------------------------------------
--- Validate logins query against wins

--- Small difference between total wins and wins with logins.
--- Some of it is explained by logins from admins after the 30 day window

--- Logins query
--- Include agent logins, as well as some segment events
--- Ensuring events are from trialists only
--- Therefore, only counting events before win date, or if win date is null

with agent_logins as (
    select distinct
        agents.instance_account_id account_id,
        agents.agent_last_login_timestamp as login_timestamp,
        'agent_login' as login_type,
    from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot agents
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            agents.instance_account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', agents.agent_last_login_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        agent_last_login_timestamp >= '2024-01-01'
        and (agent_role in ('Admin', 'Billing Admin') or agent_is_owner = True)
),

--- Segment events

--- Modal load event
modal_load as (
    select distinct
        modal.account_id,
        modal.original_timestamp as login_timestamp,
        'modal_load' as login_type
    from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            modal.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', modal.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        modal.original_timestamp >= '2024-01-01'
),

--- Billing cart loaded event
billing_cart_loaded as (
    select distinct
        billing.account_id,
        billing.original_timestamp as login_timestamp,
        'billing_cart_loaded' as login_type
    from cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            billing.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', billing.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        billing.original_timestamp >= '2024-01-01'
        and not billing.paid_customer
),
--- Billing payment loaded event
billing_payment_loaded as (
    select distinct
        billing.account_id,
        billing.original_timestamp as login_timestamp,
        'billing_payment_loaded' as login_type
    from cleansed.segment_billing.segment_billing_payment_loaded_scd2 billing
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            billing.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', billing.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        billing.original_timestamp >= '2024-01-01'
),

--- Simple setup event
simple_setup as (
    select distinct
        agents.instance_account_id as account_id,
        simple_setup.original_timestamp as login_timestamp,
        'simple_setup' as login_type
    from cleansed.segment_support.simple_setup_scd2 simple_setup
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv agents
        on simple_setup.user_id = agents.agent_id
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            agents.instance_account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', simple_setup.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        simple_setup.original_timestamp >= '2024-01-01'
),

--- Central admin navigation event
central_admin_navigation as (
    select distinct
        nav.account_id,
        nav.original_timestamp as login_timestamp,
        'central_admin_navigation' as login_type
    from cleansed.segment_central_admin.central_admin_navigation_search_scd2 nav
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            nav.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', nav.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        nav.original_timestamp >= '2024-01-01'
),

--- Group table
group_table as (
    select distinct
        group_events.group_id account_id,
        group_events.received_at as login_timestamp,
        'group_event' as login_type
    from cleansed.segment_support.groups_scd2 group_events
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            group_events.group_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', group_events.received_at) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        group_events.received_at >= '2024-01-01'
),

all_wins as (
    select 
        instance_account_id account_id,
        instance_account_created_date login_timestamp,
        'create_date' as login_type
    from presentation.growth_analytics.trial_accounts
    where 
        win_date >= '2024-01-01'
        and not is_direct_buy
        and is_abusive = false
    union all
    select 
        instance_account_id account_id,
        win_date login_timestamp,
        'win' as login_type
    from presentation.growth_analytics.trial_accounts
    where 
        win_date >= '2024-01-01'
        and not is_direct_buy
        and is_abusive = false
),

--- Union all logins
logins_union as (
    select * from agent_logins
    union all
    select * from modal_load
    union all
    select * from billing_cart_loaded
    union all
    select * from billing_payment_loaded
    union all
    select * from simple_setup
    union all
    select * from central_admin_navigation
    union all
    select * from group_table
    union all
    select * from all_wins
),

--- Wins table
wins as (
    select 
        account_id,
        win_date,
        date_trunc('month', win_date) as month_win
    from presentation.growth_analytics.trial_shopping_cart_funnel
    where 
        win_date >= '2025-05-01'
        and (sales_model_at_win is null or sales_model_at_win = 'Self-service')
        and employee_range_band in ('1-9', '10-49')
        and startup_flag = false
),

wins_vs_logins as (
    select  
        wins_.*,
        count(distinct logins_union.login_timestamp) as total_logins,
        --- Logins flags: same month vs last 30 days or 60 days
        case when total_logins >= 1 then max(logins_union.account_id) else null end as login_flag_ever,
        case 
            when 
                total_logins >= 1 
                and date_trunc('month', max(logins_union.login_timestamp)) = wins_.month_win
            then max(logins_union.account_id) 
            else null 
        end as login_flag_same_month,
        case 
            when 
                total_logins >= 1 
                and date_trunc('day', max(logins_union.login_timestamp)) >= dateadd('day', -30, wins_.win_date)
            then max(logins_union.account_id) 
            else null 
        end as login_flag_30_days,
        case 
            when 
                total_logins >= 1 
                and date_trunc('day', max(logins_union.login_timestamp)) >= dateadd('day', -60, wins_.win_date)
            then max(logins_union.account_id) 
            else null 
        end as login_flag_60_days
    from wins wins_
    left join logins_union
        on 
            logins_union.account_id = wins_.account_id
            --- Logins before win date
            and date_trunc('day', logins_union.login_timestamp) <= wins_.win_date
    group by all
)

select 
    month_win,
    count(*) as total_wins,
    count(distinct account_id) as unique_wins,
    count(distinct case when login_flag_ever is not null then account_id else null end) as wins_with_logins_ever,
    count(distinct case when login_flag_same_month is not null then account_id else null end) as wins_with_logins_same_month,
    count(distinct case when login_flag_30_days is not null then account_id else null end) as wins_with_logins_30_days,
    count(distinct case when login_flag_60_days is not null then account_id else null end) as wins_with_logins_60_days
from wins_vs_logins
group by 1
order by 1








--- Search for some Null logins examples
select *
from logins
where 
    month_win = '2025-05-01'
    and login_flag is null
limit 10


select distinct agent_last_login_timestamp, instance_account_id
from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot
where 
    instance_account_id = 25519279
    and date_trunc('month', agent_last_login_timestamp) = '2025-08-01'
order by agent_last_login_timestamp



select distinct agent_last_login_timestamp, instance_account_id
from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot
where 
    instance_account_id = 24765959
    --and date_trunc('month', agent_last_login_timestamp) = '2025-05-01'
order by agent_last_login_timestamp











------------------------------------------------------------
--- Validate logins query against cart visits

--- Small difference between total wins and wins with logins.
--- Some of it is explained by logins from admins after the 30 day window

--- Logins query
--- Include agent logins, as well as some segment events
--- Ensuring events are from trialists only
--- Therefore, only counting events before win date, or if win date is null


with agent_logins as (
    select distinct
        agents.instance_account_id account_id,
        agents.agent_last_login_timestamp as login_timestamp,
        'agent_login' as login_type,
    from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot agents
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            agents.instance_account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', agents.agent_last_login_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        agent_last_login_timestamp >= '2024-01-01'
        and (agent_role in ('Admin', 'Billing Admin') or agent_is_owner = True)
),

--- Segment events

--- Modal load event
modal_load as (
    select distinct
        modal.account_id,
        modal.original_timestamp as login_timestamp,
        'modal_load' as login_type
    from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            modal.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', modal.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        modal.original_timestamp >= '2024-01-01'
),

--- Billing cart loaded event
billing_cart_loaded as (
    select distinct
        billing.account_id,
        billing.original_timestamp as login_timestamp,
        'billing_cart_loaded' as login_type
    from cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            billing.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', billing.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        billing.original_timestamp >= '2024-01-01'
        and not billing.paid_customer
),
--- Billing payment loaded event
billing_payment_loaded as (
    select distinct
        billing.account_id,
        billing.original_timestamp as login_timestamp,
        'billing_payment_loaded' as login_type
    from cleansed.segment_billing.segment_billing_payment_loaded_scd2 billing
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            billing.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', billing.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        billing.original_timestamp >= '2024-01-01'
),

--- Simple setup event
simple_setup as (
    select distinct
        agents.instance_account_id as account_id,
        simple_setup.original_timestamp as login_timestamp,
        'simple_setup' as login_type
    from cleansed.segment_support.simple_setup_scd2 simple_setup
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv agents
        on simple_setup.user_id = agents.agent_id
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            agents.instance_account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', simple_setup.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        simple_setup.original_timestamp >= '2024-01-01'
),

--- Central admin navigation event
central_admin_navigation as (
    select distinct
        nav.account_id,
        nav.original_timestamp as login_timestamp,
        'central_admin_navigation' as login_type
    from cleansed.segment_central_admin.central_admin_navigation_search_scd2 nav
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            nav.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', nav.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        nav.original_timestamp >= '2024-01-01'
),

--- Group table
group_table as (
    select distinct
        group_events.group_id account_id,
        group_events.received_at as login_timestamp,
        'group_event' as login_type
    from cleansed.segment_support.groups_scd2 group_events
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            group_events.group_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', group_events.received_at) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        group_events.received_at >= '2024-01-01'
),

all_wins as (
    select 
        instance_account_id account_id,
        instance_account_created_date login_timestamp,
        'create_date' as login_type
    from presentation.growth_analytics.trial_accounts
    where 
        win_date >= '2024-01-01'
        and not is_direct_buy
        and is_abusive = false
    union all
    select 
        instance_account_id account_id,
        win_date login_timestamp,
        'win' as login_type
    from presentation.growth_analytics.trial_accounts
    where 
        win_date >= '2024-01-01'
        and not is_direct_buy
        and is_abusive = false
),

--- Union all logins
logins_union as (
    select * from agent_logins
    union all
    select * from modal_load
    union all
    select * from billing_cart_loaded
    union all
    select * from billing_payment_loaded
    union all
    select * from simple_setup
    union all
    select * from central_admin_navigation
    union all
    select * from group_table
    union all
    select * from all_wins
),

--- Cart visits table
cart_visits as (
    select 
        account_id,
        first_cart_visit,
        date_trunc('month', first_cart_visit) as cart_visit_month
    from presentation.growth_analytics.trial_shopping_cart_funnel
    where 
        first_cart_visit >= '2025-05-01'
        and (sales_model_at_win is null or sales_model_at_win = 'Self-service')
        and employee_range_band in ('1-9', '10-49')
        and startup_flag = false
),

cart_visits_vs_logins as (
    select  
        cart_visits_.*,
        count(distinct logins_union.login_timestamp) as total_logins,
        --- Logins flags: same month vs last 30 days or 60 days
        case when total_logins >= 1 then max(logins_union.account_id) else null end as login_flag_ever,
        case 
            when 
                total_logins >= 1 
                and date_trunc('month', max(logins_union.login_timestamp)) = cart_visits_.cart_visit_month
            then max(logins_union.account_id) 
            else null 
        end as login_flag_same_month,
        case 
            when 
                total_logins >= 1 
                and date_trunc('day', max(logins_union.login_timestamp)) >= dateadd('day', -30, cart_visits_.first_cart_visit)
            then max(logins_union.account_id) 
            else null 
        end as login_flag_30_days,
        case 
            when 
                total_logins >= 1 
                and date_trunc('day', max(logins_union.login_timestamp)) >= dateadd('day', -60, cart_visits_.first_cart_visit)
            then max(logins_union.account_id) 
            else null 
        end as login_flag_60_days
    from cart_visits cart_visits_
    left join logins_union
        on 
            logins_union.account_id = cart_visits_.account_id
            --- Logins before win date
            and date_trunc('day', logins_union.login_timestamp) <= cart_visits_.first_cart_visit
    group by all
)

select 
    cart_visit_month,
    count(*) as total_cart_visits,
    count(distinct account_id) as unique_cart_visits,
    count(distinct case when login_flag_ever is not null then account_id else null end) as cart_visits_with_logins_ever,
    count(distinct case when login_flag_same_month is not null then account_id else null end) as cart_visits_with_logins_same_month,
    count(distinct case when login_flag_30_days is not null then account_id else null end) as cart_visits_with_logins_30_days,
    count(distinct case when login_flag_60_days is not null then account_id else null end) as cart_visits_with_logins_60_days
from cart_visits_vs_logins
group by 1
order by 1











select 
    is_startup_program, 
    count(*) as total_accounts
from presentation.growth_analytics.trial_accounts
group by 1
order by 1

------------------------------------------------------------
--- Logins new funnel

--- Logins query
--- Include agent logins, as well as some segment events
--- Ensuring events are from trialists only
--- Therefore, only counting events before win date, or if win date is null


with agent_logins as (
    select distinct
        agents.instance_account_id account_id,
        agents.agent_last_login_timestamp as login_timestamp,
        'agent_login' as login_type,
    from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot agents
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            agents.instance_account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', agents.agent_last_login_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        agent_last_login_timestamp >= '2024-01-01'
        and (agent_role in ('Admin', 'Billing Admin') or agent_is_owner = True)
),

--- Segment events

--- Modal load event
modal_load as (
    select distinct
        modal.account_id,
        modal.original_timestamp as login_timestamp,
        'modal_load' as login_type
    from cleansed.segment_support.growth_engine_trial_cta_1_modal_load_scd2 modal
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            modal.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', modal.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        modal.original_timestamp >= '2024-01-01'
),

--- Billing cart loaded event
billing_cart_loaded as (
    select distinct
        billing.account_id,
        billing.original_timestamp as login_timestamp,
        'billing_cart_loaded' as login_type
    from cleansed.segment_billing.segment_billing_cart_loaded_scd2 billing
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            billing.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', billing.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        billing.original_timestamp >= '2024-01-01'
        and not billing.paid_customer
),
--- Billing payment loaded event
billing_payment_loaded as (
    select distinct
        billing.account_id,
        billing.original_timestamp as login_timestamp,
        'billing_payment_loaded' as login_type
    from cleansed.segment_billing.segment_billing_payment_loaded_scd2 billing
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            billing.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', billing.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        billing.original_timestamp >= '2024-01-01'
),

--- Simple setup event
simple_setup as (
    select distinct
        agents.instance_account_id as account_id,
        simple_setup.original_timestamp as login_timestamp,
        'simple_setup' as login_type
    from cleansed.segment_support.simple_setup_scd2 simple_setup
    left join propagated_foundational.product_agent_info.dim_agent_emails_bcv agents
        on simple_setup.user_id = agents.agent_id
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            agents.instance_account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', simple_setup.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        simple_setup.original_timestamp >= '2024-01-01'
),

--- Central admin navigation event
central_admin_navigation as (
    select distinct
        nav.account_id,
        nav.original_timestamp as login_timestamp,
        'central_admin_navigation' as login_type
    from cleansed.segment_central_admin.central_admin_navigation_search_scd2 nav
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            nav.account_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', nav.original_timestamp) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        nav.original_timestamp >= '2024-01-01'
),

--- Group table
group_table as (
    select distinct
        group_events.group_id account_id,
        group_events.received_at as login_timestamp,
        'group_event' as login_type
    from cleansed.segment_support.groups_scd2 group_events
    inner join presentation.growth_analytics.trial_accounts trial_accounts_
        on 
            group_events.group_id = trial_accounts_.instance_account_id
            --- Counting events before win date, or if win date is null
            and (date_trunc('day', group_events.received_at) <= trial_accounts_.win_date
                 or trial_accounts_.win_date is null)
    where 
        group_events.received_at >= '2024-01-01'
),

all_wins as (
    select 
        instance_account_id account_id,
        instance_account_created_date login_timestamp,
        'create_date' as login_type
    from presentation.growth_analytics.trial_accounts
    where 
        win_date >= '2024-01-01'
        and not is_direct_buy
        and is_abusive = false
    union all
    select 
        instance_account_id account_id,
        win_date login_timestamp,
        'win' as login_type
    from presentation.growth_analytics.trial_accounts
    where 
        win_date >= '2024-01-01'
        and not is_direct_buy
        and is_abusive = false
),

--- Union all logins
logins_union as (
    select * from agent_logins
    union all
    select * from modal_load
    union all
    select * from billing_cart_loaded
    union all
    select * from billing_payment_loaded
    union all
    select * from simple_setup
    union all
    select * from central_admin_navigation
    union all
    select * from group_table
    union all
    select * from all_wins
),

--- Wins table
wins as (
    select 
        account_id,
        win_date,
        date_trunc('month', win_date) as month_win
    from presentation.growth_analytics.trial_shopping_cart_funnel
    where 
        win_date >= '2025-01-01'
        and (sales_model_at_win is null or sales_model_at_win = 'Self-service')
        and employee_range_band in ('1-9', '10-49')
        and startup_flag = false
),

--- Cart visits table
cart_visits as (
    select 
        account_id,
        first_cart_visit,
        date_trunc('month', first_cart_visit) as cart_visit_month
    from presentation.growth_analytics.trial_shopping_cart_funnel
    where 
        first_cart_visit >= '2025-01-01'
        and (sales_model_at_win is null or sales_model_at_win = 'Self-service')
        and employee_range_band in ('1-9', '10-49')
        and startup_flag = false
),

logins_funnel as (
    select 
        logins_union_.*,
        --- Flags to measure if win/cart visit is from same month
        wins_.account_id wins_flag,
        cart_visits_.account_id cart_visit_flag
    from logins_union logins_union_
    left join wins wins_
        on 
            logins_union_.account_id = wins_.account_id
            and date_trunc('month', wins_.win_date) = date(date_trunc('month', logins_union_.login_timestamp)) 
    left join cart_visits cart_visits_
        on 
            logins_union_.account_id = cart_visits_.account_id
            and date_trunc('month', cart_visits_.first_cart_visit) = date(date_trunc('month', logins_union_.login_timestamp))
)


select 
    date(date_trunc('month', login_timestamp)) login_month,
    count(*) as total_logins,
    count(distinct account_id) as unique_logins,
    count(distinct cart_visit_flag) as cart_visit_flag,
    count(distinct wins_flag) as wins_flag
from logins_funnel
where login_month >= '2025-01-01'
group by 1
order by 1





--24852729
--24979789

select *
from logins_union
where 
    account_id = 24852729
    and date(date_trunc('month', login_timestamp)) = '2025-05-01'












select *
from wins
where date(date_trunc('month', win_date)) = '2025-05-01'



select distinct wins_flag
from logins_funnel
where 
    date(date_trunc('month', login_timestamp)) = '2025-05-01'
    and wins_flag is not null












select *
from logins_union
where account_id = 25750259






select *
from logins_union
where 
    account_id = 25519279
limit 10



--- Search for some Null logins examples
select *
from cart_visits_vs_logins
where 
    cart_visit_month = '2025-09-01'
    and login_flag_ever is null
limit 10








select 
    cart_visit_month,
    count(*) as total_cart_visits,
    count(distinct account_id) as unique_cart_visits,
    count(distinct case when login_flag is not null then account_id else null end) as cart_visit_with_logins,
    count(distinct case when login_1_flag is not null then account_id else null end) as cart_visit_with_1_login,
    count(distinct case when login_2_flag is not null then account_id else null end) as cart_visit_with_2_logins
from logins
group by 1
order by 1






select distinct agent_last_login_timestamp, instance_account_id
from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot
where 
    instance_account_id = 25519279
    and date_trunc('month', agent_last_login_timestamp) = '2025-08-01'
order by agent_last_login_timestamp



--25750259
--25742369

select distinct agent_last_login_timestamp, instance_account_id
from propagated_foundational.product_agent_info.dim_agent_emails_daily_snapshot
where 
    instance_account_id = 25750259
    --and date_trunc('month', agent_last_login_timestamp) = '2025-05-01'
order by agent_last_login_timestamp




select distinct
    original_timestamp,
    account_id,
    paid_customer
from cleansed.segment_billing.segment_billing_cart_loaded_scd2
where 
    not paid_customer
    and account_id = 25750259
    --and original_timestamp >= '2025-09-01'
order by original_timestamp




select distinct
    original_timestamp,
    account_id,
    paid_customer
from cleansed.segment_billing.segment_billing_payment_loaded_scd2
where 
    not paid_customer
    and account_id = 25750259
    --and original_timestamp >= '2025-09-01'
order by original_timestamp



select distinct
    original_timestamp,
    account_id,
    paid_customer
from cleansed.bq_archive.billing_cart_loaded
where 
    not paid_customer
    and account_id = 25750259
    --and original_timestamp >= '2025-09-01'
order by original_timestamp



select distinct
    original_timestamp,
    account_id,
    paid_customer
from cleansed.bq_archive.billing_payment_loaded
where 
    not paid_customer
    and account_id = 25750259
    --and original_timestamp >= '2025-09-01'
order by original_timestamp



select 
    instance_account_id,
    win_date,
    is_direct_buy,
    is_abusive
from presentation.growth_analytics.trial_accounts
where instance_account_id = 25750259




        
select *
from presentation.growth_analytics.trial_shopping_cart_funnel
where 
    account_id = 25750259



----------------------------------------------

--- Logins legacy queries

--- Uncohorted logins

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


--- Cohorted Logins
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
