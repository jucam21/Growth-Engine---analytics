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










select distinct account_id
from CLEANSED.SALESFORCE.SALESFORCE_OPPORTUNITY_SCD2
limit 10




select distinct account_id
from CLEANSED.SALESFORCE.SALESFORCE_OPPORTUNITY_bcv
limit 10


0016R0000389G23QAE
001PC00000RTq8XYAT
0011E00001oAzTtQAK
0016R00003KuhL4QAJ
001PC00000M8yK9YAJ
001PC00000PrBqNYAV
0018000000zTW0PAAW
001PC00000L5DTeYAN
001PC00000OlnviYAB



select *
from CLEANSED.SALESFORCE.SALESFORCE_OPPORTUNITY_bcv
where account_id = '0016R0000389G23QAE'



select *
from FOUNDATIONAL.CUSTOMER.DIM_CRM_OPPORTUNITIES_DAILY_SNAPSHOT_BCV
where crm_account_id = '0016R0000389G23QAE'


select *
from FOUNDATIONAL.CUSTOMER.FACT_CRM_OPPORTUNITIES_DAILY_SNAPSHOT_bcv opps
left join cleansed.salesforce.salesforce_user_bcv u
on opps.CRM_USER_OWNER_ID = u.id
where crm_account_id = '001PC00000RDAgTYAX'



select *
FROM functional.finance.sfa_crm_bookings_current
where crm_account_id = '001PC00000OlnviYAB'




FROM functional.finance.sfa_crm_bookings_current b

LEFT JOIN last_date ld 
    on ld.date = b.pro_forma_signature_date
LEFT JOIN FOUNDATIONAL.FINANCE.DIM_DATE d
	ON d.the_date = b.pro_forma_signature_date
LEFT JOIN FOUNDATIONAL.FINANCE.DIM_DATE dd 
	ON dd.the_date = ld.last_date
left join cleansed.salesforce.salesforce_user_bcv u 
    on b.opportunity_owner_id = u.id



select 
    *
from FOUNDATIONAL.CUSTOMER.FACT_CRM_OPPORTUNITIES_DAILY_SNAPSHOT_bcv opps
left join cleansed.salesforce.salesforce_user_bcv u
on opps.CRM_USER_OWNER_ID = u.id
left join FOUNDATIONAL.CUSTOMER.DIM_CRM_OPPORTUNITIES_DAILY_SNAPSHOT_BCV opps_2
on opps.crm_opportunity_id = opps_2.CRM_OPPORTUNITY_ID
where crm_account_id = '001PC00000RDAgTYAX'





select 
    u.name,
    count(*) total_opps
from FOUNDATIONAL.CUSTOMER.FACT_CRM_OPPORTUNITIES_DAILY_SNAPSHOT_bcv opps
left join cleansed.salesforce.salesforce_user_bcv u
on opps.CRM_USER_OWNER_ID = u.id
left join FOUNDATIONAL.CUSTOMER.DIM_CRM_OPPORTUNITIES_DAILY_SNAPSHOT_BCV opps_2
on opps.crm_opportunity_id = opps2.crm_opportunity_id
group by 1
order by 2 desc




25232303
25607453
23863653
25223058
25037652
25294736
25788089
24372869



select *
from foundational.customer.entity_mapping_daily_snapshot
where instance_account_id = 25788089





------------------------------------------------------------
--- Validate sales touched flag

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

--- Opps created in the last 2 months before login
gtm_touched_raw as (
    select 
        agent_logins_.account_id,
        date_trunc('month', agent_logins_.login_timestamp) as login_month,
        listagg(distinct case 
            when lower(opp_user.name) like '%sam hansen%' then 'online' 
            when lower(opp_user.name) not like '%sam hansen%' then 'gtm_touched' 
            when opp_user.id is null then 'not_touched'
            else 'error'
        end, ' / ') as gtm_touched_tmp,
        case 
            when gtm_touched_tmp like '%online%' and gtm_touched_tmp like '%gtm%' then 'gtm_touched & online' 
            else gtm_touched_tmp 
        end as gtm_touched
    from agent_logins agent_logins_
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping_bcv
        on agent_logins_.account_id = mapping_bcv.instance_account_id
    left join foundational.customer.fact_crm_opportunities_daily_snapshot_bcv opps
        on mapping_bcv.crm_account_id = opps.crm_account_id
    inner join foundational.customer.dim_crm_opportunities_daily_snapshot_bcv opps_create
        on 
            opps.crm_opportunity_id = opps_create.crm_opportunity_id
            and date_trunc('month', opps_create.opportunity_created_date) <= date_trunc('month', agent_logins_.login_timestamp)
            and date_trunc('month', opps_create.opportunity_created_date) >= date_trunc('month', agent_logins_.login_timestamp) - interval '2 months'
    inner join cleansed.salesforce.salesforce_user_bcv opp_user
        on 
            opps.crm_user_owner_id = opp_user.id
    group by 1,2
    --order by 1,2,3
),

--- Most of them do not have a CRM account id. This is the main reason for Null opps
crm_join as (
    select 
        agent_logins_.*,
        mapping_bcv.crm_account_id
    from agent_logins agent_logins_
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping_bcv
            on agent_logins_.account_id = mapping_bcv.instance_account_id
)

select 
    gtm_touched,
    count(*) as total_accounts,
    count(distinct account_id) as unique_accounts
from gtm_touched_raw
group by all
order by 1,2



select distinct account_id
from crm_join
where crm_account_id is null
limit 10












select 
    cta_name,
    count(*) as total_accounts
from FUNCTIONAL.GROWTH_ANALYTICS_STAGING.STEP_1_2_PAYMENT_PAGE
group by 1



select 
    product_cta,
    count(*) as total_accounts
from FUNCTIONAL.GROWTH_ANALYTICS_STAGING.STEP_1_2_PAYMENT_PAGE
group by 1






------------------------------------------------------------
--- Logins new funnel

--- Logins query
--- Include agent logins, as well as some segment events
--- Ensuring events are from trialists only
--- Therefore, only counting events before win date, or if win date is null
--- Add gtm touched flag


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

--- Opps created in the last 2 months before login
gtm_touched_raw as (
    select 
        agent_logins_.account_id,
        date_trunc('month', agent_logins_.login_timestamp) as login_month,
        listagg(distinct case 
            when lower(opp_user.name) like '%sam hansen%' then 'online' 
            when lower(opp_user.name) not like '%sam hansen%' then 'gtm_touched' 
            when opp_user.id is null then 'not_touched'
            else 'error'
        end, ' / ') as gtm_touched_tmp,
        case 
            when gtm_touched_tmp like '%online%' and gtm_touched_tmp like '%gtm%' then 'gtm_touched & online' 
            else gtm_touched_tmp 
        end as gtm_touched
    from logins_union agent_logins_
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping_bcv
        on agent_logins_.account_id = mapping_bcv.instance_account_id
    left join foundational.customer.fact_crm_opportunities_daily_snapshot_bcv opps
        on mapping_bcv.crm_account_id = opps.crm_account_id
    inner join foundational.customer.dim_crm_opportunities_daily_snapshot_bcv opps_create
        on 
            opps.crm_opportunity_id = opps_create.crm_opportunity_id
            and date_trunc('month', opps_create.opportunity_created_date) <= date_trunc('month', agent_logins_.login_timestamp)
            and date_trunc('month', opps_create.opportunity_created_date) >= date_trunc('month', agent_logins_.login_timestamp) - interval '2 months'
    inner join cleansed.salesforce.salesforce_user_bcv opp_user
        on 
            opps.crm_user_owner_id = opp_user.id
    group by 1,2
    --order by 1,2,3
),

logins_funnel as (
    select 
        logins_union_.*,
        --- Flags to measure if win/cart visit is from same month
        wins_.account_id wins_flag,
        cart_visits_.account_id cart_visit_flag,
        coalesce(gtm_touched_raw_.gtm_touched, 'not_touched') as gtm_touched
    from logins_union logins_union_
    left join wins wins_
        on 
            logins_union_.account_id = wins_.account_id
            and date_trunc('month', wins_.win_date) = date(date_trunc('month', logins_union_.login_timestamp)) 
    left join cart_visits cart_visits_
        on 
            logins_union_.account_id = cart_visits_.account_id
            and date_trunc('month', cart_visits_.first_cart_visit) = date(date_trunc('month', logins_union_.login_timestamp))
    left join gtm_touched_raw gtm_touched_raw_
        on 
            logins_union_.account_id = gtm_touched_raw_.account_id
            and date_trunc('month', logins_union_.login_timestamp) = gtm_touched_raw_.login_month
)

select 
    date(date_trunc('month', login_timestamp)) login_month,
    gtm_touched,
    count(*) as total_logins,
    count(distinct account_id) as unique_logins,
    count(distinct cart_visit_flag) as cart_visit_flag,
    count(distinct wins_flag) as wins_flag
from logins_funnel
where login_month >= '2025-01-01'
group by all
order by 1,2





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








-------------------------------------------------------------
--- Legacy GTM touched flag


SELECT 

b.year_quarter as close_year_quarter
, d.week_of_quarter as week_of_qtr
, b.pro_forma_market_segment_at_close_date as market_segment
, b.pro_forma_region_at_close_date as region 
, case when b.pro_forma_type_detail_at_freeze_date = 'New Business' then 'New Business' else 'Expansion' end as type
, b.purchase_method
, '--' as type_of_expansion
, '--' as type_of_expansion_group
, b.total_arr_deal_band_sec as total_booking_arr_band_primary
, case when st.startup_flag = 1 then true else false end as startup_flag 
, case  
    when b.sales_motion = 'Online' and lower(u.name) not like '%sam hansen%' then 'Online w GTM touch'
    when b.sales_motion = 'Online' and lower(u.name) like '%sam hansen%' then 'Online'
    when b.sales_motion = 'Quoted' then 'Quoted' end as sales_motion_detail
, sum(b.total_booking_arr_usd) AS total_bookings_arr
, case when b.zendesk_suite_flag = true then sum(b.total_booking_arr_usd) else 0 end as suite_bookings_arr
, case when b.zendesk_suite_flag = false then sum(b.total_booking_arr_usd) else 0 end as non_suite_bookings_arr
, count(distinct b.crm_opportunity_id) AS deals
, case when b.zendesk_suite_flag = true then count(distinct b.crm_opportunity_id) else 0 end as suite_deals
, case when b.zendesk_suite_flag = false then count(distinct b.crm_opportunity_id) else 0 end as non_suite_deals
, dd.week_of_quarter as max_week_of_qtr --
, ld.last_date as latest_close_date --
, b.pro_forma_signature_date as date_sale
, d.quarter as quarter_of_year
, d.day_of_quarter as day_of_quarter
, d.year as year
, d.week_of_year as week_of_year
, case when d.day_name = 'Wed' then 1
	   when d.day_name = 'Thu' then 2
	   when d.day_name = 'Fri' then 3
	   when d.day_name = 'Sat' then 4
	   when d.day_name = 'Sun' then 5
	   when d.day_name = 'Mon' then 6
	   when d.day_name = 'Tue' then 7
  end as day_of_week
, d.day_of_year as day_of_year
, d.month as month_of_year
, b.sales_motion as sales_motion
, b.zendesk_suite_flag as suite_booking_flag
, gab.expansion_type as new_bucket_expansion 
, '--' as product_detail_expansion 
, '--' as billing_cycle_at_win 
, gab.months_to_expand_band as months_to_expand_band --, b.months_to_expand_band
, case when d.day_of_quarter <= dd.day_of_quarter - 1 then true else false end as qtd_flag
, case when st.is_direct_buy = 1 then true else false end as direct_buy_flag
, sum(ai.ai_bookings) as ai_bookings
, sum(ai.ai_opp_count) as ai_opp_count
, case when st.es_flag = 1 then true else false end as es_booking
, case when st.crm_account_id is null then true else false end as other_flag

FROM functional.finance.sfa_crm_bookings_current b

LEFT JOIN last_date ld 
    on ld.date = b.pro_forma_signature_date
LEFT JOIN FOUNDATIONAL.FINANCE.DIM_DATE d
	ON d.the_date = b.pro_forma_signature_date
LEFT JOIN FOUNDATIONAL.FINANCE.DIM_DATE dd 
	ON dd.the_date = ld.last_date
left join cleansed.salesforce.salesforce_user_bcv u 
    on b.opportunity_owner_id = u.id
left join instance_type st 
    on st.crm_account_id = b.crm_account_id
left join ga_bookings gab 
  on gab.opportunity_id = b.crm_opportunity_id
left join ai_bookings ai 
  on ai.crm_opportunity_id = b.crm_opportunity_id

WHERE d.year >= 2024 
 AND b.pro_forma_market_segment_at_close_date = 'Digital'

GROUP BY ALL

order by date_sale desc
