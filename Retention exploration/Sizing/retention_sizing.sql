--- File with the sizing query from GE document
--- https://docs.google.com/spreadsheets/d/1BbAdM4MK9Tn-KcPXmVsE064XwBWefMopHb_wWBCj8D4/edit?gid=2093612758#gid=2093612758


--------------------------------------------
--- Funnel query:
with sku_seats as (
    select
        account_id,
        name,
        boost,
        state,
        parse_json(plan_settings) as parsed_plan_settings,
        parse_json(plan_settings):plan:value::string as plan_type,
        parse_json(plan_settings):maxAgents:value::string as maxAgents_,
        row_number() over (
            partition by account_id,name,state
            order by updated_at desc
        ) as rank
    from propagated_formatted.accountsdb.skus
    qualify rank = 1
),

sku_rules as (
    select distinct
        account_id,
        -- Adding rules based on 4 parameters:
        -- 1. SKU name
        -- 2. Plan type
        -- 3. Relationship type (greater/less)
        -- 4. Max agents
        case
            when
                {{SKU_NAME}} = 'all'
                or {{PLAN_TYPE}} = 'all'
                then 1
            when
                {{OPERATOR}} = '<='
                and {{SKU_NAME}} like lower(name)
                and {{PLAN_TYPE}} like lower(plan_type)
                and maxagents_ <= {{SEATS}}
                then 1
            when
                {{OPERATOR}} = '>'
                and {{SKU_NAME}} like lower(name)
                and {{PLAN_TYPE}} like lower(plan_type)
                and maxagents_ > {{SEATS}}
                then 1
            else 0
        end as filter_sku
    from sku_seats
    where
        filter_sku = 1
        and account_id is not null
        and state = 'subscribed'
),
cc_model as (
    select
        *,
        row_number() over (
            partition by instance_account_id
            order by source_snapshot_date desc) as rank
    from functional.growth_engine.dim_growth_engine_churn_predictions
    qualify rank = 1
),
custom_price as (
    select distinct account_id
    from DEV_FUNCTIONAL.GROWTH_ANALYTICS.TEMP_CUSTOM_PRICING_TABLE
    where enabled = True
),
--- Logins
logins as (
    select 
        instance_account_id,
        date(max(agent_last_login_timestamp)) as last_login_date
    from propagated_foundational.product_agent_info.dim_agent_emails_bcv
    group by instance_account_id
),
-- Joining all info
joined_info as (
    select
        a.*,
        c.*,
        b.predicted_probability,
        -- Logins
        e.last_login_date,
        datediff('day', e.last_login_date, current_date) as days_since_last_login,
        -- Categorization of M/A billing cycle
        case
            when
                subscription_age_in_days + days_to_subscription_renewal >= 20
                and subscription_age_in_days + days_to_subscription_renewal
                <= 40
                then 'monthly'
            when
                subscription_age_in_days + days_to_subscription_renewal >= 355
                and subscription_age_in_days + days_to_subscription_renewal
                <= 375
                then 'annual'
            else 'other'
        end as billing_cycle,

        -- Code all rules
        -- Numeric
        case
            when
                cast({{NET_ARR_RULE}} as string) = 'all'
                then 1
            when
                net_arr_usd_instance <= {{NET_ARR_RULE}}
                then 1
            else 0
        end as arr_filter,
        case
            when
                
                cast({{DAYS_RENEW_RULE}} as string) = 'all'
                then 1
            when
                days_to_subscription_renewal <= {{DAYS_RENEW_RULE}}
                then 1
            else 0
        end as days_renew_filter,
        case
            when
                cast({{ACCOUNT_AGE_RULE}} as string) = 'all'
                then 1
            when
                account_age_in_days >= {{ACCOUNT_AGE_RULE}}
                then 1
            else 0
        end as account_age_filter,
        case
            when
                cast({{PROBABILITY_RULE}} as string) = 'all'
                then 1
            when
                b.predicted_probability >= {{PROBABILITY_RULE}}
                then 1
            else 0
        end as proba_filter,
        -- Categorical
        case
            when
                {{CURRENCY_RULE}} = 'all'
                then 1
            when
                lower(currency) = {{CURRENCY_RULE}}
                then 1
            else 0
        end as currency_filter,
        case
            when
                {{BILLING_RULE}} = 'all'
                then 1
            when
                lower(billing_cycle) = {{BILLING_RULE}}
                then 1
            else 0
        end as billing_filter,
        case
            when
                {{MKT_SEGMENT_RULE}} = 'all'
                then 1
            when
                lower(market_segment) = {{MKT_SEGMENT_RULE}}
                then 1
            else 0
        end as mkt_segment_filter,
        case
            when
                {{SALES_MODEL_RULE}} = 'all'
                then 1
            when
                lower(sales_model) = {{SALES_MODEL_RULE}}
                then 1
            else 0
        end as sales_model_filter,
        case
            when
                {{DUNNING_RULE}} = 'all'
                then 1
            when
                lower(dunning_state) = {{DUNNING_RULE}}
                then 1
            else 0
        end as dunning_filter,
        -- Custom price filter
        case
        when d.account_id is null
            then 1
        else 0
        end as custom_price_filter,
        -- Account state
        case
            when
                {{ACCOUNT_STATE_RULE}} = 'all'
                then 1
            when
                {{ACCOUNT_STATE_RULE}} = 'null'
                and account_state is null
                then 1
            when
                lower(account_state) = {{ACCOUNT_STATE_RULE}}
                then 1
            else 0
        end as account_state_filter,
        -- Account category
        case
            when
                {{ACCOUNT_CATEGORY_RULE}} = 'all'
                then 1
            when
                {{ACCOUNT_CATEGORY_RULE}} = 'null'
                and account_category is null
                then 1
            when
                lower(account_category) = {{ACCOUNT_CATEGORY_RULE}}
                then 1
            else 0
        end as account_category_filter,
        -- Login case
        case
            when
                cast({{LOGIN_RULE}} as string) = 'all'
                then 1
            when
                days_since_last_login <= {{LOGIN_RULE}}
                then 1
            else 0
        end as login_filter

    from functional.growth_engine.dim_growth_engine_customer_accounts as a
        left join cc_model as b
            on a.zendesk_account_id = b.instance_account_id
        left join sku_rules as c
            on a.zendesk_account_id = c.account_id
        left join custom_price as d
            on a.zendesk_account_id = d.account_id
        left join logins as e
            on a.zendesk_account_id = e.instance_account_id
    where a.is_trial = false
),

funnel as (
    select 
        count(*) tot_obs,
        sum(custom_price_filter) custom_price_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 then 1 else 0 end) sales_model_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 then 1 else 0 end) mkt_segment_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 then 1 else 0 end) billing_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 then 1 else 0 end) filter_sku,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 then 1 else 0 end) currency_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 then 1 else 0 end) account_age_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 and days_renew_filter = 1 then 1 else 0 end) days_renew_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 and days_renew_filter = 1 and arr_filter = 1 then 1 else 0 end) arr_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 and days_renew_filter = 1 and arr_filter = 1 and proba_filter = 1 then 1 else 0 end) proba_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 and days_renew_filter = 1 and arr_filter = 1 and proba_filter = 1 and login_filter = 1 then 1 else 0 end) login_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 and days_renew_filter = 1 and arr_filter = 1 and login_filter = 1 and proba_filter = 1 and dunning_filter = 1 then 1 else 0 end) dunning_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 and days_renew_filter = 1 and arr_filter = 1 and login_filter = 1 and proba_filter = 1 and dunning_filter = 1 
        and account_state_filter = 1 then 1 else 0 end) account_state_filter,
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 and days_renew_filter = 1 and arr_filter = 1 and login_filter = 1 and proba_filter = 1 and dunning_filter = 1 
        and account_state_filter = 1 and account_category_filter = 1 then 1 else 0 end) account_category_filter
    from joined_info
)

select *
from funnel


