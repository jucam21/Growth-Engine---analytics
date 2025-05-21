-- Structure explanation
/*
Filter by searching "Query 1" or "Query 2":
    - Query 1: Sizing of each offers categories
    - Query 2: Select specific population that satisfies filter
 */

-- Test sandbox
create table sandbox.juan_salgado.test_jc as
select *
from foundational.customer.entity_mapping_daily_snapshot_bcv
limit 10


-- Analyzing duplicates
-- Around 300 duplicates

select
count(*) as tot_obs,
count(distinct crm_account_id) as crm_account_id,
count(distinct zendesk_account_id) as zendesk_account_id
from sandbox.juan_salgado.growth_engine_phase0_tmp

    -- Similar to use this table
select 
count(*) tot_obs,
count(distinct crm_account_id) crm_account_id,
count(distinct zendesk_account_id) zendesk_account_id,
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS


TOT_OBS	CRM_ACCOUNT_ID	ZENDESK_ACCOUNT_ID
140698	109042	140315

TOT_OBS	CRM_ACCOUNT_ID	ZENDESK_ACCOUNT_ID
140734	109097	140351


select zendesk_account_id, count(*)
from sandbox.juan_salgado.growth_engine_phase0_tmp
group by 1
order by 2 desc
limit 10

    -- Some duplicates because of Pod id
select *
from sandbox.juan_salgado.growth_engine_phase0_tmp
where zendesk_account_id = 19999438


select zendesk_account_id, pod_id, count(*)
from sandbox.juan_salgado.growth_engine_phase0_tmp
group by 1,2
order by 3 desc
limit 10

    -- Some because CRM id is null
select *
from sandbox.juan_salgado.growth_engine_phase0_tmp
where zendesk_account_id = 10146523



select *
from foundational.finance.fact_recurring_revenue_bcv_enriched as finance
where crm_account_id = '0018000001BbZ0aAAF'



select *
from sandbox.juan_salgado.growth_engine_phase0_tmp
where zendesk_account_id = 1977708



select *
from foundational.finance.fact_recurring_revenue_bcv_enriched as finance
where crm_account_id = '0018000001BbZ0aAAF'






-------------------------------------------------

---- Using ZDP table & joining C/C data

    -- Only last record
select SOURCE_SNAPSHOT_DATE, count(*)
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
group by 1



select *
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
where predicted_risk = 'RED'
limit 10


    -- Comparing predicted_probability vs predicted_probability_calibrared. Calibrated probs are generally lower than predicted_probability
select 
min(predicted_probability) predicted_probability_1,
max(predicted_probability) predicted_probability_2,
min(predicted_probability_calibrated) predicted_probability_3,
max(predicted_probability_calibrated) predicted_probability_4,
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS


select predicted_risk, 
count(*) tot_obs,
avg(predicted_probability) m1,
avg(predicted_probability_calibrated) m2
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
group by 1

    -- Some "GREEN" & "YELLOW" accounts with prob higher than 80%
select predicted_risk, 
count(*) tot_obs,
avg(predicted_probability) m1,
avg(predicted_probability_calibrated) m2
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
where predicted_probability >= 0.8
group by 1








-- Joining C/C model & GE data


    -- Adjusting SKU query

with non_cancelled_expired_skus as (
    select 
        account_id,
        name,
        boost,
        state,
        updated_at,
        parse_json(plan_settings) as parsed_plan_settings,
        parse_json(plan_settings):plan:value::string as plan_type,
        parse_json(plan_settings):maxAgents:value::string as maxAgents_
    from propagated_formatted.accountsdb.skus
    --where state not in ('cancelled', 'expired')
),

sku_seats as (
    select
        *,
        row_number() over (
            partition by account_id,name,state
            order by updated_at desc
        ) as rank
    from non_cancelled_expired_skus
    qualify rank = 1
)





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
)

, sku_rules as (
    select 
        account_id,
            -- Support SKUs
        max(case
            when 
                lower(name) like '%support%'
                and lower(plan_type) like '%team%'
            then 1 else 0
        end) as support_team_flag,
        max(case
            when 
                lower(name) like '%support%'
                and lower(plan_type) like '%professional%'
            then 1 else 0
        end) as support_professional_flag,
        max(case
            when 
                lower(name) like '%support%'
                and lower(plan_type) like '%enterprise%'
            then 1 else 0
        end) as support_enterprise_flag,
            -- Zendesk Suite SKUs
        max(case
            when 
                lower(name) like '%zendesk%'
                and lower(name) like '%suite%'
                and lower(plan_type) like '%growth%'
            then 1 else 0
        end) as zs_growth_flag,
        max(case
            when 
                lower(name) like '%zendesk%'
                and lower(name) like '%suite%'
                and lower(plan_type) like '%professional%'
            then 1 else 0
        end) as zs_professional_flag,
        max(case
            when 
                lower(name) like '%zendesk%'
                and lower(name) like '%suite%'
                and lower(plan_type) like '%team%'
            then 1 else 0
        end) as zs_team_flag,
    from sku_seats
    where state = 'subscribed'
    --where state not in ('cancelled', 'expired')
    group by account_id
)


    -- Some accounts have multiple SKUs
select *
from sku_rules
where support_team_flag + support_professional_flag + support_enterprise_flag + zs_growth_flag + zs_team_flag + zs_professional_flag > 1
limit 10;


select *
from sku_rules
where account_id = 18025989


select *
from sku_seats
where account_id = 18025989



select *
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS a
where zendesk_account_id =  18025989


select *
from foundational.finance.fact_recurring_revenue_bcv_enriched as finance
where crm_account_id = '00180000013hzMZAAY'


select *
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS
where crm_account_id = '00180000013hzMZAAY'




    -- Currency query. Monthly/annual can be measured directly

with curr as (
    select 
        distinct
        mapping.instance_account_id,
        finance.currency
    from foundational.finance.fact_recurring_revenue_bcv_enriched as finance
    left join foundational.customer.entity_mapping_daily_snapshot_bcv as mapping
        on finance.billing_account_id = mapping.billing_account_id
    where mapping.instance_account_id is not null
)

select instance_account_id, count(*)
from curr
group by 1
order by 2 desc
limit 10




    --- C/C model exploration
with cc_model as (
    select 
        *,
        row_number() over (
            partition by instance_account_id
            order by SOURCE_SNAPSHOT_DATE desc
        ) as rank
    from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
    qualify rank = 1
)

select count(*)
from cc_model
where predicted_probability >= 0.8


select SOURCE_SNAPSHOT_DATE,count(*)
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
where predicted_probability >= 0.8
group by 1

    -- C/C model from multiple days
select SOURCE_SNAPSHOT_DATE, count(*)
from cc_model
group by 1

    -- Not duplicated instances
select instance_account_id, count(*)
from cc_model
group by 1
order by 2 desc
limit 10




select 
    SOURCE_SNAPSHOT_DATE,
    count(*) tot_obs,
    sum(case when predicted_probability >= 0.8 then 1 else 0 end) plus_80
from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
group by 1





select 
    SOURCE_SNAPSHOT_DATE,
    count(*) tot_obs,
    sum(case when predicted_probability >= 0.8 then 1 else 0 end) plus_80
from FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
group by 1




select enabled, count(*)
from DEV_FUNCTIONAL.GROWTH_ANALYTICS.TEMP_CUSTOM_PRICING_TABLE
group by 1



----------------------------------------
---- Query 1: Sizing of each offers categories
    
    -- Selecting last record in case more dates are added
with cc_model as (
    select 
        *,
        row_number() over (
            partition by instance_account_id
            order by SOURCE_SNAPSHOT_DATE desc
        ) as rank
    from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CHURN_PREDICTIONS
    qualify rank = 1
),

-- Using SKU query because is easier to use

sku_seats as (
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
)

, sku_rules as (
    select 
        account_id,
        --- SKUs combinations with less or equal than 5 seats
            -- Support SKUs
        max(case
            when 
                lower(name) like '%support%'
                and lower(plan_type) like '%team%'
                and maxAgents_ <= 5
            then 1 else 0
        end) as support_team_less5,
        max(case
            when 
                lower(name) like '%support%'
                and lower(plan_type) like '%professional%'
                and maxAgents_ <= 5
            then 1 else 0
        end) as support_professional_less5,
        max(case
            when 
                lower(name) like '%support%'
                and lower(plan_type) like '%enterprise%'
                and maxAgents_ <= 5
            then 1 else 0
        end) as support_enterprise_less5,
            -- Zendesk Suite SKUs
        max(case
            when 
                lower(name) like '%zendesk%'
                and lower(name) like '%suite%'
                and lower(plan_type) like '%growth%'
                and maxAgents_ <= 5
            then 1 else 0
        end) as zs_growth_less5,
        max(case
            when 
                lower(name) like '%zendesk%'
                and lower(name) like '%suite%'
                and lower(plan_type) like '%professional%'
                and maxAgents_ <= 5
            then 1 else 0
        end) as zs_professional_less5,
        max(case
            when 
                lower(name) like '%zendesk%'
                and lower(name) like '%suite%'
                and lower(plan_type) like '%team%'
                and maxAgents_ <= 5
            then 1 else 0
        end) as zs_team_less5,
        
        --- SKUs combinations with more (strict) than 5 seats
            -- Support SKUs
        max(case
            when 
                lower(name) like '%support%'
                and lower(plan_type) like '%team%'
                and maxAgents_ > 5
            then 1 else 0
        end) as support_team_plus5,
        max(case
            when 
                lower(name) like '%support%'
                and lower(plan_type) like '%professional%'
                and maxAgents_ > 5
            then 1 else 0
        end) as support_professional_plus5,
        max(case
            when 
                lower(name) like '%support%'
                and lower(plan_type) like '%enterprise%'
                and maxAgents_ > 5
            then 1 else 0
        end) as support_enterprise_plus5,
            -- Zendesk Suite SKUs
        max(case
            when 
                lower(name) like '%zendesk%'
                and lower(name) like '%suite%'
                and lower(plan_type) like '%growth%'
                and maxAgents_ > 5
            then 1 else 0
        end) as zs_growth_plus5,
        max(case
            when 
                lower(name) like '%zendesk%'
                and lower(name) like '%suite%'
                and lower(plan_type) like '%professional%'
                and maxAgents_ > 5
            then 1 else 0
        end) as zs_professional_plus5,
        max(case
            when 
                lower(name) like '%zendesk%'
                and lower(name) like '%suite%'
                and lower(plan_type) like '%team%'
                and maxAgents_ > 5
            then 1 else 0
        end) as zs_team_plus5,
    from sku_seats
    where state = 'subscribed'
    --where state not in ('cancelled', 'expired')
    group by account_id
),
        
    -- Joining all info
joined_info as (
    select 
        a.*,
        b.predicted_probability,
        c.*,
        a.currency billing_currency,
            -- Categorization of M/A billing cycle
        case 
            when SUBSCRIPTION_AGE_IN_DAYS + DAYS_TO_SUBSCRIPTION_RENEWAL >= 20 and SUBSCRIPTION_AGE_IN_DAYS + DAYS_TO_SUBSCRIPTION_RENEWAL <= 40 then 'monthly' 
            when SUBSCRIPTION_AGE_IN_DAYS + DAYS_TO_SUBSCRIPTION_RENEWAL >= 355 and SUBSCRIPTION_AGE_IN_DAYS + DAYS_TO_SUBSCRIPTION_RENEWAL <= 375 then 'annual'
            else 'other' 
        end as billing_cycle,
            -- For C/C model
        'churn' as predicted_type
    
    from DEV_FUNCTIONAL.GROWTH_ENGINE.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS a
    left join cc_model b
        on a.zendesk_account_id = b.instance_account_id
    left join sku_rules c
        on a.zendesk_account_id = c.account_id
),

-- Code each rule
main_offers as (
    select
        *,
            -- Offers pasted directly from GSheet
        case when predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_1,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_2,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_3,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_4,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_5,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_6,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_7,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_8,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_9,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_10,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_11,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_12,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_13,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_14,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_15,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_16,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_17,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_18,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_19,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_20,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_21,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_22,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_23,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_24,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_25,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_26,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_27,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_28,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_29,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_30,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_31,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_32,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_33,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_34,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_35,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_36,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_37,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_38,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_39,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_40,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_41,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_42,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_43,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_44,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_45,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_46,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_47,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_48,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_49,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_50,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_51,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_52,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_53,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_54,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_55,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_56,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_57,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_58,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_59,


    -- Adding final conditions to offers
    -- Instances count    
0 as of_1_final,
case when of_2 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_less5 = 1 then 1 else 0 end as of_2_final,
case when of_3 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_3_final,
case when of_4 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_4_final,
case when of_5 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_5_final,
case when of_6 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_6_final,
case when of_7 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_less5 = 1 then 1 else 0 end as of_7_final,
case when of_8 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_8_final,
case when of_9 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_9_final,
case when of_10 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_10_final,
case when of_11 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_11_final,
case when of_12 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_less5 = 1 then 1 else 0 end as of_12_final,
case when of_13 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_13_final,
case when of_14 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_14_final,
case when of_15 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_15_final,
case when of_16 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_16_final,
case when of_17 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_less5 = 1 then 1 else 0 end as of_17_final,
case when of_18 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_18_final,
case when of_19 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_19_final,
case when of_20 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_20_final,
case when of_21 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_21_final,
case when of_22 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_less5 = 1 then 1 else 0 end as of_22_final,
case when of_23 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_23_final,
case when of_24 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_24_final,
case when of_25 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_25_final,
case when of_26 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_26_final,
case when of_27 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_less5 = 1 then 1 else 0 end as of_27_final,
case when of_28 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_28_final,
case when of_29 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_29_final,
case when of_30 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_30_final,
case when of_31 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_31_final,
0 as of_32_final,
case when of_33 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_less5 = 1 then 1 else 0 end as of_33_final,
case when of_34 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_34_final,
case when of_35 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_35_final,
case when of_36 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_36_final,
case when of_37 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_37_final,
case when of_38 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_less5 = 1 then 1 else 0 end as of_38_final,
case when of_39 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_39_final,
case when of_40 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_40_final,
case when of_41 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_41_final,
case when of_42 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_42_final,
case when of_43 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_less5 = 1 then 1 else 0 end as of_43_final,
case when of_44 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_44_final,
case when of_45 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_45_final,
case when of_46 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_46_final,
case when of_47 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_47_final,
case when of_48 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_less5 = 1 then 1 else 0 end as of_48_final,
case when of_49 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_49_final,
case when of_50 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_50_final,
case when of_51 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_51_final,
case when of_52 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_52_final,
case when of_53 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_less5 = 1 then 1 else 0 end as of_53_final,
case when of_54 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_54_final,
case when of_55 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_55_final,
case when of_56 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_56_final,
case when of_57 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_57_final,
case when of_58 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_less5 = 1 then 1 else 0 end as of_58_final,
case when of_59 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_59_final,

    -- ARR sum
0 as of_1_final_arr,
case when of_2 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_2_final_arr,
case when of_3 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_3_final_arr,
case when of_4 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_4_final_arr,
case when of_5 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_5_final_arr,
case when of_6 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_6_final_arr,
case when of_7 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_7_final_arr,
case when of_8 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_8_final_arr,
case when of_9 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_9_final_arr,
case when of_10 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_10_final_arr,
case when of_11 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_11_final_arr,
case when of_12 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_12_final_arr,
case when of_13 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_13_final_arr,
case when of_14 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_14_final_arr,
case when of_15 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_15_final_arr,
case when of_16 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_16_final_arr,
case when of_17 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_17_final_arr,
case when of_18 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_18_final_arr,
case when of_19 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_19_final_arr,
case when of_20 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_20_final_arr,
case when of_21 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_21_final_arr,
case when of_22 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_22_final_arr,
case when of_23 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_23_final_arr,
case when of_24 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_24_final_arr,
case when of_25 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_25_final_arr,
case when of_26 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_26_final_arr,
case when of_27 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_27_final_arr,
case when of_28 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_28_final_arr,
case when of_29 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_29_final_arr,
case when of_30 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_30_final_arr,
case when of_31 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_31_final_arr,
0 as of_32_final_arr,
case when of_33 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_33_final_arr,
case when of_34 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_34_final_arr,
case when of_35 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_35_final_arr,
case when of_36 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_36_final_arr,
case when of_37 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_37_final_arr,
case when of_38 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_38_final_arr,
case when of_39 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_39_final_arr,
case when of_40 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_40_final_arr,
case when of_41 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_41_final_arr,
case when of_42 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_42_final_arr,
case when of_43 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_43_final_arr,
case when of_44 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_44_final_arr,
case when of_45 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_45_final_arr,
case when of_46 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_46_final_arr,
case when of_47 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_47_final_arr,
case when of_48 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_48_final_arr,
case when of_49 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_49_final_arr,
case when of_50 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_50_final_arr,
case when of_51 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_51_final_arr,
case when of_52 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_52_final_arr,
case when of_53 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_53_final_arr,
case when of_54 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_54_final_arr,
case when of_55 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_55_final_arr,
case when of_56 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_56_final_arr,
case when of_57 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_57_final_arr,
case when of_58 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_58_final_arr,
case when of_59 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_59_final_arr
        
    from joined_info
),

main_agg as (
    select 
        -- Instances AGGs
    sum(of_1_final) as of_1_final,
sum(of_2_final) as of_2_final,
sum(of_3_final) as of_3_final,
sum(of_4_final) as of_4_final,
sum(of_5_final) as of_5_final,
sum(of_6_final) as of_6_final,
sum(of_7_final) as of_7_final,
sum(of_8_final) as of_8_final,
sum(of_9_final) as of_9_final,
sum(of_10_final) as of_10_final,
sum(of_11_final) as of_11_final,
sum(of_12_final) as of_12_final,
sum(of_13_final) as of_13_final,
sum(of_14_final) as of_14_final,
sum(of_15_final) as of_15_final,
sum(of_16_final) as of_16_final,
sum(of_17_final) as of_17_final,
sum(of_18_final) as of_18_final,
sum(of_19_final) as of_19_final,
sum(of_20_final) as of_20_final,
sum(of_21_final) as of_21_final,
sum(of_22_final) as of_22_final,
sum(of_23_final) as of_23_final,
sum(of_24_final) as of_24_final,
sum(of_25_final) as of_25_final,
sum(of_26_final) as of_26_final,
sum(of_27_final) as of_27_final,
sum(of_28_final) as of_28_final,
sum(of_29_final) as of_29_final,
sum(of_30_final) as of_30_final,
sum(of_31_final) as of_31_final,
sum(of_32_final) as of_32_final,
sum(of_33_final) as of_33_final,
sum(of_34_final) as of_34_final,
sum(of_35_final) as of_35_final,
sum(of_36_final) as of_36_final,
sum(of_37_final) as of_37_final,
sum(of_38_final) as of_38_final,
sum(of_39_final) as of_39_final,
sum(of_40_final) as of_40_final,
sum(of_41_final) as of_41_final,
sum(of_42_final) as of_42_final,
sum(of_43_final) as of_43_final,
sum(of_44_final) as of_44_final,
sum(of_45_final) as of_45_final,
sum(of_46_final) as of_46_final,
sum(of_47_final) as of_47_final,
sum(of_48_final) as of_48_final,
sum(of_49_final) as of_49_final,
sum(of_50_final) as of_50_final,
sum(of_51_final) as of_51_final,
sum(of_52_final) as of_52_final,
sum(of_53_final) as of_53_final,
sum(of_54_final) as of_54_final,
sum(of_55_final) as of_55_final,
sum(of_56_final) as of_56_final,
sum(of_57_final) as of_57_final,
sum(of_58_final) as of_58_final,
sum(of_59_final) as of_59_final,

    -- ARR aggs
sum(of_1_final_arr) as of_1_final_arr,
sum(of_2_final_arr) as of_2_final_arr,
sum(of_3_final_arr) as of_3_final_arr,
sum(of_4_final_arr) as of_4_final_arr,
sum(of_5_final_arr) as of_5_final_arr,
sum(of_6_final_arr) as of_6_final_arr,
sum(of_7_final_arr) as of_7_final_arr,
sum(of_8_final_arr) as of_8_final_arr,
sum(of_9_final_arr) as of_9_final_arr,
sum(of_10_final_arr) as of_10_final_arr,
sum(of_11_final_arr) as of_11_final_arr,
sum(of_12_final_arr) as of_12_final_arr,
sum(of_13_final_arr) as of_13_final_arr,
sum(of_14_final_arr) as of_14_final_arr,
sum(of_15_final_arr) as of_15_final_arr,
sum(of_16_final_arr) as of_16_final_arr,
sum(of_17_final_arr) as of_17_final_arr,
sum(of_18_final_arr) as of_18_final_arr,
sum(of_19_final_arr) as of_19_final_arr,
sum(of_20_final_arr) as of_20_final_arr,
sum(of_21_final_arr) as of_21_final_arr,
sum(of_22_final_arr) as of_22_final_arr,
sum(of_23_final_arr) as of_23_final_arr,
sum(of_24_final_arr) as of_24_final_arr,
sum(of_25_final_arr) as of_25_final_arr,
sum(of_26_final_arr) as of_26_final_arr,
sum(of_27_final_arr) as of_27_final_arr,
sum(of_28_final_arr) as of_28_final_arr,
sum(of_29_final_arr) as of_29_final_arr,
sum(of_30_final_arr) as of_30_final_arr,
sum(of_31_final_arr) as of_31_final_arr,
sum(of_32_final_arr) as of_32_final_arr,
sum(of_33_final_arr) as of_33_final_arr,
sum(of_34_final_arr) as of_34_final_arr,
sum(of_35_final_arr) as of_35_final_arr,
sum(of_36_final_arr) as of_36_final_arr,
sum(of_37_final_arr) as of_37_final_arr,
sum(of_38_final_arr) as of_38_final_arr,
sum(of_39_final_arr) as of_39_final_arr,
sum(of_40_final_arr) as of_40_final_arr,
sum(of_41_final_arr) as of_41_final_arr,
sum(of_42_final_arr) as of_42_final_arr,
sum(of_43_final_arr) as of_43_final_arr,
sum(of_44_final_arr) as of_44_final_arr,
sum(of_45_final_arr) as of_45_final_arr,
sum(of_46_final_arr) as of_46_final_arr,
sum(of_47_final_arr) as of_47_final_arr,
sum(of_48_final_arr) as of_48_final_arr,
sum(of_49_final_arr) as of_49_final_arr,
sum(of_50_final_arr) as of_50_final_arr,
sum(of_51_final_arr) as of_51_final_arr,
sum(of_52_final_arr) as of_52_final_arr,
sum(of_53_final_arr) as of_53_final_arr,
sum(of_54_final_arr) as of_54_final_arr,
sum(of_55_final_arr) as of_55_final_arr,
sum(of_56_final_arr) as of_56_final_arr,
sum(of_57_final_arr) as of_57_final_arr,
sum(of_58_final_arr) as of_58_final_arr,
sum(of_59_final_arr) as of_59_final_arr
from main_offers
)


select *
from main_offers
where of_27_final >= 1;






select *
from main_offers
where of_1_final + 
of_2_final + 
of_3_final + 
of_4_final + 
of_5_final + 
of_6_final + 
of_7_final + 
of_8_final + 
of_9_final + 
of_10_final + 
of_11_final + 
of_12_final + 
of_13_final + 
of_14_final + 
of_15_final + 
of_16_final + 
of_17_final + 
of_18_final + 
of_19_final + 
of_20_final + 
of_21_final + 
of_22_final + 
of_23_final + 
of_24_final + 
of_25_final + 
of_26_final + 
of_27_final + 
of_28_final + 
of_29_final + 
of_30_final + 
of_31_final + 
of_32_final + 
of_33_final + 
of_34_final + 
of_35_final + 
of_36_final + 
of_37_final + 
of_38_final + 
of_39_final + 
of_40_final + 
of_41_final + 
of_42_final + 
of_43_final + 
of_44_final + 
of_45_final + 
of_46_final + 
of_47_final + 
of_48_final + 
of_49_final + 
of_50_final + 
of_51_final + 
of_52_final + 
of_53_final + 
of_54_final + 
of_55_final + 
of_56_final + 
of_57_final + 
of_58_final + 
of_59_final >= 1;


select
    count(*) as tot_obs,
    count(distinct zendesk_account_id) as zendesk_account_id,

    sum(of_1) as of_1,
    sum(of_2) as of_2,
    sum(of_3) as of_3,
    sum(of_4) as of_4,
    sum(of_5) as of_5,
    sum(of_6) as of_6,
    sum(of_7) as of_7
from main_offers




select *
from main_agg






select 
count(*) tot_obs,
count(distinct crm_account_id) crm_account_id,
count(distinct zendesk_account_id) zendesk_account_id,
from joined_info













-------------------------------------------------
---- Query 2: Filter specific accounts


-- Create text variable with SKUs combination
-- Fixed to pre-defined SKUs & seats

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
        -- Define variables to test them
        'all' as SKU_NAME,
        'professional' as PLAN_TYPE,
        '<=' as OPERATOR,
        5 as SEATS,
        case 
            when 
                SKU_NAME = 'all' 
                or PLAN_TYPE = 'all' 
                then 1
            when 
                OPERATOR = '<='
                and SKU_NAME like lower(name)
                and PLAN_TYPE like lower(plan_type)
                and maxagents_ <= SEATS
                then 1
            when
                OPERATOR = '>'
                and SKU_NAME like lower(name)
                and PLAN_TYPE like lower(plan_type)
                and maxagents_ > SEATS
                then 1
            else 0
        end as filter_sku
    from sku_seats
    where 
        filter_sku = 1
        and account_id is not null
)

select count(*)
from sku_rules;





----------------------------------------
--- Main Query
-- Selecting last record in case more dates are added

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

        -- Define variables to test them
        'all' as SKU_NAME,
        'professional' as PLAN_TYPE,
        '<=' as OPERATOR,
        5 as SEATS,
        case
            when
                SKU_NAME = 'all'
                or PLAN_TYPE = 'all'
                then 1
            when
                OPERATOR = '<='
                and SKU_NAME like lower(name)
                and PLAN_TYPE like lower(plan_type)
                and maxagents_ <= SEATS
                then 1
            when
                OPERATOR = '>'
                and SKU_NAME like lower(name)
                and PLAN_TYPE like lower(plan_type)
                and maxagents_ > SEATS
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
    from dev_functional.growth_engine.dim_growth_engine_churn_predictions
    qualify rank = 1
),
custom_price as (
    select distinct account_id
    from DEV_FUNCTIONAL.GROWTH_ANALYTICS.TEMP_CUSTOM_PRICING_TABLE
    where enabled = True
),
-- Joining all info
joined_info as (
    select
        a.*,
        c.*,
        b.predicted_probability,
        -- Define variables to test them
        'all' as NET_ARR_RULE,
        'all' as DAYS_RENEW_RULE,
        'all' as ACCOUNT_AGE_RULE,
        '0.8' as PROBABILITY_RULE,
        'all' as CURRENCY_RULE,
        'all' as BILLING_RULE,
        'digital' as MKT_SEGMENT_RULE,
        'self-service' as SALES_MODEL_RULE,
        'all' as DUNNING_RULE,
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
                try_cast(NET_ARR_RULE as string) = 'all'
                then 1
            when
                net_arr_usd_instance <= try_cast(NET_ARR_RULE as double)
                then 1
            else 0
        end as arr_filter,
        case
            when
                try_cast(DAYS_RENEW_RULE as string) = 'all'
                then 1
            when
                days_to_subscription_renewal <= try_cast(DAYS_RENEW_RULE as double)
                then 1
            else 0
        end as days_renew_filter,
        case
            when
                try_cast(ACCOUNT_AGE_RULE as string) = 'all'
                then 1
            when
                account_age_in_days >= try_cast(ACCOUNT_AGE_RULE as double)
                then 1
            else 0
        end as account_age_filter,
        case
            when
                try_cast(PROBABILITY_RULE as string) = 'all'
                then 1
            when
                b.predicted_probability >= try_cast(PROBABILITY_RULE as double)
                then 1
            else 0
        end as proba_filter,
        -- Categorical
        case
            when
                CURRENCY_RULE = 'all'
                then 1
            when
                lower(currency) = CURRENCY_RULE
                then 1
            else 0
        end as currency_filter,
        case
            when
                BILLING_RULE = 'all'
                then 1
            when
                lower(billing_cycle) = BILLING_RULE
                then 1
            else 0
        end as billing_filter,
        case
            when
                MKT_SEGMENT_RULE = 'all'
                then 1
            when
                lower(market_segment) = MKT_SEGMENT_RULE
                then 1
            else 0
        end as mkt_segment_filter,
        case
            when
                SALES_MODEL_RULE = 'all'
                then 1
            when
                lower(sales_model) = SALES_MODEL_RULE
                then 1
            else 0
        end as sales_model_filter,
        case
            when
                DUNNING_RULE = 'all'
                then 1
            when
                lower(dunning_state) = DUNNING_RULE
                then 1
            else 0
        end as dunning_filter,
        -- Custom price filter
        case
        when d.account_id is null
            then 1
        else 0
        end as custom_price_filter


    from dev_functional.growth_engine.dim_growth_engine_customer_accounts as a
        left join cc_model as b
            on a.zendesk_account_id = b.instance_account_id
        inner join sku_rules as c
            on a.zendesk_account_id = c.account_id
        left join custom_price as d
            on a.zendesk_account_id = d.account_id
    where
        arr_filter = 1
        and days_renew_filter = 1
        and account_age_filter = 1
        and proba_filter = 1
        and currency_filter = 1
        and billing_filter = 1
        and mkt_segment_filter = 1
        and sales_model_filter = 1
        and dunning_filter = 1
        and custom_price_filter = 1
)

select count(*)
from joined_info







select count(*),
count(distinct account_id) as account_id,
    from DEV_FUNCTIONAL.GROWTH_ANALYTICS.TEMP_CUSTOM_PRICING_TABLE
    where enabled = True




----------- Code to Snowflake
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
),
cc_model as (
    select
        *,
        row_number() over (
            partition by instance_account_id
            order by source_snapshot_date desc) as rank
    from dev_functional.growth_engine.dim_growth_engine_churn_predictions
    qualify rank = 1
),
custom_price as (
    select distinct account_id
    from DEV_FUNCTIONAL.GROWTH_ANALYTICS.TEMP_CUSTOM_PRICING_TABLE
    where enabled = True
),
-- Joining all info
joined_info as (
    select
        a.*,
        c.*,
        b.predicted_probability,
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
        end as custom_price_filter


    from dev_functional.growth_engine.dim_growth_engine_customer_accounts as a
        left join cc_model as b
            on a.zendesk_account_id = b.instance_account_id
        inner join sku_rules as c
            on a.zendesk_account_id = c.account_id
        left join custom_price as d
            on a.zendesk_account_id = d.account_id
    where
        arr_filter = 1
        and days_renew_filter = 1
        and account_age_filter = 1
        and proba_filter = 1
        and currency_filter = 1
        and billing_filter = 1
        and mkt_segment_filter = 1
        and sales_model_filter = 1
        and dunning_filter = 1
        and custom_price_filter = 1
),

main as (
    select 
        zendesk_account_id, 
        CRM_ACCOUNT_ID,
        ACCOUNT_CATEGORY,
        ACCOUNT_STATE,
        market_segment, 
        sales_model, 
        predicted_probability, 
        net_arr_usd_instance, 
        account_age_in_days, 
        days_to_subscription_renewal, 
        dunning_state
    from joined_info
)

select count(*)
from main









----------------------------------------
--- Query 3: Funnel with parameters



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

        -- Define variables to test them
        'all' as SKU_NAME,
        'professional' as PLAN_TYPE,
        '<=' as OPERATOR,
        5 as SEATS,
        case
            when
                SKU_NAME = 'all'
                or PLAN_TYPE = 'all'
                then 1
            when
                OPERATOR = '<='
                and SKU_NAME like lower(name)
                and PLAN_TYPE like lower(plan_type)
                and maxagents_ <= SEATS
                then 1
            when
                OPERATOR = '>'
                and SKU_NAME like lower(name)
                and PLAN_TYPE like lower(plan_type)
                and maxagents_ > SEATS
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
    from dev_functional.growth_engine.dim_growth_engine_churn_predictions
    qualify rank = 1
),
custom_price as (
    select distinct account_id
    from DEV_FUNCTIONAL.GROWTH_ANALYTICS.TEMP_CUSTOM_PRICING_TABLE
    where enabled = True
),
-- Joining all info
joined_info as (
    select
        a.*,
        c.*,
        b.predicted_probability,
        -- Define variables to test them
        '25000' as NET_ARR_RULE,
        '10' as DAYS_RENEW_RULE,
        '60' as ACCOUNT_AGE_RULE,
        '0.8' as PROBABILITY_RULE,
        'all' as CURRENCY_RULE,
        'monthly' as BILLING_RULE,
        'digital' as MKT_SEGMENT_RULE,
        'self-service' as SALES_MODEL_RULE,
        'all' as DUNNING_RULE,
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
                try_cast(NET_ARR_RULE as string) = 'all'
                then 1
            when
                net_arr_usd_instance <= try_cast(NET_ARR_RULE as double)
                then 1
            else 0
        end as arr_filter,
        case
            when
                try_cast(DAYS_RENEW_RULE as string) = 'all'
                then 1
            when
                days_to_subscription_renewal <= try_cast(DAYS_RENEW_RULE as double)
                then 1
            else 0
        end as days_renew_filter,
        case
            when
                try_cast(ACCOUNT_AGE_RULE as string) = 'all'
                then 1
            when
                account_age_in_days >= try_cast(ACCOUNT_AGE_RULE as double)
                then 1
            else 0
        end as account_age_filter,
        case
            when
                try_cast(PROBABILITY_RULE as string) = 'all'
                then 1
            when
                b.predicted_probability >= try_cast(PROBABILITY_RULE as number)
                then 1
            else 0
        end as proba_filter,
        -- Categorical
        case
            when
                CURRENCY_RULE = 'all'
                then 1
            when
                lower(currency) = CURRENCY_RULE
                then 1
            else 0
        end as currency_filter,
        case
            when
                BILLING_RULE = 'all'
                then 1
            when
                lower(billing_cycle) = BILLING_RULE
                then 1
            else 0
        end as billing_filter,
        case
            when
                MKT_SEGMENT_RULE = 'all'
                then 1
            when
                lower(market_segment) = MKT_SEGMENT_RULE
                then 1
            else 0
        end as mkt_segment_filter,
        case
            when
                SALES_MODEL_RULE = 'all'
                then 1
            when
                lower(sales_model) = SALES_MODEL_RULE
                then 1
            else 0
        end as sales_model_filter,
        case
            when
                DUNNING_RULE = 'all'
                then 1
            when
                lower(dunning_state) = DUNNING_RULE
                then 1
            else 0
        end as dunning_filter,
        -- Custom price filter
        case
        when d.account_id is null
            then 1
        else 0
        end as custom_price_filter

    from dev_functional.growth_engine.dim_growth_engine_customer_accounts as a
        left join cc_model as b
            on a.zendesk_account_id = b.instance_account_id
        left join sku_rules as c
            on a.zendesk_account_id = c.account_id
        left join custom_price as d
            on a.zendesk_account_id = d.account_id
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
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 and days_renew_filter = 1 and arr_filter = 1 and proba_filter = 1 and dunning_filter = 1 then 1 else 0 end) dunning_filter
    from joined_info
)

select *
from funnel;






--- Query to Snowflake
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
    from dev_functional.growth_engine.dim_growth_engine_churn_predictions
    qualify rank = 1
),
custom_price as (
    select distinct account_id
    from DEV_FUNCTIONAL.GROWTH_ANALYTICS.TEMP_CUSTOM_PRICING_TABLE
    where enabled = True
),
-- Joining all info
joined_info as (
    select
        a.*,
        c.*,
        b.predicted_probability,
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
        end as custom_price_filter


    from dev_functional.growth_engine.dim_growth_engine_customer_accounts as a
        left join cc_model as b
            on a.zendesk_account_id = b.instance_account_id
        left join sku_rules as c
            on a.zendesk_account_id = c.account_id
        left join custom_price as d
            on a.zendesk_account_id = d.account_id
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
        sum(case when custom_price_filter = 1 and sales_model_filter = 1 and mkt_segment_filter = 1 and billing_filter = 1 and filter_sku = 1 and currency_filter = 1 and account_age_filter = 1 and days_renew_filter = 1 and arr_filter = 1 and proba_filter = 1 and dunning_filter = 1 then 1 else 0 end) dunning_filter
    from joined_info
)

select *
from funnel
































select *
from joined_info
where lower(sales_model) = 'self-service'
and lower(market_segment) = 'digital' 
and predicted_probability >= 0.8
and account_age_in_days >= 60
and days_to_subscription_renewal <= 60
and dunning_state != 'OK'



group by 1

and DUNNING_STATE = 'OK'


select distinct currency
from dev_functional.growth_engine.dim_growth_engine_customer_accounts




ACCOUNT_AGE_IN_DAYS

/*

select distinct DUNNING_STATE, count(*)
from joined_info
where lower(sales_model) = 'self-service'
and lower(MARKET_SEGMENT) = 'digital' 
and predicted_probability >= 0.8
and SUBSCRIPTION_AGE_IN_DAYS >= 60
and DAYS_TO_SUBSCRIPTION_RENEWAL <= 60
group by 1
and DUNNING_STATE = 'OK'




select *
from joined_info
where predicted_probability >= 0.8
and SUBSCRIPTION_AGE_IN_DAYS >= 60
and DAYS_TO_SUBSCRIPTION_RENEWAL <= 60
and DUNNING_STATE = 'OK'
and lower(MARKET_SEGMENT) = 'digital' 
and lower(sales_model) = 'self-service'



    -- Understanding why so many zeros
    
select lower(sales_model), count(*)
from joined_info
where predicted_probability >= 0.8
and SUBSCRIPTION_AGE_IN_DAYS >= 60
and DAYS_TO_SUBSCRIPTION_RENEWAL <= 10
and DUNNING_STATE = 'OK'
and lower(MARKET_SEGMENT) = 'digital'
group by 1




select billing_cycle,count(*)
from joined_info
where predicted_probability >= 0.8
and SUBSCRIPTION_AGE_IN_DAYS >= 60
and DAYS_TO_SUBSCRIPTION_RENEWAL <= 60
and DUNNING_STATE = 'OK'
and lower(MARKET_SEGMENT) = 'digital' 
and lower(sales_model) = 'self-service'
group by 1




*/

    -- Code each rule

main_offers as (
    select
        *,
            -- Offers pasted directly from GSheet
        case when predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_1,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_2,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_3,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_4,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_5,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_6,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_7,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_8,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_9,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_10,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_11,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_12,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_13,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_14,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_15,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_16,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_17,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_18,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_19,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_20,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_21,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_22,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_23,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_24,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_25,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_26,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_27,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_28,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_29,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_30,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'monthly' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 10 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_31,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_32,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_33,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_34,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_35,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_36,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_37,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_38,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_39,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_40,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_41,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_42,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_43,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_44,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_45,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_46,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_47,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_48,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_49,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_50,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_51,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_52,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_53,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_54,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'GBP' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_55,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'EUR' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_56,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'BRL' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_57,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_58,
case when NET_ARR_USD_INSTANCE <= 25000 and predicted_type = 'churn' and billing_currency = 'USD' and predicted_probability >= 0.8 and billing_cycle = 'annual' and lower(MARKET_SEGMENT) = 'digital' and lower(sales_model) = 'self-service' and  DAYS_TO_SUBSCRIPTION_RENEWAL <= 60 and ACCOUNT_AGE_IN_DAYS >= 60 then 1 else 0 end as of_59,


    -- Adding final conditions to offers
    -- Instances count    
0 as of_1_final,
case when of_2 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_less5 = 1 then 1 else 0 end as of_2_final,
case when of_3 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_3_final,
case when of_4 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_4_final,
case when of_5 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_5_final,
case when of_6 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_6_final,
case when of_7 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_less5 = 1 then 1 else 0 end as of_7_final,
case when of_8 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_8_final,
case when of_9 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_9_final,
case when of_10 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_10_final,
case when of_11 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_11_final,
case when of_12 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_less5 = 1 then 1 else 0 end as of_12_final,
case when of_13 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_13_final,
case when of_14 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_14_final,
case when of_15 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_15_final,
case when of_16 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_16_final,
case when of_17 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_less5 = 1 then 1 else 0 end as of_17_final,
case when of_18 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_18_final,
case when of_19 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_19_final,
case when of_20 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_20_final,
case when of_21 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_21_final,
case when of_22 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_less5 = 1 then 1 else 0 end as of_22_final,
case when of_23 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_23_final,
case when of_24 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_24_final,
case when of_25 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_25_final,
case when of_26 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_26_final,
case when of_27 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_less5 = 1 then 1 else 0 end as of_27_final,
case when of_28 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_28_final,
case when of_29 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_29_final,
case when of_30 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_30_final,
case when of_31 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_31_final,
0 as of_32_final,
case when of_33 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_less5 = 1 then 1 else 0 end as of_33_final,
case when of_34 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_34_final,
case when of_35 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_35_final,
case when of_36 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_36_final,
case when of_37 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then 1 else 0 end as of_37_final,
case when of_38 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_less5 = 1 then 1 else 0 end as of_38_final,
case when of_39 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_39_final,
case when of_40 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_40_final,
case when of_41 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_41_final,
case when of_42 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then 1 else 0 end as of_42_final,
case when of_43 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_less5 = 1 then 1 else 0 end as of_43_final,
case when of_44 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_44_final,
case when of_45 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_45_final,
case when of_46 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_46_final,
case when of_47 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then 1 else 0 end as of_47_final,
case when of_48 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_less5 = 1 then 1 else 0 end as of_48_final,
case when of_49 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_49_final,
case when of_50 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_50_final,
case when of_51 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_51_final,
case when of_52 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then 1 else 0 end as of_52_final,
case when of_53 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_less5 = 1 then 1 else 0 end as of_53_final,
case when of_54 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_54_final,
case when of_55 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_55_final,
case when of_56 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_56_final,
case when of_57 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then 1 else 0 end as of_57_final,
case when of_58 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_less5 = 1 then 1 else 0 end as of_58_final,
case when of_59 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then 1 else 0 end as of_59_final,

    -- ARR sum
0 as of_1_final_arr,
case when of_2 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_2_final_arr,
case when of_3 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_3_final_arr,
case when of_4 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_4_final_arr,
case when of_5 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_5_final_arr,
case when of_6 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_6_final_arr,
case when of_7 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_7_final_arr,
case when of_8 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_8_final_arr,
case when of_9 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_9_final_arr,
case when of_10 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_10_final_arr,
case when of_11 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_11_final_arr,
case when of_12 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_12_final_arr,
case when of_13 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_13_final_arr,
case when of_14 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_14_final_arr,
case when of_15 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_15_final_arr,
case when of_16 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_16_final_arr,
case when of_17 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_17_final_arr,
case when of_18 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_18_final_arr,
case when of_19 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_19_final_arr,
case when of_20 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_20_final_arr,
case when of_21 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_21_final_arr,
case when of_22 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_22_final_arr,
case when of_23 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_23_final_arr,
case when of_24 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_24_final_arr,
case when of_25 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_25_final_arr,
case when of_26 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_26_final_arr,
case when of_27 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_27_final_arr,
case when of_28 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_28_final_arr,
case when of_29 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_29_final_arr,
case when of_30 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_30_final_arr,
case when of_31 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_31_final_arr,
0 as of_32_final_arr,
case when of_33 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_33_final_arr,
case when of_34 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_34_final_arr,
case when of_35 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_35_final_arr,
case when of_36 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_36_final_arr,
case when of_37 = 1 and NET_ARR_USD_INSTANCE > 0 and support_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_37_final_arr,
case when of_38 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_38_final_arr,
case when of_39 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_39_final_arr,
case when of_40 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_40_final_arr,
case when of_41 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_41_final_arr,
case when of_42 = 1 and NET_ARR_USD_INSTANCE > 0 and support_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_42_final_arr,
case when of_43 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_43_final_arr,
case when of_44 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_44_final_arr,
case when of_45 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_45_final_arr,
case when of_46 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_46_final_arr,
case when of_47 = 1 and NET_ARR_USD_INSTANCE > 0 and support_enterprise_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_47_final_arr,
case when of_48 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_48_final_arr,
case when of_49 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_49_final_arr,
case when of_50 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_50_final_arr,
case when of_51 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_51_final_arr,
case when of_52 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_growth_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_52_final_arr,
case when of_53 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_53_final_arr,
case when of_54 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_54_final_arr,
case when of_55 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_55_final_arr,
case when of_56 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_56_final_arr,
case when of_57 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_professional_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_57_final_arr,
case when of_58 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_less5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_58_final_arr,
case when of_59 = 1 and NET_ARR_USD_INSTANCE > 0 and zs_team_plus5 = 1 then NET_ARR_USD_INSTANCE else 0 end as of_59_final_arr
        
    from joined_info
),

main_agg as (
    select 
        -- Instances AGGs
    sum(of_1_final) as of_1_final,
sum(of_2_final) as of_2_final,
sum(of_3_final) as of_3_final,
sum(of_4_final) as of_4_final,
sum(of_5_final) as of_5_final,
sum(of_6_final) as of_6_final,
sum(of_7_final) as of_7_final,
sum(of_8_final) as of_8_final,
sum(of_9_final) as of_9_final,
sum(of_10_final) as of_10_final,
sum(of_11_final) as of_11_final,
sum(of_12_final) as of_12_final,
sum(of_13_final) as of_13_final,
sum(of_14_final) as of_14_final,
sum(of_15_final) as of_15_final,
sum(of_16_final) as of_16_final,
sum(of_17_final) as of_17_final,
sum(of_18_final) as of_18_final,
sum(of_19_final) as of_19_final,
sum(of_20_final) as of_20_final,
sum(of_21_final) as of_21_final,
sum(of_22_final) as of_22_final,
sum(of_23_final) as of_23_final,
sum(of_24_final) as of_24_final,
sum(of_25_final) as of_25_final,
sum(of_26_final) as of_26_final,
sum(of_27_final) as of_27_final,
sum(of_28_final) as of_28_final,
sum(of_29_final) as of_29_final,
sum(of_30_final) as of_30_final,
sum(of_31_final) as of_31_final,
sum(of_32_final) as of_32_final,
sum(of_33_final) as of_33_final,
sum(of_34_final) as of_34_final,
sum(of_35_final) as of_35_final,
sum(of_36_final) as of_36_final,
sum(of_37_final) as of_37_final,
sum(of_38_final) as of_38_final,
sum(of_39_final) as of_39_final,
sum(of_40_final) as of_40_final,
sum(of_41_final) as of_41_final,
sum(of_42_final) as of_42_final,
sum(of_43_final) as of_43_final,
sum(of_44_final) as of_44_final,
sum(of_45_final) as of_45_final,
sum(of_46_final) as of_46_final,
sum(of_47_final) as of_47_final,
sum(of_48_final) as of_48_final,
sum(of_49_final) as of_49_final,
sum(of_50_final) as of_50_final,
sum(of_51_final) as of_51_final,
sum(of_52_final) as of_52_final,
sum(of_53_final) as of_53_final,
sum(of_54_final) as of_54_final,
sum(of_55_final) as of_55_final,
sum(of_56_final) as of_56_final,
sum(of_57_final) as of_57_final,
sum(of_58_final) as of_58_final,
sum(of_59_final) as of_59_final,

    -- ARR aggs
sum(of_1_final_arr) as of_1_final_arr,
sum(of_2_final_arr) as of_2_final_arr,
sum(of_3_final_arr) as of_3_final_arr,
sum(of_4_final_arr) as of_4_final_arr,
sum(of_5_final_arr) as of_5_final_arr,
sum(of_6_final_arr) as of_6_final_arr,
sum(of_7_final_arr) as of_7_final_arr,
sum(of_8_final_arr) as of_8_final_arr,
sum(of_9_final_arr) as of_9_final_arr,
sum(of_10_final_arr) as of_10_final_arr,
sum(of_11_final_arr) as of_11_final_arr,
sum(of_12_final_arr) as of_12_final_arr,
sum(of_13_final_arr) as of_13_final_arr,
sum(of_14_final_arr) as of_14_final_arr,
sum(of_15_final_arr) as of_15_final_arr,
sum(of_16_final_arr) as of_16_final_arr,
sum(of_17_final_arr) as of_17_final_arr,
sum(of_18_final_arr) as of_18_final_arr,
sum(of_19_final_arr) as of_19_final_arr,
sum(of_20_final_arr) as of_20_final_arr,
sum(of_21_final_arr) as of_21_final_arr,
sum(of_22_final_arr) as of_22_final_arr,
sum(of_23_final_arr) as of_23_final_arr,
sum(of_24_final_arr) as of_24_final_arr,
sum(of_25_final_arr) as of_25_final_arr,
sum(of_26_final_arr) as of_26_final_arr,
sum(of_27_final_arr) as of_27_final_arr,
sum(of_28_final_arr) as of_28_final_arr,
sum(of_29_final_arr) as of_29_final_arr,
sum(of_30_final_arr) as of_30_final_arr,
sum(of_31_final_arr) as of_31_final_arr,
sum(of_32_final_arr) as of_32_final_arr,
sum(of_33_final_arr) as of_33_final_arr,
sum(of_34_final_arr) as of_34_final_arr,
sum(of_35_final_arr) as of_35_final_arr,
sum(of_36_final_arr) as of_36_final_arr,
sum(of_37_final_arr) as of_37_final_arr,
sum(of_38_final_arr) as of_38_final_arr,
sum(of_39_final_arr) as of_39_final_arr,
sum(of_40_final_arr) as of_40_final_arr,
sum(of_41_final_arr) as of_41_final_arr,
sum(of_42_final_arr) as of_42_final_arr,
sum(of_43_final_arr) as of_43_final_arr,
sum(of_44_final_arr) as of_44_final_arr,
sum(of_45_final_arr) as of_45_final_arr,
sum(of_46_final_arr) as of_46_final_arr,
sum(of_47_final_arr) as of_47_final_arr,
sum(of_48_final_arr) as of_48_final_arr,
sum(of_49_final_arr) as of_49_final_arr,
sum(of_50_final_arr) as of_50_final_arr,
sum(of_51_final_arr) as of_51_final_arr,
sum(of_52_final_arr) as of_52_final_arr,
sum(of_53_final_arr) as of_53_final_arr,
sum(of_54_final_arr) as of_54_final_arr,
sum(of_55_final_arr) as of_55_final_arr,
sum(of_56_final_arr) as of_56_final_arr,
sum(of_57_final_arr) as of_57_final_arr,
sum(of_58_final_arr) as of_58_final_arr,
sum(of_59_final_arr) as of_59_final_arr
from main_offers
)


select *
from main_offers
where of_1_final + 
of_2_final + 
of_3_final + 
of_4_final + 
of_5_final + 
of_6_final + 
of_7_final + 
of_8_final + 
of_9_final + 
of_10_final + 
of_11_final + 
of_12_final + 
of_13_final + 
of_14_final + 
of_15_final + 
of_16_final + 
of_17_final + 
of_18_final + 
of_19_final + 
of_20_final + 
of_21_final + 
of_22_final + 
of_23_final + 
of_24_final + 
of_25_final + 
of_26_final + 
of_27_final + 
of_28_final + 
of_29_final + 
of_30_final + 
of_31_final + 
of_32_final + 
of_33_final + 
of_34_final + 
of_35_final + 
of_36_final + 
of_37_final + 
of_38_final + 
of_39_final + 
of_40_final + 
of_41_final + 
of_42_final + 
of_43_final + 
of_44_final + 
of_45_final + 
of_46_final + 
of_47_final + 
of_48_final + 
of_49_final + 
of_50_final + 
of_51_final + 
of_52_final + 
of_53_final + 
of_54_final + 
of_55_final + 
of_56_final + 
of_57_final + 
of_58_final + 
of_59_final >= 1;


select
    count(*) as tot_obs,
    count(distinct zendesk_account_id) as zendesk_account_id,

    sum(of_1) as of_1,
    sum(of_2) as of_2,
    sum(of_3) as of_3,
    sum(of_4) as of_4,
    sum(of_5) as of_5,
    sum(of_6) as of_6,
    sum(of_7) as of_7
from main_offers




select *
from main_agg






select 
count(*) tot_obs,
count(distinct crm_account_id) crm_account_id,
count(distinct zendesk_account_id) zendesk_account_id,
from joined_info




