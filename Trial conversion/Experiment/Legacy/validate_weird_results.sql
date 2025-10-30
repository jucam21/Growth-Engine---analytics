
------ Checking why this calculation returns null for some accounts
------ It seems the filters are removing customers that we should not remove

with trial_expiry as (
    select
        instance_account_id,
        instance_account_is_trial,
        instance_account_trial_expires_on,
        current_date,
        instance_account_trial_expires_on,
        instance_account_created_timestamp,
        -- Days to trial expiry
        datediff('day', current_date, instance_account_trial_expires_on) as days_to_expiry,
        -- Trial account age
        datediff('day', date(instance_account_created_timestamp), current_date) as trial_age
    from
        foundational.customer.dim_instance_accounts_daily_snapshot_bcv
    where instance_account_id in (
        9616711,
        19628646,
        15951963,
        21504822)
    --instance_account_is_trial = True
    --and days_to_expiry > 0
)

select *
from
    trial_expiry
limit 10


select *
from FEATURE_PUFFINS614.DEV.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS
where trial_age is null
and is_trial = True and account_category != 'Internal Instance'
limit 10

select 
count(*) total_web_form_tickets,
count(case when trial_age is null then 1 end) as web_form_tickets_no_trial_age,
from FEATURE_PUFFINS614.DEV.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS
where is_trial = True and account_category != 'Internal Instance'


--- Checking NULL sku trial name

select 
count(*) tot_obs,
count(case when trial_age is null then 1 end) as trial_age,
count(case when trial_sku_names is null then 1 end) as trial_sku_names,
from FEATURE_PUFFINS614.DEV.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS
where is_trial = True and account_category != 'Internal Instance'



select *
from FEATURE_PUFFINS614.DEV.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS
where trial_sku_names is null
and is_trial = True and account_category != 'Internal Instance'
limit 10








-- Selects skus that are relevant for trial accounts
with ranked_skus as (
    select
        instance_account_id,
        created_timestamp,
        sku_name,
        sku_state,
        lag(created_timestamp) over (partition by instance_account_id order by created_timestamp) as prev_timestamp
    from
        propagated_cleansed.product_accounts.base_skus_scd2
    where
        sku_name in ('sell_suite', 'sell', 'support', 'zendesk_suite', 'suite')
        and sku_state = 'trial'
),

-- Deduplicate trial skus
ranked_trials as (
    select
        instance_account_id,
        created_timestamp,
        sku_name,
        sku_state
    from
        ranked_skus
    -- assume it's the same trial if created < 5 sec apart#}
    qualify
        dense_rank()
            over (
                partition by
                    instance_account_id
                order by
                    case
                        when prev_timestamp is null then created_timestamp
                        when datediff(second, prev_timestamp, created_timestamp) <= 5 then prev_timestamp
                        else created_timestamp
                    end
            )
        = 1
),

-- Aggregate trial types by instance_account_id
import_trial_types as (
    select
        instance_account_id,
        listagg(distinct sku_name, ',') within group (order by sku_name) as trial_sku_names,
        listagg(distinct sku_state, ',') within group (order by sku_state) as trial_sku_states
    from
        ranked_trials
    group by
        instance_account_id
)

select *
from import_trial_types
where instance_account_id in (
10719489
,17425307
,1499683
,2291396
,9160052)


(
12212715
,11659995
,10062156
,14113363)


10719489
17425307
1499683
2291396
9160052



--- Opportunity owner & Startup Null

--- Startup

----------------------------------------------
-- 3. Startup flag
-- Startups are identified on the crm account in Salesforce. Assume all instances under the account are startup accounts
with startup as (
    select
        emd.crm_account_id,
        emd.instance_account_id,
        max(1) as startup_flag
    from
        cleansed.salesforce.salesforce_account_bcv as sab
    inner join
        foundational.customer.entity_mapping_daily_snapshot_bcv as emd
        on
            sab.id = emd.crm_account_id
    where
        sab.startup_program_c
        and sab.valid_to_timestamp = date('9999-12-31')
        and emd.instance_account_id is not null
    group by
        emd.crm_account_id,
        emd.instance_account_id
)

select count(*)
from startup
limit 10



with import_salesforce_data as (
    select
        id as sf_account_id,
        industry,
        startup_program_c as is_startup
    from cleansed.salesforce.salesforce_account_bcv
    where valid_to_timestamp = date('9999-12-31')
    and is_startup = True
)

select count(*)
from import_salesforce_data
limit 10


select *
from FEATURE_PUFFINS614.DEV.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS
where crm_account_id in ('0016R00003BDJTyQAP','0011E00001oqHuIQAU','0011E00001oG2VOQA0','0011E00001pJjsJQAS'
)


select *
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv a
left join foundational.customer.entity_mapping_daily_snapshot_bcv as emd
        on
            a.instance_account_id = emd.instance_account_id
where emd.crm_account_id in ('0016R00003BDJTyQAP','0011E00001oqHuIQAU','0011E00001oG2VOQA0','0011E00001pJjsJQAS')



select account_category, count(*)
from FEATURE_PUFFINS614.DEV.DIM_GROWTH_ENGINE_CUSTOMER_ACCOUNTS
group by account_category



----- Checking trial accounts
select 
    a.source_snapshot_date,
    a.instance_account_derived_type,
    count(*) tot_obs,
    count(distinct a.instance_account_id) as distinct_instance_accounts,
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv a
inner join
        foundational.customer.entity_mapping_daily_snapshot_bcv as emd
        on
            a.instance_account_id = emd.instance_account_id
where 
    instance_account_is_trial = True
    and instance_account_is_deleted = false
    and (instance_account_state <> 'deleted' or instance_account_state is null)
group by all




----- Checking trial accounts
select 
    a.source_snapshot_date,
    count(*) tot_obs,
    count(distinct a.instance_account_id) as distinct_instance_accounts,
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv a
inner join
        foundational.customer.entity_mapping_daily_snapshot_bcv as emd
        on
            a.instance_account_id = emd.instance_account_id
where 
    instance_account_trial_expires_on > current_date
    and instance_account_is_deleted = false
    and (instance_account_state <> 'deleted' or instance_account_state is null)
group by all




select 
    a.source_snapshot_date,
    count(*) tot_obs,
    count(distinct a.instance_account_id) as distinct_instance_accounts,
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv a
--inner join
--        foundational.customer.entity_mapping_daily_snapshot_bcv as emd
--        on
--            a.instance_account_id = emd.instance_account_id
where 
    instance_account_is_deleted = false
    and (instance_account_state <> 'deleted' or instance_account_state is null)
    and instance_account_is_active = True
    and instance_account_is_serviceable = True
    --instance_account_trial_expires_on > current_date
group by all




--- Count all active trial accounts
select count(*)
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv customer

where customer.instance_account_is_abusive = False 
  and customer.instance_account_derived_type='Active Trial'


--- Count all active trial accounts with inner join
select count(*)
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv customer
inner join
        foundational.customer.entity_mapping_daily_snapshot_bcv as emd
        on
            customer.instance_account_id = emd.instance_account_id
where customer.instance_account_is_abusive = False 
  and customer.instance_account_derived_type='Active Trial'



--- Count all active trial accounts and account age less than 30 days
select count(*)
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv customer

where customer.instance_account_is_abusive = False 
  and customer.instance_account_derived_type='Active Trial'
  and datediff('day', date(customer.instance_account_created_timestamp), current_date) < 30


--- Count all active trial accounts and days to trial expiry less than 15 days
select count(*)
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv customer

where customer.instance_account_is_abusive = False 
  and customer.instance_account_derived_type='Active Trial'
  and datediff('day', current_date, customer.instance_account_trial_expires_on) < 15


--- Count all active trial accounts and days to trial expiry less than 15 days and account age less than 30 days
select count(*)
from foundational.customer.dim_instance_accounts_daily_snapshot_bcv customer

where customer.instance_account_is_abusive = False 
  and customer.instance_account_derived_type='Active Trial'
  and datediff('day', current_date, customer.instance_account_trial_expires_on) < 15
  and datediff('day', date(customer.instance_account_created_timestamp), current_date) < 30








-----------------------------------------------
-- Trial accounts from trial SKUs



select sku_state, count(*) as total_accounts
from propagated_cleansed.product_accounts.base_skus_scd2
group by sku_state






with latest_records as (
    select
        instance_account_id,
        date(created_timestamp) as created_date,
        sku_name, 
        sku_state,
        row_number() over (
            partition by instance_account_id, sku_name, sku_state
            order by date(created_timestamp) desc
        ) as rank
    from
        propagated_cleansed.product_accounts.base_skus --_scd2
    where sku_state not in ('expired', 'cancelled')
    qualify rank = 1
),

prop_sku_trial as (
    select
        instance_account_id,
        min(created_date) as first_sku_created_date,
        count(*) total_skus,
        sum(case when sku_state = 'trial' then 1 else 0 end) as number_trial_skus,
        number_trial_skus / count(*) as trial_sku_ratio,
    from latest_records
    group by instance_account_id
)


select 
    count(*) as total_accounts,
    sum(case when trial_sku_ratio = 1 then 1 else 0 end) as total_trial_accounts,
from prop_sku_trial
--where date_trunc('quarter', first_sku_created_date) = '2025-01-01'





select *
from prop_sku_trial
where trial_sku_ratio = 1
and total_skus = 3
limit 10


select *
from propagated_cleansed.product_accounts.base_skus_scd2
where instance_account_id = 10471019




select *
from propagated_cleansed.product_accounts.base_skus
where instance_account_id = 10471019






select sku_state,
       count(*) as total_accounts,
from latest_records
where sku_state is null
group by sku_state

select min(created_date) as first_trial_date,
       max(created_date) as last_trial_date,
       count(*) as total_trials,
from latest_records

ranked_trials as (
    select
        instance_account_id,
        created_timestamp,
        sku_name
    from
        ranked_skus
    -- assume it's the same trial if created < 5 sec apart#}
    qualify
        dense_rank()
            over (
                partition by
                    instance_account_id
                order by
                    case
                        when prev_timestamp is null then created_timestamp
                        when datediff(second, prev_timestamp, created_timestamp) <= 5 then prev_timestamp
                        else created_timestamp
                    end
            ) = 1
),

trial_types as (
    select
        instance_account_id,
        listagg(distinct sku_name, ' + ') within group (order by sku_name) as trial_type
    from
        ranked_trials
    group by
        instance_account_id
)

select *
from trial_types
limit 10



---- Check trial extras data

select trial_extra_key, count(*)
from propagated_cleansed.product_accounts.base_trial_extras
where 
    lower(trial_extra_key) like '%comp%'
    --or lower(trial_extra_key) like '%region%'
group by trial_extra_key
order by 2 desc



with region as (
    select instance_account_id, trial_extra_value as trial_region
    from propagated_cleansed.product_accounts.base_trial_extras
    where 
        trial_extra_key = 'Inferred_Region'
        and date(created_timestamp) > date('2025-05-01')
)


select trial_region, count(*)
from region
group by trial_region
order by 2 desc





select *
from region
limit 10





with size as (
    select instance_account_id, trial_extra_value as trial_size
    from propagated_cleansed.product_accounts.base_trial_extras
    where 
        trial_extra_key = 'cb_company_size'
        and date(created_timestamp) > date('2025-05-01')
)


select trial_size, count(*)
from size
group by trial_size
order by 2 desc




select *
from size
limit 10






---- Validate region

select
    sf.region_c,
    region_.trial_extra_value as inferred_region,
    count(distinct af.instance_account_id) as num_trials,
    count(distinct(iff(af.first_verified_date is not null, af.instance_account_id, null))) as num_verified_trials

from presentation.growth_analytics.trial_accounts af
left join cleansed.salesforce.salesforce_lead_bcv as sf
    on af.cust_owner_email = sf.email
left join propagated_cleansed.product_accounts.base_trial_extras region_
        on af.instance_account_id = region_.instance_account_id
        and region_.trial_extra_key = 'Inferred_Region'
where af.instance_account_created_date >= '2025-01-01'
    and af.is_direct_buy = FALSE
    and sf.region_c is not null
    --and af.instance_account_created_date <= '2025-05-11'

group by all
order by 3 desc




---- Validate sizing

select
    region_.trial_extra_value as inferred_size,
    count(distinct af.instance_account_id) as num_trials,
    count(distinct(iff(af.first_verified_date is not null, af.instance_account_id, null))) as num_verified_trials

from presentation.growth_analytics.trial_accounts af
left join propagated_cleansed.product_accounts.base_trial_extras region_
        on af.instance_account_id = region_.instance_account_id
        and region_.trial_extra_key = 'help_desk_size'
where af.instance_account_created_date >= '2025-01-01'
    and af.is_direct_buy = FALSE
    --and af.instance_account_created_date <= '2025-05-11'

group by all
order by 3 desc




select 
    max(sent_at) as max_date,
    count(*) as total_accounts,
    count(distinct account_id) as unique_accounts,
    sum(case when account_id is not null then 1 else 0 end) as accounts_no_null
from RAW.CENTRAL_ADMIN.ONBOARDING_PANEL_TASK_COMPLETION






select  
    instance_account_id,
    date(max(agent_last_login_timestamp)) as last_login_date
from propagated_foundational.product_agent_info.dim_agent_emails_bcv
group by instance_account_id



select 
    AGENT_ROLE,
    AGENT_IS_OWNER,
    count(*) as total_accounts,
from propagated_foundational.product_agent_info.dim_agent_emails_bcv
group by all
order by 2 desc, 1


