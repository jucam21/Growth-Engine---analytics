select billing_account_type, count(*)
from foundational.customer.dim_billing_accounts_daily_snapshot_bcv
group by 1
order by 2 desc;

with import_billing_accounts as (
    select
        billing_account.billing_account_id,
        billing_account.currency,
        billing_account.dunning_state,
        billing_account.is_autopay,
        billing_account.sales_model,
        billing_account.updated_timestamp,
        entity_mapping_daily_snapshot_bcv.instance_account_id
            as zendesk_account_id,
        row_number() over (
            partition by zendesk_account_id
            order by billing_account.updated_timestamp desc
        ) as rank,
    from
        foundational.customer.dim_billing_accounts_daily_snapshot_bcv
            as billing_account
        left join
            foundational.customer.entity_mapping_daily_snapshot_bcv
                as entity_mapping_daily_snapshot_bcv
            on
                billing_account.billing_account_id
                = entity_mapping_daily_snapshot_bcv.billing_account_id
    where billing_account.billing_account_type not in (
                    'Test',
                    'Employee',
                    'Fraudulent'
                )
    qualify 
        rank = 1
)

select
    count(*) as tot_obs,
    count(distinct zendesk_account_id) as zendesk_account_id
from import_billing_accounts;


 where billing_account.billing_account_type not in ('Test','Employee','Fraudulent')


select
    count(*) as tot_obs,
    count(distinct entity_mapping_daily_snapshot_bcv.instance_account_id) as zendesk_account_id
from
    foundational.customer.dim_billing_accounts_daily_snapshot_bcv
        as billing_account
    left join
        foundational.customer.entity_mapping_daily_snapshot_bcv
            as entity_mapping_daily_snapshot_bcv
        on
            billing_account.billing_account_id
            = entity_mapping_daily_snapshot_bcv.billing_account_id


select
entity_mapping_daily_snapshot_bcv.instance_account_id, count(*)
from
    foundational.customer.dim_billing_accounts_daily_snapshot_bcv
        as billing_account
    left join
        foundational.customer.entity_mapping_daily_snapshot_bcv
            as entity_mapping_daily_snapshot_bcv
        on
            billing_account.billing_account_id
            = entity_mapping_daily_snapshot_bcv.billing_account_id
group by 1
order by 2 desc
limit 10;



select
    billing_account.billing_account_id,
    billing_account.updated_timestamp,
    --billing_account.billing_account_hierarchy_type,
    billing_account.billing_account_status,
    billing_account.billing_account_type,
    billing_account.dunning_state,
    billing_account.days_in_dunning
from
    foundational.customer.dim_billing_accounts_daily_snapshot_bcv
        as billing_account
    left join
        foundational.customer.entity_mapping_daily_snapshot_bcv
            as entity_mapping_daily_snapshot_bcv
        on
            billing_account.billing_account_id
            = entity_mapping_daily_snapshot_bcv.billing_account_id
where entity_mapping_daily_snapshot_bcv.instance_account_id = 9987557
order by billing_account.updated_timestamp desc



select
    *
from
    foundational.customer.dim_billing_accounts_daily_snapshot_bcv
        as billing_account
    left join
        foundational.customer.entity_mapping_daily_snapshot_bcv
            as entity_mapping_daily_snapshot_bcv
        on
            billing_account.billing_account_id
            = entity_mapping_daily_snapshot_bcv.billing_account_id
where entity_mapping_daily_snapshot_bcv.instance_account_id = 97001
order by billing_account.updated_timestamp desc


select
    dunning_state,
    billing_account_type,
    min(days_in_dunning),
    max(days_in_dunning)
from
    foundational.customer.dim_billing_accounts_daily_snapshot_bcv
group by 1,2
order by 1







select
    entity_mapping_daily_snapshot_bcv.instance_account_id,
    billing_account.billing_account_id,
    billing_account.updated_timestamp,
    --billing_account.billing_account_hierarchy_type,
    billing_account.billing_account_status,
    billing_account.billing_account_type,
    billing_account.dunning_state,
    billing_account.days_in_dunning
from
    foundational.customer.dim_billing_accounts_daily_snapshot_bcv
        as billing_account
    left join
        foundational.customer.entity_mapping_daily_snapshot_bcv
            as entity_mapping_daily_snapshot_bcv
        on
            billing_account.billing_account_id
            = entity_mapping_daily_snapshot_bcv.billing_account_id
where entity_mapping_daily_snapshot_bcv.instance_account_id in (
    11732316,
10560574,
10109574,
19897007,
20751786,
9923230,
23740691,
1043556,
21652832,
18490843,
22098235,
11681372,
9987557,
21487495,
18822946,
23755756,
14242062,
1041102,
16696888,
20574649,
11151791,
20246105,
23971078,
20180148,
11347821,
14159238,
22453677,
20416231,
10210089,
9810042,
10736087,
22222287,
22817750,
11554468,
10482218,
9241100,
16506627,
23877876,
23709551,
9100352,
10831809,
13491243,
23389165,
22772040,
97001,
13273381,
13160076,
14242163,
15079541,
22917637,
10673645,
23731923,
10695309,
23101310,
20698236,
9893273,
18230527,
17514825,
10922003,
21955425,
807052,
14777973,
817600,
10840502,
17214525,
23086772,
18428426,
19158126,
10493840,
17007791,
23620718,
18082374,
17347497,
21059498,
23890375,
18578021,
22161658,
23879779,
20681680,
22176792,
10914703,
19147025,
14586478,
20261240,
9201512,
20888041,
9590112,
15502254,
18061566,
20120851,
22827782,
9802358,
20006662,
14584237,
22076425,
14176581,
18761925,
20216109,
13770075
)
order by entity_mapping_daily_snapshot_bcv.instance_account_id, billing_account.updated_timestamp desc

