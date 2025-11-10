--- Step 0: Import relevant fields from trial accounts
with accounts as (
    select 
        trial_accounts.instance_account_id,
        trial_accounts.crm_account_id,
        --- Overall Win data
        trial_accounts.win_date,
        case when trial_accounts.win_date is not null then 1 else null end as is_won,
        case when trial_accounts.win_date is not null then instance_account_arr_usd_at_win else null end as is_won_arr,
        --- SS wins
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then 1 else null 
        end as is_won_ss,
        case 
            when 
                trial_accounts.win_date is not null 
                and trial_accounts.sales_model_at_win <> 'Assisted'
                and trial_accounts.is_direct_buy = FALSE
                then instance_account_arr_usd_at_win else null 
        end as is_won_ss_arr,
        --- Trials extra info
        trial_accounts.region,
        trial_accounts.help_desk_size_grouped,
        trial_accounts.instance_account_created_date,
        trial_accounts.seats_capacity_band_at_win,
    from presentation.growth_analytics.trial_accounts trial_accounts 
)

select *
from accounts