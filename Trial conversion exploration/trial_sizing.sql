--- Query for trial sizing
--- Replicates easy to buy funnel & adds additional fields

-------------------------------------
--- Add ticket counts during trial period
with ticket_count_account as (
select
        tickets.instance_account_id,
        date_trunc('day', min(tickets.source_snapshot_date)) as first_ticket_created_date,
        -- Total tickets created
        sum(count_created_tickets) as total_tickets,
        sum(case when ticket_via_id in (51, 52) then count_created_tickets end) as total_sample_tickets,
        -- Total tickets closed & solved flag
        sum(count_closed_tickets) as total_solved_tickets,
        sum(case when ticket_via_id in (51, 52) then count_closed_tickets end) as total_solved_sample_tickets,
        case when total_solved_tickets > 0 then 1 else 0 end as ticket_solved_flag,
        case when total_solved_sample_tickets > 0 then 1 else 0 end as ticket_sample_solved_flag,
        -- Total tickets created by channel
        -- Used this link: https://developer.zendesk.com/documentation/ticketing/reference-guides/via-types/
        -- Grouped by Ticket created by
        -- In case null, grouped as "other"
        sum(case when ticket_via_id in (0, 17, 20, 27, 42, 48) then count_created_tickets end) as web_tickets,
        sum(case when ticket_via_id = 4 then count_created_tickets end) as email_tickets,
        sum(case when ticket_via_id in (5, 31, 36, 39, 40, 44, 45, 46) then count_created_tickets end) as api_tickets,
        sum(case when ticket_via_id in (8, 62) then count_created_tickets end) as rule_tickets,
        sum(case when ticket_via_id in (23, 26, 30, 88) then count_created_tickets end) as twitter_tickets,
        sum(case when ticket_via_id = 24 then count_created_tickets end) as forum_tickets,
        sum(case when ticket_via_id = 29 then count_created_tickets end) as chat_tickets,
        sum(case when ticket_via_id in (33, 34, 35) then count_created_tickets end) as voice_tickets,
        sum(case when ticket_via_id in (38, 41) then count_created_tickets end) as facebook_tickets,
        sum(case when ticket_via_id = 49 then count_created_tickets end) as mobile_sdk_tickets,

        sum(case when ticket_via_id = 50 then count_created_tickets end) as help_center_tickets,
        sum(case when ticket_via_id in (51, 52) then count_created_tickets end) as sample_ticket_tickets,
        sum(case when ticket_via_id = 55 then count_created_tickets end) as any_channel_tickets,
        sum(case when ticket_via_id = 56 then count_created_tickets end) as mobile_tickets,
        sum(case when ticket_via_id = 57 then count_created_tickets end) as sms_tickets,
        sum(case when ticket_via_id = 72 then count_created_tickets end) as line_tickets,
        sum(case when ticket_via_id = 73 then count_created_tickets end) as wechat_tickets,
        sum(case when ticket_via_id = 74 then count_created_tickets end) as whatsapp_tickets,
        sum(case when ticket_via_id = 75 then count_created_tickets end) as native_messaging_tickets,
        sum(case when ticket_via_id = 76 then count_created_tickets end) as mailgun_tickets,

        sum(case when ticket_via_id = 77 then count_created_tickets end) as messagebird_sms_tickets,
        sum(case when ticket_via_id = 79 then count_created_tickets end) as telegram_tickets,
        sum(case when ticket_via_id = 80 then count_created_tickets end) as twilio_sms_tickets,
        sum(case when ticket_via_id = 81 then count_created_tickets end) as viber_tickets,
        sum(case when ticket_via_id = 82 then count_created_tickets end) as google_rcs_tickets,
        sum(case when ticket_via_id = 83 then count_created_tickets end) as apple_business_chat_tickets,
        sum(case when ticket_via_id = 84 then count_created_tickets end) as google_business_messages_tickets,
        sum(case when ticket_via_id = 85 then count_created_tickets end) as kakaotalk_tickets,
        sum(case when ticket_via_id = 86 then count_created_tickets end) as instagram_dm_tickets,
        sum(case when ticket_via_id = 87 then count_created_tickets end) as sunshine_conversations_api_tickets,
        sum(case when ticket_via_id = 90 then count_created_tickets end) as chat_transcript_tickets,
        sum(case when ticket_via_id = 91 then count_created_tickets end) as business_messaging_slack_connect_tickets,

        sum(case when ticket_via_id = 63 then count_created_tickets end) as answer_bot_for_agents_tickets,
        sum(case when ticket_via_id = 64 then count_created_tickets end) as answer_bot_for_slack_tickets,
        sum(case when ticket_via_id = 65 then count_created_tickets end) as answer_bot_for_sdk_tickets,
        sum(case when ticket_via_id = 66 then count_created_tickets end) as answer_bot_api_tickets,
        sum(case when ticket_via_id = 67 then count_created_tickets end) as answer_bot_for_web_widget_tickets,
        sum(case when ticket_via_id = 69 then count_created_tickets end) as side_conversation_tickets,
        sum(case when ticket_via_id = 78 then count_created_tickets end) as sunshine_conversations_facebook_messenger_tickets,
        sum(case when ticket_via_id = 88 then count_created_tickets end) as sunshine_conversations_twitter_dm_tickets,
        sum(case when ticket_via_id in (9, 10, 11, 12, 13, 14, 15, 16, 19, 21, 22, 25, 28, 32, 37, 43, 47, 53, 54, 58, 59, 60, 61, 68, 70, 71, 89) then count_created_tickets end) as other_tickets
    from
        propagated_functional.product_analytics.fact_aggregated_tickets_data_daily_snapshot tickets
    inner join presentation.growth_analytics.trial_shopping_cart_funnel trial_funnel
        on 
            tickets.instance_account_id = trial_funnel.account_id
            and tickets.source_snapshot_date >= trial_funnel.trial_create_date
            and tickets.source_snapshot_date <= dateadd('day', 14, trial_funnel.trial_create_date)
    group by
        tickets.instance_account_id
),

-------------------------------------
--- Add trial type SKUs
ranked_skus as (
    select
        instance_account_id,
        created_timestamp,
        case
            when sku_name in ('sell_suite', 'sell') then 'Sell'
            when sku_name = 'support' then 'Support'
            when sku_name in ('zendesk_suite', 'suite') then 'Suite'
        end as sku_name,
        lag(created_timestamp) over (partition by instance_account_id order by created_timestamp) as prev_timestamp
    from
        propagated_cleansed.product_accounts.base_skus_scd2
    where
        sku_name in ('sell_suite', 'sell', 'support', 'zendesk_suite', 'suite')
),

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
),

-------------------------------------
--- Join back to trial funnel the data & bucket some variables
trial_funnel as (
    select
        -- Extract all fields from trial funnel table
        trial_funnel.*,
        -- Adjust product at win
        case 
            when trial_funnel.win then trial_funnel.product_at_win
            else 'Not Win'
        end as product_at_win_adjusted,
        -- Add ticket counts
        ticket_count_account.total_tickets as total_tickets,
        ticket_count_account.total_sample_tickets as total_sample_tickets,
        ticket_count_account.total_solved_tickets as total_solved_tickets,
        ticket_count_account.total_solved_sample_tickets as total_solved_sample_tickets,
        -- Add trial type SKUs
        trial_types.trial_type trial_type_sku,
        -- Adding region 
        region_.trial_extra_value as region,
        -- Create grouping variables
        case 
            when trial_funnel.arr_at_win = 0 or trial_funnel.arr_at_win is null then '1. NA'
            when trial_funnel.arr_at_win <= 5000 then '2. 1-5k'
            when trial_funnel.arr_at_win <= 10000 then '3. 5k-10k'
            when trial_funnel.arr_at_win <= 25000 then '4. 10k-25k'
            when trial_funnel.arr_at_win <= 50000 then '5. 25k-50k'
            when trial_funnel.arr_at_win <= 100000 then '6. 50k-100k'
            else '7. 100k+' 
        end as arr_at_win_band,
        case 
            when trial_funnel.cart_visit = 0 or trial_funnel.cart_visit is null then '1. NA'
            when trial_funnel.cart_visit <= 2 then '2. 1-2'
            when trial_funnel.cart_visit <= 5 then '3. 2-5'
            when trial_funnel.cart_visit <= 10 then '4. 5-10'
            else '7. 10+' 
        end as cart_visits_band,
        case
            when trial_funnel.seats_at_win = 0 or trial_funnel.seats_at_win is null then '1. NA'
            when trial_funnel.seats_at_win <= 5 then '2. 1-5'
            when trial_funnel.seats_at_win <= 10 then '3. 6-10'
            when trial_funnel.seats_at_win <= 25 then '4. 11-25'
            when trial_funnel.seats_at_win <= 50 then '5. 26-50'
            when trial_funnel.seats_at_win <= 100 then '6. 51-100'
            else '7. 100+'
        end as seats_at_win_range_band,
        case
            when ticket_count_account.total_tickets = 0 or ticket_count_account.total_tickets is null then '1. NA'
            when ticket_count_account.total_tickets <= 5 then '2. 1-5'
            when ticket_count_account.total_tickets <= 10 then '3. 6-10'
            when ticket_count_account.total_tickets <= 25 then '4. 11-25'
            when ticket_count_account.total_tickets <= 50 then '5. 26-50'
            when ticket_count_account.total_tickets <= 100 then '6. 51-100'
            else '7. 100+'
        end as total_tickets_range_band,
        case
            when ticket_count_account.total_sample_tickets = 0 or ticket_count_account.total_sample_tickets is null then '1. NA'
            when ticket_count_account.total_sample_tickets <= 5 then '2. 1-5'
            when ticket_count_account.total_sample_tickets <= 10 then '3. 6-10'
            when ticket_count_account.total_sample_tickets <= 25 then '4. 11-25'
            when ticket_count_account.total_sample_tickets <= 50 then '5. 26-50'
            when ticket_count_account.total_sample_tickets <= 100 then '6. 51-100'
            else '7. 100+'
        end as total_sample_tickets_range_band,
        case
            when ticket_count_account.total_solved_tickets = 0 or ticket_count_account.total_solved_tickets is null then '1. NA'
            when ticket_count_account.total_solved_tickets <= 5 then '2. 1-5'
            when ticket_count_account.total_solved_tickets <= 10 then '3. 6-10'
            when ticket_count_account.total_solved_tickets <= 25 then '4. 11-25'
            when ticket_count_account.total_solved_tickets <= 50 then '5. 26-50'
            when ticket_count_account.total_solved_tickets <= 100 then '6. 51-100'
            else '7. 100+'
        end as total_solved_tickets_range_band,
        case
            when ticket_count_account.total_solved_sample_tickets = 0 or ticket_count_account.total_solved_sample_tickets is null then '1. NA'
            when ticket_count_account.total_solved_sample_tickets <= 5 then '2. 1-5'
            when ticket_count_account.total_solved_sample_tickets <= 10 then '3. 6-10'
            when ticket_count_account.total_solved_sample_tickets <= 25 then '4. 11-25'
            when ticket_count_account.total_solved_sample_tickets <= 50 then '5. 26-50'
            when ticket_count_account.total_solved_sample_tickets <= 100 then '6. 51-100'
            else '7. 100+'
        end as total_solved_sample_tickets_range_band,
        --- Adding range bands for tickets by channel

case
    when ticket_count_account.web_tickets = 0 or ticket_count_account.web_tickets is null then '1. NA'
    when ticket_count_account.web_tickets <= 5 then '2. 1-5'
    when ticket_count_account.web_tickets <= 10 then '3. 6-10'
    when ticket_count_account.web_tickets <= 25 then '4. 11-25'
    when ticket_count_account.web_tickets <= 50 then '5. 26-50'
    when ticket_count_account.web_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as web_tickets_range_band,

case
    when ticket_count_account.email_tickets = 0 or ticket_count_account.email_tickets is null then '1. NA'
    when ticket_count_account.email_tickets <= 5 then '2. 1-5'
    when ticket_count_account.email_tickets <= 10 then '3. 6-10'
    when ticket_count_account.email_tickets <= 25 then '4. 11-25'
    when ticket_count_account.email_tickets <= 50 then '5. 26-50'
    when ticket_count_account.email_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as email_tickets_range_band,

case
    when ticket_count_account.api_tickets = 0 or ticket_count_account.api_tickets is null then '1. NA'
    when ticket_count_account.api_tickets <= 5 then '2. 1-5'
    when ticket_count_account.api_tickets <= 10 then '3. 6-10'
    when ticket_count_account.api_tickets <= 25 then '4. 11-25'
    when ticket_count_account.api_tickets <= 50 then '5. 26-50'
    when ticket_count_account.api_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as api_tickets_range_band,

case
    when ticket_count_account.rule_tickets = 0 or ticket_count_account.rule_tickets is null then '1. NA'
    when ticket_count_account.rule_tickets <= 5 then '2. 1-5'
    when ticket_count_account.rule_tickets <= 10 then '3. 6-10'
    when ticket_count_account.rule_tickets <= 25 then '4. 11-25'
    when ticket_count_account.rule_tickets <= 50 then '5. 26-50'
    when ticket_count_account.rule_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as rule_tickets_range_band,

case
    when ticket_count_account.twitter_tickets = 0 or ticket_count_account.twitter_tickets is null then '1. NA'
    when ticket_count_account.twitter_tickets <= 5 then '2. 1-5'
    when ticket_count_account.twitter_tickets <= 10 then '3. 6-10'
    when ticket_count_account.twitter_tickets <= 25 then '4. 11-25'
    when ticket_count_account.twitter_tickets <= 50 then '5. 26-50'
    when ticket_count_account.twitter_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as twitter_tickets_range_band,

case
    when ticket_count_account.forum_tickets = 0 or ticket_count_account.forum_tickets is null then '1. NA'
    when ticket_count_account.forum_tickets <= 5 then '2. 1-5'
    when ticket_count_account.forum_tickets <= 10 then '3. 6-10'
    when ticket_count_account.forum_tickets <= 25 then '4. 11-25'
    when ticket_count_account.forum_tickets <= 50 then '5. 26-50'
    when ticket_count_account.forum_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as forum_tickets_range_band,

case
    when ticket_count_account.chat_tickets = 0 or ticket_count_account.chat_tickets is null then '1. NA'
    when ticket_count_account.chat_tickets <= 5 then '2. 1-5'
    when ticket_count_account.chat_tickets <= 10 then '3. 6-10'
    when ticket_count_account.chat_tickets <= 25 then '4. 11-25'
    when ticket_count_account.chat_tickets <= 50 then '5. 26-50'
    when ticket_count_account.chat_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as chat_tickets_range_band,

case
    when ticket_count_account.voice_tickets = 0 or ticket_count_account.voice_tickets is null then '1. NA'
    when ticket_count_account.voice_tickets <= 5 then '2. 1-5'
    when ticket_count_account.voice_tickets <= 10 then '3. 6-10'
    when ticket_count_account.voice_tickets <= 25 then '4. 11-25'
    when ticket_count_account.voice_tickets <= 50 then '5. 26-50'
    when ticket_count_account.voice_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as voice_tickets_range_band,

case
    when ticket_count_account.facebook_tickets = 0 or ticket_count_account.facebook_tickets is null then '1. NA'
    when ticket_count_account.facebook_tickets <= 5 then '2. 1-5'
    when ticket_count_account.facebook_tickets <= 10 then '3. 6-10'
    when ticket_count_account.facebook_tickets <= 25 then '4. 11-25'
    when ticket_count_account.facebook_tickets <= 50 then '5. 26-50'
    when ticket_count_account.facebook_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as facebook_tickets_range_band,

case
    when ticket_count_account.mobile_sdk_tickets = 0 or ticket_count_account.mobile_sdk_tickets is null then '1. NA'
    when ticket_count_account.mobile_sdk_tickets <= 5 then '2. 1-5'
    when ticket_count_account.mobile_sdk_tickets <= 10 then '3. 6-10'
    when ticket_count_account.mobile_sdk_tickets <= 25 then '4. 11-25'
    when ticket_count_account.mobile_sdk_tickets <= 50 then '5. 26-50'
    when ticket_count_account.mobile_sdk_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as mobile_sdk_tickets_range_band,


case
    when ticket_count_account.help_center_tickets = 0 or ticket_count_account.help_center_tickets is null then '1. NA'
    when ticket_count_account.help_center_tickets <= 5 then '2. 1-5'
    when ticket_count_account.help_center_tickets <= 10 then '3. 6-10'
    when ticket_count_account.help_center_tickets <= 25 then '4. 11-25'
    when ticket_count_account.help_center_tickets <= 50 then '5. 26-50'
    when ticket_count_account.help_center_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as help_center_tickets_range_band,

case
    when ticket_count_account.sample_ticket_tickets = 0 or ticket_count_account.sample_ticket_tickets is null then '1. NA'
    when ticket_count_account.sample_ticket_tickets <= 5 then '2. 1-5'
    when ticket_count_account.sample_ticket_tickets <= 10 then '3. 6-10'
    when ticket_count_account.sample_ticket_tickets <= 25 then '4. 11-25'
    when ticket_count_account.sample_ticket_tickets <= 50 then '5. 26-50'
    when ticket_count_account.sample_ticket_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as sample_ticket_tickets_range_band,

case
    when ticket_count_account.any_channel_tickets = 0 or ticket_count_account.any_channel_tickets is null then '1. NA'
    when ticket_count_account.any_channel_tickets <= 5 then '2. 1-5'
    when ticket_count_account.any_channel_tickets <= 10 then '3. 6-10'
    when ticket_count_account.any_channel_tickets <= 25 then '4. 11-25'
    when ticket_count_account.any_channel_tickets <= 50 then '5. 26-50'
    when ticket_count_account.any_channel_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as any_channel_tickets_range_band,

case
    when ticket_count_account.mobile_tickets = 0 or ticket_count_account.mobile_tickets is null then '1. NA'
    when ticket_count_account.mobile_tickets <= 5 then '2. 1-5'
    when ticket_count_account.mobile_tickets <= 10 then '3. 6-10'
    when ticket_count_account.mobile_tickets <= 25 then '4. 11-25'
    when ticket_count_account.mobile_tickets <= 50 then '5. 26-50'
    when ticket_count_account.mobile_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as mobile_tickets_range_band,

case
    when ticket_count_account.sms_tickets = 0 or ticket_count_account.sms_tickets is null then '1. NA'
    when ticket_count_account.sms_tickets <= 5 then '2. 1-5'
    when ticket_count_account.sms_tickets <= 10 then '3. 6-10'
    when ticket_count_account.sms_tickets <= 25 then '4. 11-25'
    when ticket_count_account.sms_tickets <= 50 then '5. 26-50'
    when ticket_count_account.sms_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as sms_tickets_range_band,

case
    when ticket_count_account.line_tickets = 0 or ticket_count_account.line_tickets is null then '1. NA'
    when ticket_count_account.line_tickets <= 5 then '2. 1-5'
    when ticket_count_account.line_tickets <= 10 then '3. 6-10'
    when ticket_count_account.line_tickets <= 25 then '4. 11-25'
    when ticket_count_account.line_tickets <= 50 then '5. 26-50'
    when ticket_count_account.line_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as line_tickets_range_band,

case
    when ticket_count_account.wechat_tickets = 0 or ticket_count_account.wechat_tickets is null then '1. NA'
    when ticket_count_account.wechat_tickets <= 5 then '2. 1-5'
    when ticket_count_account.wechat_tickets <= 10 then '3. 6-10'
    when ticket_count_account.wechat_tickets <= 25 then '4. 11-25'
    when ticket_count_account.wechat_tickets <= 50 then '5. 26-50'
    when ticket_count_account.wechat_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as wechat_tickets_range_band,

case
    when ticket_count_account.whatsapp_tickets = 0 or ticket_count_account.whatsapp_tickets is null then '1. NA'
    when ticket_count_account.whatsapp_tickets <= 5 then '2. 1-5'
    when ticket_count_account.whatsapp_tickets <= 10 then '3. 6-10'
    when ticket_count_account.whatsapp_tickets <= 25 then '4. 11-25'
    when ticket_count_account.whatsapp_tickets <= 50 then '5. 26-50'
    when ticket_count_account.whatsapp_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as whatsapp_tickets_range_band,

case
    when ticket_count_account.native_messaging_tickets = 0 or ticket_count_account.native_messaging_tickets is null then '1. NA'
    when ticket_count_account.native_messaging_tickets <= 5 then '2. 1-5'
    when ticket_count_account.native_messaging_tickets <= 10 then '3. 6-10'
    when ticket_count_account.native_messaging_tickets <= 25 then '4. 11-25'
    when ticket_count_account.native_messaging_tickets <= 50 then '5. 26-50'
    when ticket_count_account.native_messaging_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as native_messaging_tickets_range_band,

case
    when ticket_count_account.mailgun_tickets = 0 or ticket_count_account.mailgun_tickets is null then '1. NA'
    when ticket_count_account.mailgun_tickets <= 5 then '2. 1-5'
    when ticket_count_account.mailgun_tickets <= 10 then '3. 6-10'
    when ticket_count_account.mailgun_tickets <= 25 then '4. 11-25'
    when ticket_count_account.mailgun_tickets <= 50 then '5. 26-50'
    when ticket_count_account.mailgun_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as mailgun_tickets_range_band,


case
    when ticket_count_account.messagebird_sms_tickets = 0 or ticket_count_account.messagebird_sms_tickets is null then '1. NA'
    when ticket_count_account.messagebird_sms_tickets <= 5 then '2. 1-5'
    when ticket_count_account.messagebird_sms_tickets <= 10 then '3. 6-10'
    when ticket_count_account.messagebird_sms_tickets <= 25 then '4. 11-25'
    when ticket_count_account.messagebird_sms_tickets <= 50 then '5. 26-50'
    when ticket_count_account.messagebird_sms_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as messagebird_sms_tickets_range_band,

case
    when ticket_count_account.telegram_tickets = 0 or ticket_count_account.telegram_tickets is null then '1. NA'
    when ticket_count_account.telegram_tickets <= 5 then '2. 1-5'
    when ticket_count_account.telegram_tickets <= 10 then '3. 6-10'
    when ticket_count_account.telegram_tickets <= 25 then '4. 11-25'
    when ticket_count_account.telegram_tickets <= 50 then '5. 26-50'
    when ticket_count_account.telegram_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as telegram_tickets_range_band,

case
    when ticket_count_account.twilio_sms_tickets = 0 or ticket_count_account.twilio_sms_tickets is null then '1. NA'
    when ticket_count_account.twilio_sms_tickets <= 5 then '2. 1-5'
    when ticket_count_account.twilio_sms_tickets <= 10 then '3. 6-10'
    when ticket_count_account.twilio_sms_tickets <= 25 then '4. 11-25'
    when ticket_count_account.twilio_sms_tickets <= 50 then '5. 26-50'
    when ticket_count_account.twilio_sms_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as twilio_sms_tickets_range_band,

case
    when ticket_count_account.viber_tickets = 0 or ticket_count_account.viber_tickets is null then '1. NA'
    when ticket_count_account.viber_tickets <= 5 then '2. 1-5'
    when ticket_count_account.viber_tickets <= 10 then '3. 6-10'
    when ticket_count_account.viber_tickets <= 25 then '4. 11-25'
    when ticket_count_account.viber_tickets <= 50 then '5. 26-50'
    when ticket_count_account.viber_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as viber_tickets_range_band,

case
    when ticket_count_account.google_rcs_tickets = 0 or ticket_count_account.google_rcs_tickets is null then '1. NA'
    when ticket_count_account.google_rcs_tickets <= 5 then '2. 1-5'
    when ticket_count_account.google_rcs_tickets <= 10 then '3. 6-10'
    when ticket_count_account.google_rcs_tickets <= 25 then '4. 11-25'
    when ticket_count_account.google_rcs_tickets <= 50 then '5. 26-50'
    when ticket_count_account.google_rcs_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as google_rcs_tickets_range_band,

case
    when ticket_count_account.apple_business_chat_tickets = 0 or ticket_count_account.apple_business_chat_tickets is null then '1. NA'
    when ticket_count_account.apple_business_chat_tickets <= 5 then '2. 1-5'
    when ticket_count_account.apple_business_chat_tickets <= 10 then '3. 6-10'
    when ticket_count_account.apple_business_chat_tickets <= 25 then '4. 11-25'
    when ticket_count_account.apple_business_chat_tickets <= 50 then '5. 26-50'
    when ticket_count_account.apple_business_chat_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as apple_business_chat_tickets_range_band,

case
    when ticket_count_account.google_business_messages_tickets = 0 or ticket_count_account.google_business_messages_tickets is null then '1. NA'
    when ticket_count_account.google_business_messages_tickets <= 5 then '2. 1-5'
    when ticket_count_account.google_business_messages_tickets <= 10 then '3. 6-10'
    when ticket_count_account.google_business_messages_tickets <= 25 then '4. 11-25'
    when ticket_count_account.google_business_messages_tickets <= 50 then '5. 26-50'
    when ticket_count_account.google_business_messages_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as google_business_messages_tickets_range_band,

case
    when ticket_count_account.kakaotalk_tickets = 0 or ticket_count_account.kakaotalk_tickets is null then '1. NA'
    when ticket_count_account.kakaotalk_tickets <= 5 then '2. 1-5'
    when ticket_count_account.kakaotalk_tickets <= 10 then '3. 6-10'
    when ticket_count_account.kakaotalk_tickets <= 25 then '4. 11-25'
    when ticket_count_account.kakaotalk_tickets <= 50 then '5. 26-50'
    when ticket_count_account.kakaotalk_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as kakaotalk_tickets_range_band,

case
    when ticket_count_account.instagram_dm_tickets = 0 or ticket_count_account.instagram_dm_tickets is null then '1. NA'
    when ticket_count_account.instagram_dm_tickets <= 5 then '2. 1-5'
    when ticket_count_account.instagram_dm_tickets <= 10 then '3. 6-10'
    when ticket_count_account.instagram_dm_tickets <= 25 then '4. 11-25'
    when ticket_count_account.instagram_dm_tickets <= 50 then '5. 26-50'
    when ticket_count_account.instagram_dm_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as instagram_dm_tickets_range_band,

case
    when ticket_count_account.sunshine_conversations_api_tickets = 0 or ticket_count_account.sunshine_conversations_api_tickets is null then '1. NA'
    when ticket_count_account.sunshine_conversations_api_tickets <= 5 then '2. 1-5'
    when ticket_count_account.sunshine_conversations_api_tickets <= 10 then '3. 6-10'
    when ticket_count_account.sunshine_conversations_api_tickets <= 25 then '4. 11-25'
    when ticket_count_account.sunshine_conversations_api_tickets <= 50 then '5. 26-50'
    when ticket_count_account.sunshine_conversations_api_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as sunshine_conversations_api_tickets_range_band,

case
    when ticket_count_account.chat_transcript_tickets = 0 or ticket_count_account.chat_transcript_tickets is null then '1. NA'
    when ticket_count_account.chat_transcript_tickets <= 5 then '2. 1-5'
    when ticket_count_account.chat_transcript_tickets <= 10 then '3. 6-10'
    when ticket_count_account.chat_transcript_tickets <= 25 then '4. 11-25'
    when ticket_count_account.chat_transcript_tickets <= 50 then '5. 26-50'
    when ticket_count_account.chat_transcript_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as chat_transcript_tickets_range_band,

case
    when ticket_count_account.business_messaging_slack_connect_tickets = 0 or ticket_count_account.business_messaging_slack_connect_tickets is null then '1. NA'
    when ticket_count_account.business_messaging_slack_connect_tickets <= 5 then '2. 1-5'
    when ticket_count_account.business_messaging_slack_connect_tickets <= 10 then '3. 6-10'
    when ticket_count_account.business_messaging_slack_connect_tickets <= 25 then '4. 11-25'
    when ticket_count_account.business_messaging_slack_connect_tickets <= 50 then '5. 26-50'
    when ticket_count_account.business_messaging_slack_connect_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as business_messaging_slack_connect_tickets_range_band,

case
    when ticket_count_account.answer_bot_for_agents_tickets = 0 or ticket_count_account.answer_bot_for_agents_tickets is null then '1. NA'
    when ticket_count_account.answer_bot_for_agents_tickets <= 5 then '2. 1-5'
    when ticket_count_account.answer_bot_for_agents_tickets <= 10 then '3. 6-10'
    when ticket_count_account.answer_bot_for_agents_tickets <= 25 then '4. 11-25'
    when ticket_count_account.answer_bot_for_agents_tickets <= 50 then '5. 26-50'
    when ticket_count_account.answer_bot_for_agents_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as answer_bot_for_agents_tickets_range_band,

case
    when ticket_count_account.answer_bot_for_slack_tickets = 0 or ticket_count_account.answer_bot_for_slack_tickets is null then '1. NA'
    when ticket_count_account.answer_bot_for_slack_tickets <= 5 then '2. 1-5'
    when ticket_count_account.answer_bot_for_slack_tickets <= 10 then '3. 6-10'
    when ticket_count_account.answer_bot_for_slack_tickets <= 25 then '4. 11-25'
    when ticket_count_account.answer_bot_for_slack_tickets <= 50 then '5. 26-50'
    when ticket_count_account.answer_bot_for_slack_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as answer_bot_for_slack_tickets_range_band,

case
    when ticket_count_account.answer_bot_for_sdk_tickets = 0 or ticket_count_account.answer_bot_for_sdk_tickets is null then '1. NA'
    when ticket_count_account.answer_bot_for_sdk_tickets <= 5 then '2. 1-5'
    when ticket_count_account.answer_bot_for_sdk_tickets <= 10 then '3. 6-10'
    when ticket_count_account.answer_bot_for_sdk_tickets <= 25 then '4. 11-25'
    when ticket_count_account.answer_bot_for_sdk_tickets <= 50 then '5. 26-50'
    when ticket_count_account.answer_bot_for_sdk_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as answer_bot_for_sdk_tickets_range_band,

case
    when ticket_count_account.answer_bot_api_tickets = 0 or ticket_count_account.answer_bot_api_tickets is null then '1. NA'
    when ticket_count_account.answer_bot_api_tickets <= 5 then '2. 1-5'
    when ticket_count_account.answer_bot_api_tickets <= 10 then '3. 6-10'
    when ticket_count_account.answer_bot_api_tickets <= 25 then '4. 11-25'
    when ticket_count_account.answer_bot_api_tickets <= 50 then '5. 26-50'
    when ticket_count_account.answer_bot_api_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as answer_bot_api_tickets_range_band,

case
    when ticket_count_account.answer_bot_for_web_widget_tickets = 0 or ticket_count_account.answer_bot_for_web_widget_tickets is null then '1. NA'
    when ticket_count_account.answer_bot_for_web_widget_tickets <= 5 then '2. 1-5'
    when ticket_count_account.answer_bot_for_web_widget_tickets <= 10 then '3. 6-10'
    when ticket_count_account.answer_bot_for_web_widget_tickets <= 25 then '4. 11-25'
    when ticket_count_account.answer_bot_for_web_widget_tickets <= 50 then '5. 26-50'
    when ticket_count_account.answer_bot_for_web_widget_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as answer_bot_for_web_widget_tickets_range_band,

case
    when ticket_count_account.side_conversation_tickets = 0 or ticket_count_account.side_conversation_tickets is null then '1. NA'
    when ticket_count_account.side_conversation_tickets <= 5 then '2. 1-5'
    when ticket_count_account.side_conversation_tickets <= 10 then '3. 6-10'
    when ticket_count_account.side_conversation_tickets <= 25 then '4. 11-25'
    when ticket_count_account.side_conversation_tickets <= 50 then '5. 26-50'
    when ticket_count_account.side_conversation_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as side_conversation_tickets_range_band,

case
    when ticket_count_account.sunshine_conversations_facebook_messenger_tickets = 0 or ticket_count_account.sunshine_conversations_facebook_messenger_tickets is null then '1. NA'
    when ticket_count_account.sunshine_conversations_facebook_messenger_tickets <= 5 then '2. 1-5'
    when ticket_count_account.sunshine_conversations_facebook_messenger_tickets <= 10 then '3. 6-10'
    when ticket_count_account.sunshine_conversations_facebook_messenger_tickets <= 25 then '4. 11-25'
    when ticket_count_account.sunshine_conversations_facebook_messenger_tickets <= 50 then '5. 26-50'
    when ticket_count_account.sunshine_conversations_facebook_messenger_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as sunshine_conversations_facebook_messenger_tickets_range_band,

case
    when ticket_count_account.sunshine_conversations_twitter_dm_tickets = 0 or ticket_count_account.sunshine_conversations_twitter_dm_tickets is null then '1. NA'
    when ticket_count_account.sunshine_conversations_twitter_dm_tickets <= 5 then '2. 1-5'
    when ticket_count_account.sunshine_conversations_twitter_dm_tickets <= 10 then '3. 6-10'
    when ticket_count_account.sunshine_conversations_twitter_dm_tickets <= 25 then '4. 11-25'
    when ticket_count_account.sunshine_conversations_twitter_dm_tickets <= 50 then '5. 26-50'
    when ticket_count_account.sunshine_conversations_twitter_dm_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as sunshine_conversations_twitter_dm_tickets_range_band,

case
    when ticket_count_account.other_tickets = 0 or ticket_count_account.other_tickets is null then '1. NA'
    when ticket_count_account.other_tickets <= 5 then '2. 1-5'
    when ticket_count_account.other_tickets <= 10 then '3. 6-10'
    when ticket_count_account.other_tickets <= 25 then '4. 11-25'
    when ticket_count_account.other_tickets <= 50 then '5. 26-50'
    when ticket_count_account.other_tickets <= 100 then '6. 51-100'
    else '7. 100+'
end as other_tickets_range_band

    from
        presentation.growth_analytics.trial_shopping_cart_funnel trial_funnel
    left join ticket_count_account
        on trial_funnel.account_id = ticket_count_account.instance_account_id
    left join trial_types
        on trial_funnel.account_id = trial_types.instance_account_id
    left join propagated_cleansed.product_accounts.base_trial_extras region_
        on trial_funnel.account_id = region_.instance_account_id
        and region_.trial_extra_key = 'Inferred_Region'
),


-------------------------------------
--- Aggregate data
main as (
    select 
        -- Variables for grouping
        date_trunc('quarter', trial_funnel.trial_create_date) trial_created_quarter,
        trial_funnel.paid_products,
        --trial_funnel.is_mobile_created,
        trial_funnel.core_base_plan,
        trial_funnel.product_at_win,
        trial_funnel.startup_flag,
        --trial_funnel.is_personal_domain,
        trial_funnel.instance_account_trial_expired,
        --trial_funnel.seats_at_win,
        trial_funnel.in_trial_go_live,
        trial_funnel.in_trial_ticket_created,
        trial_funnel.in_trial_ticket_solved,
        trial_funnel.trial_type_sku,
        trial_funnel.region,
        -- Variables for grouping that are bucketed
        trial_funnel.product_at_win_adjusted,
        trial_funnel.employee_range_band,
        trial_funnel.arr_at_win_band,
        trial_funnel.cart_visits_band,
        trial_funnel.seats_at_win_range_band,
        trial_funnel.total_tickets_range_band,
        trial_funnel.total_sample_tickets_range_band,
        trial_funnel.total_solved_tickets_range_band,
        trial_funnel.total_solved_sample_tickets_range_band,
--- Tickets by channel range bands
trial_funnel.web_tickets_range_band,
trial_funnel.email_tickets_range_band,
trial_funnel.api_tickets_range_band,
trial_funnel.rule_tickets_range_band,
trial_funnel.twitter_tickets_range_band,
trial_funnel.forum_tickets_range_band,
trial_funnel.chat_tickets_range_band,
trial_funnel.voice_tickets_range_band,
trial_funnel.facebook_tickets_range_band,
trial_funnel.mobile_sdk_tickets_range_band,
trial_funnel.help_center_tickets_range_band,
trial_funnel.sample_ticket_tickets_range_band,
trial_funnel.any_channel_tickets_range_band,
trial_funnel.mobile_tickets_range_band,
trial_funnel.sms_tickets_range_band,
trial_funnel.line_tickets_range_band,
trial_funnel.wechat_tickets_range_band,
trial_funnel.whatsapp_tickets_range_band,
trial_funnel.native_messaging_tickets_range_band,
trial_funnel.mailgun_tickets_range_band,
trial_funnel.messagebird_sms_tickets_range_band,
trial_funnel.telegram_tickets_range_band,
trial_funnel.twilio_sms_tickets_range_band,
trial_funnel.viber_tickets_range_band,
trial_funnel.google_rcs_tickets_range_band,
trial_funnel.apple_business_chat_tickets_range_band,
trial_funnel.google_business_messages_tickets_range_band,
trial_funnel.kakaotalk_tickets_range_band,
trial_funnel.instagram_dm_tickets_range_band,
trial_funnel.sunshine_conversations_api_tickets_range_band,
trial_funnel.chat_transcript_tickets_range_band,
trial_funnel.business_messaging_slack_connect_tickets_range_band,
trial_funnel.answer_bot_for_agents_tickets_range_band,
trial_funnel.answer_bot_for_slack_tickets_range_band,
trial_funnel.answer_bot_for_sdk_tickets_range_band,
trial_funnel.answer_bot_api_tickets_range_band,
trial_funnel.answer_bot_for_web_widget_tickets_range_band,
trial_funnel.side_conversation_tickets_range_band,
trial_funnel.sunshine_conversations_facebook_messenger_tickets_range_band,
trial_funnel.sunshine_conversations_twitter_dm_tickets_range_band,
trial_funnel.other_tickets_range_band,

        -- Aggregated variables
        count(*) as verified_trials,
        sum(case when trial_funnel.cart_visit then 1 else 0 end) as cart_visits,
        sum(case when trial_funnel.all_plans_cta_flag then 1 else 0 end) as pricing_lineup,
        sum(case 
            when 
                trial_funnel.payment_page_from_suite_flag 
                or trial_funnel.payment_page_from_support_flag 
                and trial_funnel.all_plans_cta_flag 
                then 1 else 0 
            end) as payment_page_visits_pricing,
        sum(case when trial_funnel.payment_page_visit then 1 else 0 end) as payment_page_visits_overall,
        sum(case when trial_funnel.payment_submission then 1 else 0 end) as payment_submissions,
        sum(case when trial_funnel.win then 1 else 0 end) as wins,
        sum(case when datediff('day', trial_funnel.trial_create_date, trial_funnel.win_date) <= 30 then 1 else 0 end) as wins_30d,
        sum(case when datediff('day', trial_funnel.trial_create_date, trial_funnel.win_date) <= 60 then 1 else 0 end) as wins_60d,
        sum(trial_funnel.arr_at_win) as arr_at_win,
        sum(trial_funnel.arr_at_win) / sum(case when trial_funnel.win then 1 else 0 end) as ADS,
        -- Aggregated variables: tickets
        sum(trial_funnel.total_tickets) as total_tickets,
        sum(trial_funnel.total_sample_tickets) as total_sample_tickets,
        sum(trial_funnel.total_solved_tickets) as total_solved_tickets,
        sum(trial_funnel.total_solved_sample_tickets) as total_solved_sample_tickets
    from trial_funnel trial_funnel
    group by all
)

select 
    region, 
    count(*) as tot_obs,
    sum(verified_trials) as tot_verified_trials,
from main
group by region
order by 1
--limit 10








-------------------------------------------------------
--- Query that populates 
--- https://docs.google.com/spreadsheets/d/1BbAdM4MK9Tn-KcPXmVsE064XwBWefMopHb_wWBCj8D4/edit?gid=2093612758#gid=2093612758



Is trial
Account age
Employee size
Region
# of tickets
Tickets by channel
HC created





Sample ticket solved
External email connected




select region, count(*)
from functional.growth_engine.dim_growth_engine_customer_accounts 
where is_trial = true
group by all
order by 2 desc


with accounts as (
    select
        a.*,
        -- Account age
        case
            when
                cast({{TRIAL_ACCOUNT_AGE_RULE}} as string) = 'all'
                then 1
            when
                TRIAL_AGE >= {{TRIAL_ACCOUNT_AGE_RULE}}
                then 1
            else 0
        end as trial_account_age_filter,
        -- Empoyee band
        case
            when
                {{EMPLOYEE_RULE}} = 'all'
                then 1
            when
                EMPLOYEE_COUNT_RANGE = {{EMPLOYEE_RULE}}
                then 1
            else 0
        end as employee_filter,
        -- Region
        case
            when
                {{REGION_RULE}} = 'all'
                then 1
            when
                region = {{REGION_RULE}}
                then 1
            else 0
        end as region_filter,
        -- Min number tickets
        case
            when
                cast({{TICKETS_RULE}} as string) = 'all'
                then 1
            when
                TOTAL_TICKETS >= {{TICKETS_RULE}}
                then 1
            else 0
        end as tickets_filter,
        -- Min MSG number tickets
        case
            when
                cast({{MSG_TICKETS_RULE}} as string) = 'all'
                then 1
            when
                TOTAL_MSG_TICKETS >= {{MSG_TICKETS_RULE}}
                then 1
            else 0
        end as msg_tickets_filter,
        -- Min WEB number tickets
        case
            when
                cast({{WEB_TICKETS_RULE}} as string) = 'all'
                then 1
            when
                TOTAL_WEB_FORM_TICKETS >= {{WEB_TICKETS_RULE}}
                then 1
            else 0
        end as web_tickets_filter,
        -- Min CHAT number tickets
        case
            when
                cast({{CHAT_TICKETS_RULE}} as string) = 'all'
                then 1
            when
                TOTAL_CHAT_TICKETS >= {{CHAT_TICKETS_RULE}}
                then 1
            else 0
        end as chat_tickets_filter,
        -- Min TALK number tickets
        case
            when
                cast({{TALK_TICKETS_RULE}} as string) = 'all'
                then 1
            when
                TOTAL_TALK_TICKETS >= {{TALK_TICKETS_RULE}}
                then 1
            else 0
        end as talk_tickets_filter,
        -- HC
        case
            when
                {{HC_RULE}} = 'all'
                then 1
            when
                cast(lower(IS_HELP_CENTER_CREATED) as string) = {{HC_RULE}}
                then 1
            else 0
        end as hc_filter,
        -- Sample ticket solved
        case
            when
                {{SAMPLE_TICKET_RULE}} = 'all'
                then 1
            when
                cast(lower(IS_SAMPLE_TICKET_SOLVED) as string) = {{SAMPLE_TICKET_RULE}}
                then 1
            else 0
        end as sample_ticket_filter,
        -- External email connected
        case
            when
                {{EXTERNAL_EMAIL_RULE}} = 'all'
                then 1
            when
                cast(lower(IS_EXTERNAL_EMAIL_CONNECTED) as string) = {{EXTERNAL_EMAIL_RULE}}
                then 1
            else 0
        end as external_email_filter

    from functional.growth_engine.dim_growth_engine_customer_accounts as a
    where a.is_trial = true
),

accounts_funnel as (
    select 
        zendesk_account_id, 
        CRM_ACCOUNT_ID,
        market_segment, 
        sales_model, 
        TRIAL_AGE,
        TRIAL_SKU_NAMES,
        TOTAL_TICKETS,
        TOTAL_SOLVED_TICKETS,
        TOTAL_SAMPLE_TICKETS
    from accounts
    where 
        trial_account_age_filter = 1
        and employee_filter = 1
        and region_filter = 1
        and tickets_filter = 1
        and msg_tickets_filter = 1
        and web_tickets_filter = 1
        and chat_tickets_filter = 1
        and talk_tickets_filter = 1
        and hc_filter = 1
        and sample_ticket_filter = 1
        and external_email_filter = 1
)

funnel as (
    select 
        count(*) tot_obs,
        sum(trial_account_age_filter) trial_account_age_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 then 1 else 0 end) employee_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 and region_filter = 1 then 1 else 0 end) region_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 and region_filter = 1 and tickets_filter = 1 then 1 else 0 end) tickets_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 and region_filter = 1 and tickets_filter = 1 and msg_tickets_filter = 1 then 1 else 0 end) msg_tickets_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 and region_filter = 1 and tickets_filter = 1 and msg_tickets_filter = 1 and web_tickets_filter = 1 then 1 else 0 end) web_tickets_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 and region_filter = 1 and tickets_filter = 1 and msg_tickets_filter = 1 and web_tickets_filter = 1 and chat_tickets_filter = 1 then 1 else 0 end) chat_tickets_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 and region_filter = 1 and tickets_filter = 1 and msg_tickets_filter = 1 and web_tickets_filter = 1 and chat_tickets_filter = 1 and talk_tickets_filter = 1 then 1 else 0 end) talk_tickets_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 and region_filter = 1 and tickets_filter = 1 and msg_tickets_filter = 1 and web_tickets_filter = 1 and chat_tickets_filter = 1 and talk_tickets_filter = 1 and hc_filter = 1 then 1 else 0 end) hc_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 and region_filter = 1 and tickets_filter = 1 and msg_tickets_filter = 1 and web_tickets_filter = 1 and chat_tickets_filter = 1 and talk_tickets_filter = 1 and hc_filter = 1 and sample_ticket_filter = 1 then 1 else 0 end) sample_ticket_filter,
        sum(case when trial_account_age_filter = 1 and employee_filter = 1 and region_filter = 1 and tickets_filter = 1 and msg_tickets_filter = 1 and web_tickets_filter = 1 and chat_tickets_filter = 1 and talk_tickets_filter = 1 and hc_filter = 1 and sample_ticket_filter = 1 and external_email_filter = 1 then 1 else 0 end) external_email_filter
    from accounts
)




