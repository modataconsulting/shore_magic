{{ config(materialized='table') }}

with ads_insights as(
    select
        date_start as date,
        account_name,
        campaign_name,
        adset_name,
        ad_name,
        clicks,
        impressions,
        spend,
        _airbyte_ads_insights_hashid
    from AIRBYTE.ads_insights
), temp_ads_insights_actions as (
    with temp as (
        select
            _airbyte_ads_insights_hashid,
            value,
            action_type
        from AIRBYTE.ads_insights_actions
    ) 
    select * 
    from (select * from temp) 
    pivot (
        sum(value) for action_type in ("post_engagement", "video_view", "link_click", "landing_page_view", "view_content", "post_reaction", "post", "add_to_cart", "purchase", "initiate_checkout", "comment")
    ) 
), ads_insights_actions as (
    select 
        _airbyte_ads_insights_hashid,
        sum(link_click) as link_click, 
        sum(landing_page_view) as landing_page_view, 
        sum(add_to_cart) as add_to_cart, 
        sum(purchase) as purchase
    from temp_ads_insights_actions
    group by _airbyte_ads_insights_hashid
), report as(
    select
        ads_insights.date,
        ads_insights.account_name,
        ads_insights.campaign_name,
        ads_insights.adset_name,
        ads_insights.ad_name,
        ads_insights.clicks,
        ads_insights.impressions,
        ads_insights.spend,
        ads_insights_actions.link_click,
        ads_insights_actions.landing_page_view,
        ads_insights_actions.add_to_cart,
        ads_insights_actions.purchase
    from ads_insights
    right join ads_insights_actions
        on ads_insights._airbyte_ads_insights_hashid = ads_insights_actions._airbyte_ads_insights_hashid
)

select *
from report