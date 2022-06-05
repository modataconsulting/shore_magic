with temp_ads_insights_actions as (
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
        sum(link_click) as link_click, 
        sum(landing_page_view) as landing_page_view, 
        sum(add_to_cart) as add_to_cart, 
        sum(purchase) as purchase
    from temp_ads_insights_actions
    group by _airbyte_ads_insights_hashid
)

select *
from ads_insights_actions