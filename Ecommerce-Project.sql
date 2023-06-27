--Q1 calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) as month,
SUM(totals.visits) as visits, 
SUM(totals.pageviews) as pageviews, 
SUM(totals.transactions) as transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month 
ORDER BY month;

--Q2 Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
SELECT DISTINCT trafficSource.source, 
      SUM(totals.visits) as total_visits,
      SUM(totals.bounces) as total_no_of_bounces,
      (SUM(totals.bounces) ) / (SUM(totals.visits) ) *100.0 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix BETWEEN '0701' AND '0731' 
GROUP BY trafficSource.source
ORDER BY total_visits DESC;

--Q3 Revenue by traffic source by week, by month in June 2017
SELECT  'Month' as timetpye,
        FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) AS time,
        trafficSource.source,
        ROUND(SUM(productRevenue)/1000000, 4) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE _table_suffix BETWEEN '0601' AND '0630' 
AND  productRevenue IS NOT NULL 
GROUP BY timetpye, time , trafficSource.source
UNION ALL 
SELECT  'Week' as timetpye,
        FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) AS time,
        trafficSource.source,
        ROUND(SUM(productRevenue)/1000000, 4) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE _table_suffix BETWEEN '0601' AND '0630'
AND  productRevenue IS NOT NULL 
GROUP BY timetpye, time , trafficSource.source
ORDER BY time ASC;

--Q4 Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
WITH  pageviews as (
      SELECT FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) as month,
        (SUM(totals.pageviews)/ COUNT(DISTINCT fullVisitorId)) as avg_pageviews_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE _table_suffix BETWEEN '0601' AND '0731'
      AND totals.transactions >=1 AND productRevenue is not null
      GROUP BY month
 ) ,
      non_pageviews as (
      SELECT FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) as month,
       (SUM(totals.pageviews)/ COUNT(DISTINCT fullVisitorId)) as avg_pageviews_non_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) hits,
      UNNEST (hits.product) product
      WHERE _table_suffix BETWEEN '0601' AND '0731'
      AND totals.transactions is null AND productRevenue is null
      GROUP BY month 
)
SELECT p.month, 
      p.avg_pageviews_purchase,
      n.avg_pageviews_non_purchase
FROM pageviews as p
LEFT JOIN non_pageviews as n 
USING(month);

--Q5  Average number of transactions per user that made a purchase in July 2017
SELECT FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) as month,
(SUM(totals.transactions) / COUNT(DISTINCT fullVisitorId )) as avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE _table_suffix BETWEEN '0701' AND '0731'
AND productRevenue is not null
GROUP BY month;

--Q6 Average amount of money spent per session. Only include purchaser data in July 2017
SELECT  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) as month,
       SUM(productRevenue) / SUM(totals.visits)/ 1000000 as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE _table_suffix BETWEEN '0701' AND '0731'
 AND totals.transactions is not null AND productRevenue is not null
GROUP BY  month;

--Q7 Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
SELECT  product.v2productname as other_purchased_product,
        SUM(product.productQuantity) as quantity
FROM	`bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,	
UNNEST (hits) hits,	
UNNEST (hits.product) product	
WHERE product.v2productname != "YouTube Men's Vintage Henley"
AND fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and product.productRevenue is not null )
AND product.productRevenue is not null
GROUP BY other_purchased_product
ORDER BY quantity desc;

--Q8 Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
left join add_to_cart a on pv.month = a.month
left join purchase p on pv.month = p.month
order by pv.month;