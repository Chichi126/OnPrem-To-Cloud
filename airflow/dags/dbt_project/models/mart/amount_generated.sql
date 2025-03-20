SELECT 
    country,
    COUNT(customer_id) AS customer_count,
    SUM(amount) AS country_total,
    AVG(amount) AS avg_country_transaction

FROM {{ref('all_countries_transac')}}
GROUP BY country