SELECT
    c.customer_id,
    c.name AS customer_name,
    c.industry,
    c.signup_date,
    c.last_interaction_date,
    t.transaction_id,
    t.region_id,
    t.product_id,
    t.transaction_date,
    t.amount,
    t.discount_offered,
    t.payment_method,
    t.pricing_model,
    p.product_name,
    r.region_name,  
    r.country

FROM {{ source('source_apex', 'kenya_customers') }} AS c
JOIN {{ source('source_apex', 'kenya_transactions') }} AS t
ON c.customer_id = t.customer_id
JOIN {{ source('source_apex', 'products') }} AS p  -- Removed extra semicolon
ON t.product_id = p.product_id
JOIN {{ source('source_apex', 'regions') }} AS r  -- Added alias for 'regions'
ON t.region_id = r.region_id
