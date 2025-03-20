SELECT 
    product_id,
    product_name
FROM
    {{source('source_apex', 'products')}}