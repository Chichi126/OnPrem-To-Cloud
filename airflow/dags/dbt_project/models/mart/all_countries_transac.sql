{{config (materialized = 'table', schema = 'mart')}}

WITH nigeria AS (
    SELECT *
FROM {{ref ('stg_nigeria')}}
),

kenya AS (
    SELECT *
FROM {{ref ('stg_kenya')}}
),

ghana AS (
    SELECT *
FROM {{ref ('stg_ghana')}}
)


SELECT * FROM nigeria
UNION ALL
SELECT * FROM kenya
UNION ALL
SELECT * FROM ghana 
