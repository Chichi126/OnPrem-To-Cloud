SELECT
    region_id,
    country,
    region_name

FROM
    {{source('source_apex', 'regions')}}