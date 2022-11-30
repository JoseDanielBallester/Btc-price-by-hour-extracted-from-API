{{
    config(
        materialized='incremental',
        unique_key='time_updated_hour'
    )
}}
select 
    parse_json(source) as json,
    to_timestamp(replace(json:time:updated, ' UTC'), 'MON DD, YYYY HH24:MI:SS') as time_updated_utc,
    json:time:updatedISO::timestamp as time_updated_iso,
    to_timestamp(replace(replace(json:time:updateduk, ' GMT'), ' at'), 'MON DD, YYYY HH24:MI') as time_updated_uk,
    json:disclaimer::varchar as disclaimer,
    json:chartName::varchar as chart_name,
    json:bpi:USD:code::varchar as usd_code,
    json:bpi:USD:symbol::varchar as usd_symbol,
    json:bpi:USD:rate::varchar as usd_rate,
    json:bpi:USD:description::varchar as usd_description,
    json:bpi:USD:rate_float::float as usd_rate_float,
    json:bpi:GBP:code::varchar as gbp_code,
    json:bpi:GBP:symbol::varchar as gbp_symbol,
    json:bpi:GBP:rate::varchar as gbp_rate,
    json:bpi:GBP:description::varchar as gbp_description,
    json:bpi:GBP:rate_float::float as gbp_rate_float,
    json:bpi:EUR:code::varchar as eur_code,
    json:bpi:EUR:symbol::varchar as eur_symbol,
    json:bpi:EUR:rate::varchar as eur_rate,
    json:bpi:EUR:description::varchar as eur_description,
    json:bpi:EUR:rate_float::float as eur_rate_float,
    CONVERT_TIMEZONE( 'UTC' ,'Europe/Madrid', time_updated_utc ) as time_updated_spain,
    to_timestamp(to_varchar(time_updated_utc, 'YYYY-MM-DD"T"HH24')) as time_updated_hour
from {{ source('data', 'json') }}

{% if is_incremental() %}

  where time_updated_hour > (select max(time_updated_hour) from {{ this }})

{% endif %}