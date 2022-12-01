{{
    config(
        materialized='incremental'
    )
}} /* materialized has incremental in order to be able to apply incremental data loading process.
I wouldn't agree using an incremental load for now, we have few data, I would have started with materialized as view when starts being slow to query
I would change it to materialized as table, and when starts being slow transforming the data I would change it to materialized as incremental*/
select 
    parse_json(source) as json,
    to_timestamp(replace(json:time:updated, ' UTC'), 'MON DD, YYYY HH24:MI:SS') as time_updated_utc, -- delete UTC to fit as timestamp data type
    json:time:updatedISO::timestamp as time_updated_iso,
    to_timestamp(replace(replace(json:time:updateduk, ' GMT'), ' at'), 'MON DD, YYYY HH24:MI') as time_updated_uk, --same as UTC with GMT
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
    CONVERT_TIMEZONE( 'UTC' ,'Europe/Madrid', time_updated_utc ) as time_updated_spain, --time of the BTC price in the Europe/Madrid timezone
    to_timestamp(to_varchar(time_updated_utc, 'YYYY-MM-DD"T"HH24')) as time_updated_hour --needed for the incremental load process
from {{ source('data', 'json') }}  
{% if is_incremental() %} --executes if is incremental (is incremental except first and full-refresh loads)
  where time_updated_hour > (select max(time_updated_hour) from {{ this }}) --loads only new data (data which date is newer than theoldest )
{% endif %} --in snowflake you can use in the where clause a field you have created in the select unlike other DB as PostgresSQL