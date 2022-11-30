select  
    hour, 
    count(date) as times_lowest_daily_price
from (  select
            date(time_updated_spain) as date,
            hour(time_updated_spain) as hour
        from {{ ref('stg_btc_price')}}
        qualify row_number() over (partition by date order by eur_rate_float) =1)
group by hour order by hour
