select  hour, 
        count(date) as times_lowest_daily_price
from (  select
            date(time_updated_spain) as date,
            split_part(min(to_varchar(eur_rate_float, '000000.00000')||' '||hour(time_updated_spain)), ' ',-1) as hour
        from {{ ref('stg_btc_price')}}
        group by date)
group by hour order by hour