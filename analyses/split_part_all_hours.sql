with formating as(
    select
        date(time_updated_spain) as date,
        hour(time_updated_spain) as hour,
        eur_rate_float as price
    from {{ ref('stg_btc_price')}}    
),

minimun_price as(
    select 
        date,
        split_part(min(to_varchar(price, '000000.00000')||' '||hour), ' ',-1) as hour 
    from formating 
    group by date
)

select  
    hour, 
    count(date) as times_lowest_daily_price
from  (select distinct hour from formating) left join minimun_price using(hour)
group by hour order by hour