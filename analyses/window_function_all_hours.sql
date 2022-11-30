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
        hour
    from formating
    qualify row_number() over (partition by date order by price) =1
)

select  
    hour, 
    count(date) as times_lowest_daily_price
from  (select distinct hour from formating) left join minimun_price using(hour)
group by hour order by hour