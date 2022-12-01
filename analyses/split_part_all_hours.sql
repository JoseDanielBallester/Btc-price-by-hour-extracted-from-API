with formating as(
    select
        date(time_updated_spain) as date,
        hour(time_updated_spain) as hour,
        eur_rate_float as price
    from {{ ref('stg_btc_price')}}    
),--inside a with in order to reutilice this transformation  and do it one time insted of two

minimun_price as(
    select 
        date,
        split_part(min(to_varchar(price, '000000.00000')||' '||hour), ' ',-1) as hour 
    from formating 
    group by date
) --this subquery is already explained in the previous script, inside a with for readability

select  
    hour, 
    count(date) as times_lowest_daily_price
from  (select distinct hour from formating) left join minimun_price using(hour)
group by hour order by hour

/*
In order to have also the hours that haven't been at least 1 time the minumun price in a day
we make a left join with a list of all the hours (obtained with a select distinct of the hour field
from the first table) and the previous table. We group by hour and count the date, that will be 
null for the hours which have been lees than 1 time the minimun price in a day and will be shown as 0, 
and the other hours will the numer of times that has been each hour the lowest price in the day.
*/