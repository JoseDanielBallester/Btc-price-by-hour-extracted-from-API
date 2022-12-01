select  hour, 
        count(date) as times_lowest_daily_price
from (  select
            date(time_updated_spain) as date, --subtract the date from the raw data datatimstamp field
            split_part(min(to_varchar(eur_rate_float, '000000.00000')||' '||hour(time_updated_spain)), ' ',-1) as hour
        from {{ ref('stg_btc_price')}}
        group by date)
group by hour order by hour

/*
In line 5 I concatenate the price with the hour (I subtract the hour from the raw datatimestamp field), I change the
format to ensure that when I aply the min function is going to choose the one with the minimun price, this works 
because the string is compared char by char, the split_part function takes the hour part. As I'm grouping by the date, I
have for each date the hour with the minumun value, this for the subquery (this method is more trambolic than the
classic row_number() over partition by... but is more efficient as the window funtions are usualy not very performant).
In the main query I just have to group by hour and count the date, and I have how many times has been each hour the 
lowest price in the day (only appeats the hours which have been at least 1 time the minumun price in a day)
/*
