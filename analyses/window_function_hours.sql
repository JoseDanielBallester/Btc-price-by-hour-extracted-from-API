select  
    hour, 
    count(date) as times_lowest_daily_price
from (  select
            date(time_updated_spain) as date, --subtract the date from the raw data datatimstamp field
            hour(time_updated_spain) as hour --subtract the hour from the raw data datatimstamp field
        from {{ ref('stg_btc_price')}}
        qualify row_number() over (partition by date order by eur_rate_float) =1)
group by hour order by hour

/*
In line 8 I use the row_number divided by date and order by price (lowest first) with the qualify to
filter all the lines that are are not the lowest price of that day, in this lines I have the date and the 
hour of that daily lowest price, this for the subquery.
In the main query I just have to group by hour and count the date, and I have how many times has been each hour the 
lowest price in the day (only appears the hours which have been at least 1 time the minumun price in a day).
/*