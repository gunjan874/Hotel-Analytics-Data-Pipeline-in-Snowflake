create database hotel_db;

create or replace FILE FORMAT ff_csv
    type= "CSV"
    field_optionally_enclosed_by= '"'
    skip_header= 1
    null_if= ('NULL','null','')
    
// creating a stage for temporary holding the data
create or replace stage stg_hotel_bookings
    FILE_FORMAT = ff_csv;

// creating bronze table for storing the raw data

create table bronze_hotel_booking(
    booking_id String,
    hotel_id String,
    hotel_city String,
    customer_id String,
    customer_name String,
    customer_email String,
    check_in_date String,
    check_out_date String,
    room_type String,
    num_guests String,
    total_amount String,
    currency String,
    booking_status String
);


copy into bronze_hotel_booking from @stg_hotel_bookings
file_format = (format_name = ff_csv)
ON_ERROR = 'continue';

// since the table is loaded, we can select the details by limiting some rows
select * from bronze_hotel_booking limit 50;


// step 2 - putting the data in the silver layer -> cleaned data
// assignn the correct data types to columns

drop table silver_hotel_bookings;


create table silver_hotel_bookings(
    booking_id varchar,
    hotel_id varchar,
    hotel_city varchar,
    customer_id varchar,
    customer_name varchar,
    customer_email varchar,
    check_in_date date,
    check_out_date date,
    room_type varchar,
    num_guests integer,
    total_amount float,
    currency varchar,
    booking_status varchar
);

select customer_email from bronze_hotel_booking 
where not (customer_email like '%@%.%')
or customer_email is null;

select * from bronze_hotel_booking;

select total_amount from bronze_hotel_booking 
where try_to_number(total_amount) < 0 ;

select count(total_amount) from BRONZE_HOTEL_BOOKING
where  try_to_number(total_amount) < 0 ;

select count(distinct  booking_id  )from bronze_hotel_booking;

// checking whether the check in date is more or not than check out date
select check_in_date, check_out_date 
from bronze_hotel_booking 
where try_to_date(check_in_date)> try_to_date(check_out_date);

// counting the records where this kind of a scenario exists also doing transformations

SELECT COUNT(*) AS Invalid_dates_count
FROM bronze_hotel_booking
WHERE TRY_TO_DATE(check_in_date) > TRY_TO_DATE(check_out_date);

// finding typos in the booking status

select distinct booking_status 
from bronze_hotel_booking;

insert into silver_hotel_bookings
select 
booking_id,
hotel_id,
initcap(TRIM(hotel_city)) as hotel_city,
customer_id,
initcap(trim(customer_name)) as customer_name,
case
    when customer_email like '%@%.%' then lower (trim(customer_email))
    else null
end as customer_email,
try_to_date(nullif(check_in_date, '')) as check_in_date,
try_to_date(nullif(check_out_date, '')) as check_out_date,
room_type,
num_guests,
abs(try_to_number(total_amount)) as total_amount,
currency,
case 
    when lower(booking_status) in ('confirmeeed', 'confirmd') then 'Confirmed'
    else booking_status

end as booking_status
from bronze_hotel_booking
where
try_to_date(check_in_date) is not null 
and try_to_date(check_out_date) is not null
and try_to_date(check_out_date) >= try_to_date(check_in_date) // not deleted those values which contains errors we have just filtered out

select * from silver_hotel_bookings;

// to save the data in the final silver layer

create table gold_agg_daily_booking as
select 
    check_in_date as date,
    count(*) as total_booking,
    sum(total_amount) as total_revenue
from silver_hotel_bookings
group by check_in_date
order by date;


create table gold_agg_hotel_city_sales as 
select
    hotel_city,
    sum(total_amount) as total_revenue,
from silver_hotel_bookings
group by hotel_city
order by total_revenue desc;

// creating a table that contains all the kpis since we have not selected any as of now
// also we can make general visuals with the help of the kpis

create table gold_booking_clean 
as 
select
    booking_id ,
    hotel_id ,
    hotel_city ,
    customer_id ,
    customer_name ,
    customer_email ,
    check_in_date ,
    check_out_date ,
    room_type ,
    num_guests ,
    total_amount ,
    currency ,
    booking_status 
from silver_hotel_bookings


select * from gold_agg_daily_booking;
select * from gold_agg_hotel_city_sales;
select * from gold_booking_clean;


