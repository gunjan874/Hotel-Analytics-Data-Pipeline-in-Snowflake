-- Query for Montly Revenue:
  select date, total_revenue 
  from gold_agg_daily_booking
  order by date;


-- Query for Monthly Bookings
  select date, total_booking
  from gold_agg_daily_booking
  order by date

-- Query for Total Bookings:
  select count (*) as total_bookings
  from gold_booking_clean;

-- Query for Total Revenue:
  select sum(total_amount) as total_revenue
  from gold_booking_clean;

-- Query for Average Booking Value:
  // selecting the average booking value
  select avg(total_amount) as avg_booking_val
  from gold_booking_clean;

-- Query for Top 5 Cities by Revenue:
  select hotel_city, total_revenue
  from  GOLD_AGG_HOTEL_CITY_SALES
  where total_revenue is not null
  order by total_revenue desc
  limit 5;

-- Query for Booking By Status:
  select booking_status, count(*) as total_booked
  from gold_booking_clean
  group by booking_status;

-- Query for Booking by Room Type:
  // analyse bookings by type and status
  select room_type, count(*) as total_bookings
  from gold_booking_clean 
  group by room_type
  order by total_bookings desc;
