-- Import new CSV datasets into the analytics schema from pgAdmin Query Tool.
-- Replace paths below if your CSV files are in another folder.
-- Important: PostgreSQL server must be able to read these paths.
-- For Windows paths, use forward slashes: D:/Download/incity.csv

copy analytics.incity (
    city_id,
    order_id,
    tender_id,
    user_id,
    driver_id,
    offset_hours,
    status_order,
    status_tender,
    order_timestamp,
    tender_timestamp,
    driveraccept_timestamp,
    driverarrived_timestamp,
    driverstarttheride_timestamp,
    driverdone_timestamp,
    clientcancel_timestamp,
    drivercancel_timestamp,
    order_modified_local,
    cancel_before_accept_local,
    distance_in_meters,
    duration_in_seconds,
    price_order_local,
    price_tender_local,
    price_start_local
) from 'D:/Download/incity.csv' with (format csv, header true, null '', encoding 'UTF8');

copy analytics.driver_detail (
    city_id,
    driver_id,
    tender_date_part,
    driver_reg_date,
    orders,
    orders_cnt_with_tenders,
    orders_cnt_accepted,
    rides_count,
    rides_time_sum_seconds,
    online_time_sum_seconds,
    client_cancel_after_accept
) from 'D:/Download/driver_detail.csv' with (format csv, header true, null '', encoding 'UTF8');

copy analytics.pass_detail (
    city_id,
    user_id,
    order_date_part,
    user_reg_date,
    orders_count,
    orders_cnt_with_tenders,
    orders_cnt_accepted,
    rides_count,
    rides_time_sum_seconds,
    online_time_sum_seconds,
    client_cancel_after_accept
) from 'D:/Download/pass_detail.csv' with (format csv, header true, null '', encoding 'UTF8');

refresh materialized view analytics.v_incity_orders_latest;
refresh materialized view analytics.v_ride_metrics;
refresh materialized view analytics.v_driver_daily_metrics;
refresh materialized view analytics.v_passenger_daily_metrics;

analyze analytics.incity;
analyze analytics.driver_detail;
analyze analytics.pass_detail;
analyze analytics.v_incity_orders_latest;
analyze analytics.v_ride_metrics;
analyze analytics.v_driver_daily_metrics;
analyze analytics.v_passenger_daily_metrics;
