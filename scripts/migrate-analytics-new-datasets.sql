-- Migrates an existing Drivee Analytics database to the new CSV dataset layout.
-- Run against the target database, then import CSV files with headers, for example:
-- \copy analytics.incity from 'incity.csv' with (format csv, header true)
-- \copy analytics.driver_detail from 'driver_detail.csv' with (format csv, header true)
-- \copy analytics.pass_detail from 'pass_detail.csv' with (format csv, header true)

create extension if not exists pgcrypto;

create schema if not exists analytics;

do $$
begin
    if to_regclass('analytics.incity') is null
       and to_regclass('analytics.incity_orders') is not null then
        alter table analytics.incity_orders rename to incity;
    end if;
end $$;

create table if not exists analytics.incity (
    city_id text not null,
    order_id text not null,
    tender_id text,
    user_id text,
    driver_id text,
    offset_hours integer,
    status_order text,
    status_tender text,
    order_timestamp timestamptz,
    tender_timestamp timestamptz,
    driveraccept_timestamp timestamptz,
    driverarrived_timestamp timestamptz,
    driverstarttheride_timestamp timestamptz,
    driverdone_timestamp timestamptz,
    clientcancel_timestamp timestamptz,
    drivercancel_timestamp timestamptz,
    order_modified_local timestamptz,
    cancel_before_accept_local timestamptz,
    distance_in_meters numeric(14,2),
    duration_in_seconds integer,
    price_order_local numeric(14,2),
    price_tender_local numeric(14,2),
    price_start_local numeric(14,2)
);

alter table analytics.incity add column if not exists city_id text;
alter table analytics.incity add column if not exists order_id text;
alter table analytics.incity add column if not exists tender_id text;
alter table analytics.incity add column if not exists user_id text;
alter table analytics.incity add column if not exists driver_id text;
alter table analytics.incity add column if not exists offset_hours integer;
alter table analytics.incity add column if not exists status_order text;
alter table analytics.incity add column if not exists status_tender text;
alter table analytics.incity add column if not exists order_timestamp timestamptz;
alter table analytics.incity add column if not exists tender_timestamp timestamptz;
alter table analytics.incity add column if not exists driveraccept_timestamp timestamptz;
alter table analytics.incity add column if not exists driverarrived_timestamp timestamptz;
alter table analytics.incity add column if not exists driverstarttheride_timestamp timestamptz;
alter table analytics.incity add column if not exists driverdone_timestamp timestamptz;
alter table analytics.incity add column if not exists clientcancel_timestamp timestamptz;
alter table analytics.incity add column if not exists drivercancel_timestamp timestamptz;
alter table analytics.incity add column if not exists order_modified_local timestamptz;
alter table analytics.incity add column if not exists cancel_before_accept_local timestamptz;
alter table analytics.incity add column if not exists distance_in_meters numeric(14,2);
alter table analytics.incity add column if not exists duration_in_seconds integer;
alter table analytics.incity add column if not exists price_order_local numeric(14,2);
alter table analytics.incity add column if not exists price_tender_local numeric(14,2);
alter table analytics.incity add column if not exists price_start_local numeric(14,2);

create table if not exists analytics.driver_detail (
    city_id text not null,
    driver_id text not null,
    tender_date_part date not null,
    driver_reg_date date,
    orders integer not null default 0,
    orders_cnt_with_tenders integer not null default 0,
    orders_cnt_accepted integer not null default 0,
    rides_count integer not null default 0,
    rides_time_sum_seconds numeric(14,2) not null default 0,
    online_time_sum_seconds numeric(14,2) not null default 0,
    client_cancel_after_accept integer not null default 0
);

create table if not exists analytics.pass_detail (
    city_id text not null,
    user_id text not null,
    order_date_part date not null,
    user_reg_date date,
    orders_count integer not null default 0,
    orders_cnt_with_tenders integer not null default 0,
    orders_cnt_accepted integer not null default 0,
    rides_count integer not null default 0,
    rides_time_sum_seconds numeric(14,2) not null default 0,
    online_time_sum_seconds numeric(14,2) not null default 0,
    client_cancel_after_accept integer not null default 0
);

do $$
declare
    view_name text;
    relation_kind "char";
begin
    foreach view_name in array array[
        'v_ride_metrics',
        'v_incity_orders_latest',
        'v_driver_daily_metrics',
        'v_passenger_daily_metrics'
    ]
    loop
        select c.relkind
        into relation_kind
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'analytics'
          and c.relname = view_name;

        if relation_kind = 'v' then
            execute format('drop view analytics.%I cascade', view_name);
        elsif relation_kind = 'm' then
            execute format('drop materialized view analytics.%I cascade', view_name);
        end if;
    end loop;
end $$;

create materialized view if not exists analytics.v_incity_orders_latest as
with ranked as (
    select
        src.*,
        row_number() over (
            partition by src.order_id
            order by
                coalesce(
                    src.order_modified_local,
                    src.driverdone_timestamp,
                    src.clientcancel_timestamp,
                    src.drivercancel_timestamp,
                    src.driverstarttheride_timestamp,
                    src.driveraccept_timestamp,
                    src.tender_timestamp,
                    src.order_timestamp
                ) desc nulls last,
                src.tender_timestamp desc nulls last,
                src.tender_id desc
        ) as rn
    from analytics.incity src
),
order_level as (
    select
        coalesce(
            (order_timestamp + make_interval(hours => coalesce(offset_hours, 0)))::date,
            order_modified_local::date,
            (tender_timestamp + make_interval(hours => coalesce(offset_hours, 0)))::date
        ) as stat_date,
        city_id as city,
        coalesce(nullif(trim(status_order), ''), 'unknown') as status_order,
        coalesce(nullif(trim(status_tender), ''), 'unknown') as status_tender,
        order_id,
        tender_id,
        user_id,
        driver_id,
        distance_in_meters,
        duration_in_seconds,
        coalesce(price_order_local, price_tender_local, price_start_local, 0)::numeric(14,2) as final_price_local,
        case
            when driverdone_timestamp is not null then 1
            when lower(coalesce(status_order, '')) ~ '(done|completed|complete|finish|finished|success|successful)' then 1
            else 0
        end as completed_orders,
        case
            when driverdone_timestamp is not null then 0
            when clientcancel_timestamp is not null or drivercancel_timestamp is not null or cancel_before_accept_local is not null then 1
            when lower(coalesce(status_order, '')) ~ '(cancel|canceled|cancelled)' then 1
            when lower(coalesce(status_tender, '')) ~ '(cancel|canceled|cancelled)' then 1
            else 0
        end as cancelled_orders
    from ranked
    where rn = 1
)
select
    stat_date,
    city,
    status_order,
    status_tender,
    order_id,
    tender_id,
    user_id,
    driver_id,
    distance_in_meters,
    duration_in_seconds,
    final_price_local,
    completed_orders,
    cancelled_orders,
    1 as total_orders
from order_level;

create materialized view if not exists analytics.v_ride_metrics as
select
    stat_date,
    city,
    status_order,
    status_tender,
    sum(completed_orders)::integer as completed_orders,
    sum(cancelled_orders)::integer as cancelled_orders,
    count(*)::integer as total_orders,
    round(sum(case when completed_orders = 1 then final_price_local else 0 end)::numeric, 2) as gross_revenue_local,
    round(
        sum(case when completed_orders = 1 then final_price_local else 0 end)::numeric
        / nullif(sum(completed_orders), 0),
        2
    ) as avg_price_local,
    round(
        sum(case when completed_orders = 1 then coalesce(distance_in_meters, 0) else 0 end)::numeric
        / nullif(sum(completed_orders), 0),
        2
    ) as avg_distance_meters,
    round(
        sum(case when completed_orders = 1 then coalesce(duration_in_seconds, 0) else 0 end)::numeric
        / nullif(sum(completed_orders), 0),
        2
    ) as avg_duration_seconds
from analytics.v_incity_orders_latest
where stat_date is not null
group by stat_date, city, status_order, status_tender;

create materialized view if not exists analytics.v_driver_daily_metrics as
select
    tender_date_part as stat_date,
    city_id as city,
    count(distinct driver_id)::integer as active_drivers,
    count(distinct case when driver_reg_date = tender_date_part then driver_id end)::integer as new_drivers,
    sum(orders)::integer as total_orders,
    sum(orders_cnt_with_tenders)::integer as orders_with_tenders,
    sum(orders_cnt_accepted)::integer as accepted_orders,
    sum(rides_count)::integer as completed_rides,
    sum(client_cancel_after_accept)::integer as client_cancel_after_accept,
    round(sum(rides_time_sum_seconds)::numeric, 2) as rides_time_sum_seconds,
    round(sum(online_time_sum_seconds)::numeric, 2) as online_time_sum_seconds,
    round(sum(orders_cnt_accepted)::numeric / nullif(sum(orders_cnt_with_tenders), 0), 4) as acceptance_rate,
    round(sum(rides_count)::numeric / nullif(sum(orders), 0), 4) as completion_rate,
    round(sum(client_cancel_after_accept)::numeric / nullif(sum(orders_cnt_accepted), 0), 4) as cancel_after_accept_rate,
    round(sum(rides_time_sum_seconds)::numeric / nullif(sum(rides_count), 0), 2) as avg_ride_time_seconds,
    round(sum(online_time_sum_seconds)::numeric / nullif(count(distinct driver_id), 0), 2) as avg_online_time_seconds_per_driver
from analytics.driver_detail
group by tender_date_part, city_id;

create materialized view if not exists analytics.v_passenger_daily_metrics as
select
    order_date_part as stat_date,
    city_id as city,
    count(distinct user_id)::integer as active_passengers,
    count(distinct case when user_reg_date = order_date_part then user_id end)::integer as new_passengers,
    sum(orders_count)::integer as total_orders,
    sum(orders_cnt_with_tenders)::integer as orders_with_tenders,
    sum(orders_cnt_accepted)::integer as accepted_orders,
    sum(rides_count)::integer as completed_rides,
    sum(client_cancel_after_accept)::integer as client_cancel_after_accept,
    round(sum(rides_time_sum_seconds)::numeric, 2) as rides_time_sum_seconds,
    round(sum(online_time_sum_seconds)::numeric, 2) as online_time_sum_seconds,
    round(sum(orders_cnt_accepted)::numeric / nullif(sum(orders_cnt_with_tenders), 0), 4) as acceptance_rate,
    round(sum(rides_count)::numeric / nullif(sum(orders_count), 0), 4) as completion_rate,
    round(sum(client_cancel_after_accept)::numeric / nullif(sum(orders_cnt_accepted), 0), 4) as cancel_after_accept_rate,
    round(sum(rides_time_sum_seconds)::numeric / nullif(sum(rides_count), 0), 2) as avg_ride_time_seconds,
    round(sum(online_time_sum_seconds)::numeric / nullif(count(distinct user_id), 0), 2) as avg_online_time_seconds_per_passenger
from analytics.pass_detail
group by order_date_part, city_id;

create index if not exists idx_incity_order_timestamp on analytics.incity (order_timestamp);
create index if not exists idx_incity_modified_local on analytics.incity (order_modified_local);
create index if not exists idx_incity_city_order_ts on analytics.incity (city_id, order_timestamp);
create index if not exists idx_incity_statuses on analytics.incity (status_order, status_tender);
create index if not exists idx_incity_driver_ts on analytics.incity (driver_id, order_timestamp);
create index if not exists idx_incity_order_tender on analytics.incity (order_id, tender_id);
create index if not exists idx_driver_detail_date_city on analytics.driver_detail (tender_date_part, city_id);
create index if not exists idx_driver_detail_driver_date on analytics.driver_detail (driver_id, tender_date_part);
create index if not exists idx_pass_detail_date_city on analytics.pass_detail (order_date_part, city_id);
create index if not exists idx_pass_detail_user_date on analytics.pass_detail (user_id, order_date_part);
create index if not exists idx_v_incity_orders_latest_stat_date on analytics.v_incity_orders_latest (stat_date);
create index if not exists idx_v_incity_orders_latest_city_date on analytics.v_incity_orders_latest (city, stat_date);
create index if not exists idx_v_ride_metrics_stat_date on analytics.v_ride_metrics (stat_date);
create index if not exists idx_v_ride_metrics_city_date on analytics.v_ride_metrics (city, stat_date);
create index if not exists idx_v_driver_daily_metrics_stat_date on analytics.v_driver_daily_metrics (stat_date);
create index if not exists idx_v_driver_daily_metrics_city_date on analytics.v_driver_daily_metrics (city, stat_date);
create index if not exists idx_v_passenger_daily_metrics_stat_date on analytics.v_passenger_daily_metrics (stat_date);
create index if not exists idx_v_passenger_daily_metrics_city_date on analytics.v_passenger_daily_metrics (city, stat_date);

do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'analytics_readonly') then
        create role analytics_readonly login password 'analytics_demo';
    end if;
exception
    when insufficient_privilege then
        null;
end $$;

grant usage on schema analytics to analytics_readonly;
grant select on analytics.incity to analytics_readonly;
grant select on analytics.driver_detail to analytics_readonly;
grant select on analytics.pass_detail to analytics_readonly;
grant select on analytics.v_incity_orders_latest to analytics_readonly;
grant select on analytics.v_ride_metrics to analytics_readonly;
grant select on analytics.v_driver_daily_metrics to analytics_readonly;
grant select on analytics.v_passenger_daily_metrics to analytics_readonly;
