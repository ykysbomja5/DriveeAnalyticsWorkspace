create extension if not exists pgcrypto;

create schema if not exists analytics;
create schema if not exists app;

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

create table if not exists app.departments (
    id bigserial primary key,
    name text not null unique,
    created_at timestamptz not null default now()
);

create table if not exists app.users (
    id bigserial primary key,
    email text not null unique,
    password_hash text not null,
    full_name text not null,
    role text not null default 'user' check (role in ('root', 'manager', 'user')),
    department_id bigint references app.departments(id) on delete set null,
    is_active boolean not null default true,
    is_approved boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists app.user_sessions (
    id bigserial primary key,
    user_id bigint not null references app.users(id) on delete cascade,
    token_hash text not null unique,
    expires_at timestamptz not null,
    created_at timestamptz not null default now()
);

create table if not exists app.department_access (
    id bigserial primary key,
    user_id bigint not null references app.users(id) on delete cascade,
    department_id bigint not null references app.departments(id) on delete cascade,
    granted_by bigint references app.users(id) on delete set null,
    created_at timestamptz not null default now(),
    unique (user_id, department_id)
);

create table if not exists app.saved_reports (
    id bigserial primary key,
    name text not null,
    query_text text not null,
    sql_text text not null,
    intent jsonb not null,
    preview_json jsonb,
    result_json jsonb,
    provider text,
    source text not null default 'manual',
    owner_name text not null default '',
    owner_department text not null default '',
    owner_user_id bigint references app.users(id) on delete set null,
    is_public boolean not null default false,
    template_id bigint,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists app.report_templates (
    id bigserial primary key,
    name text not null,
    description text not null default '',
    query_text text not null,
    owner_name text not null default '',
    owner_department text not null default '',
    owner_user_id bigint references app.users(id) on delete set null,
    is_public boolean not null default false,
    schedule_enabled boolean not null default false,
    schedule_day_of_week integer,
    schedule_hour integer,
    schedule_minute integer,
    schedule_timezone text not null default 'Europe/Moscow',
    last_run_at timestamptz,
    last_scheduled_for timestamptz,
    last_status text not null default 'idle',
    last_error_text text,
    last_result_count integer not null default 0,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint report_templates_schedule_day check (schedule_day_of_week is null or schedule_day_of_week between 0 and 6),
    constraint report_templates_schedule_hour check (schedule_hour is null or schedule_hour between 0 and 23),
    constraint report_templates_schedule_minute check (schedule_minute is null or schedule_minute between 0 and 59)
);

create table if not exists app.chat_rooms (
    id bigserial primary key,
    title text not null,
    created_by bigint not null references app.users(id) on delete cascade,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists app.chat_room_members (
    room_id bigint not null references app.chat_rooms(id) on delete cascade,
    user_id bigint not null references app.users(id) on delete cascade,
    added_by bigint references app.users(id) on delete set null,
    created_at timestamptz not null default now(),
    primary key (room_id, user_id)
);

create table if not exists app.chat_messages (
    id bigserial primary key,
    room_id bigint not null references app.chat_rooms(id) on delete cascade,
    sender_id bigint not null references app.users(id) on delete cascade,
    body text not null default '',
    attachment_type text check (attachment_type is null or attachment_type in ('report', 'template')),
    attachment_id bigint,
    attachment_title text not null default '',
    created_at timestamptz not null default now()
);

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'saved_reports_template_id_fkey'
    ) then
        alter table app.saved_reports
            add constraint saved_reports_template_id_fkey
            foreign key (template_id) references app.report_templates(id) on delete set null;
    end if;
end $$;

create table if not exists app.report_runs (
    id bigserial primary key,
    report_id bigint not null references app.saved_reports(id) on delete cascade,
    executed_at timestamptz not null default now(),
    status text not null,
    row_count integer not null default 0,
    error_text text
);

create table if not exists app.query_logs (
    id bigserial primary key,
    query_text text not null,
    intent jsonb not null,
    sql_text text,
    confidence numeric(4,2) not null,
    status text not null,
    latency_ms bigint not null default 0,
    error_text text,
    created_at timestamptz not null default now()
);

create table if not exists app.semantic_terms (
    id bigserial primary key,
    term text not null,
    kind text not null,
    canonical_value text not null,
    description text not null,
    unique (term, kind)
);

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

create index if not exists idx_query_logs_created_at on app.query_logs (created_at desc);
create index if not exists idx_users_department on app.users (department_id, role);
create index if not exists idx_user_sessions_user on app.user_sessions (user_id, expires_at desc);
create index if not exists idx_department_access_user on app.department_access (user_id, department_id);
create index if not exists idx_chat_members_user on app.chat_room_members (user_id, room_id);
create index if not exists idx_chat_messages_room on app.chat_messages (room_id, id desc);
create index if not exists idx_report_runs_report_id on app.report_runs (report_id, executed_at desc);
create index if not exists idx_saved_reports_template_id on app.saved_reports (template_id, updated_at desc);
create index if not exists idx_saved_reports_owner on app.saved_reports (owner_name, updated_at desc);
create index if not exists idx_saved_reports_owner_user on app.saved_reports (owner_user_id, updated_at desc);
create index if not exists idx_saved_reports_public_department on app.saved_reports (is_public, owner_department, updated_at desc);
create index if not exists idx_report_templates_schedule on app.report_templates (schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute);
create index if not exists idx_report_templates_owner on app.report_templates (owner_name, updated_at desc);
create index if not exists idx_report_templates_owner_user on app.report_templates (owner_user_id, updated_at desc);
create index if not exists idx_report_templates_public_department on app.report_templates (is_public, owner_department, updated_at desc);

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
