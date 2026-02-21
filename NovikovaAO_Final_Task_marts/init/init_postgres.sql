create schema if not exists etl;

create table if not exists etl.user_sessions (
    session_id text primary key,
    user_id text,
    start_time timestamp,
    end_time timestamp,
    duration_minutes int,
    device text
);

create table if not exists etl.session_pages (
    session_id text,
    page text
);

create table if not exists etl.support_tickets (
    ticket_id text primary key,
    user_id text,
    status text,
    issue_type text,
    created_at timestamp,
    updated_at timestamp,
    resolution_hours numeric
);