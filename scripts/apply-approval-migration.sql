-- Apply the user approval migration
\c drivee_db

-- Add is_approved column to track admin approval for new users
alter table app.users add column if not exists is_approved boolean not null default false;

-- Set all existing users as approved by default
update app.users set is_approved = true where is_approved = false;
