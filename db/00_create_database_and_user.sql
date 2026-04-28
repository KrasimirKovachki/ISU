-- Run as a PostgreSQL superuser, for example:
-- psql -h 192.168.1.18 -U postgres -f db/00_create_database_and_user.sql
--
-- Optional psql variables:
-- psql -h 192.168.1.18 -U postgres \
--   -v app_db=skating_data \
--   -v app_user=skating_app \
--   -v app_password="'change_me_strong_password'" \
--   -f db/00_create_database_and_user.sql

\if :{?app_db}
\else
  \set app_db skating_data
\endif

\if :{?app_user}
\else
  \set app_user skating_app
\endif

\if :{?app_password}
\else
  \set app_password change_me_strong_password
\endif

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'app_user', :'app_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'app_user')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I ENCODING %L', :'app_db', :'app_user', 'UTF8')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'app_db')
\gexec

GRANT CONNECT ON DATABASE :"app_db" TO :"app_user";

\connect :app_db

CREATE SCHEMA IF NOT EXISTS ingest AUTHORIZATION :"app_user";
CREATE SCHEMA IF NOT EXISTS core AUTHORIZATION :"app_user";

GRANT USAGE, CREATE ON SCHEMA ingest TO :"app_user";
GRANT USAGE, CREATE ON SCHEMA core TO :"app_user";

ALTER DEFAULT PRIVILEGES FOR USER :"app_user" IN SCHEMA ingest
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"app_user";
ALTER DEFAULT PRIVILEGES FOR USER :"app_user" IN SCHEMA core
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"app_user";
ALTER DEFAULT PRIVILEGES FOR USER :"app_user" IN SCHEMA ingest
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO :"app_user";
ALTER DEFAULT PRIVILEGES FOR USER :"app_user" IN SCHEMA core
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO :"app_user";
