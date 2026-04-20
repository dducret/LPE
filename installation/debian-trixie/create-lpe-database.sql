\set ON_ERROR_STOP on

\if :{?db_name}
\else
\set db_name lpe
\endif

\if :{?db_user}
\else
\set db_user lpe
\endif

\if :{?db_password}
\else
\echo db_password must be provided with -v db_password=...
\quit 1
\endif

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = :'db_user'
    ) THEN
        EXECUTE format(
            'CREATE ROLE %I LOGIN PASSWORD %L',
            :'db_user',
            :'db_password'
        );
    END IF;
END
$$;

SELECT format(
    'CREATE DATABASE %I OWNER %I',
    :'db_name',
    :'db_user'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = :'db_name'
)
\gexec
