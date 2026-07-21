BEGIN;

SET LOCAL search_path = pg_catalog, public;

DO $schema_version_transition$
DECLARE
    installed_schema_version TEXT;
BEGIN
    IF to_regclass('public.schema_metadata') IS NULL THEN
        RAISE EXCEPTION
            'LPE schema metadata is missing; this update only supports 0.5.0-sql-v1 and 0.5.1-sql';
    END IF;

    SELECT schema_version
    INTO installed_schema_version
    FROM public.schema_metadata
    WHERE singleton = TRUE;

    IF installed_schema_version IS DISTINCT FROM '0.5.0-sql-v1'
       AND installed_schema_version IS DISTINCT FROM '0.5.1-sql' THEN
        RAISE EXCEPTION
            'unsupported LPE schema version: expected 0.5.0-sql-v1 or 0.5.1-sql, found %',
            COALESCE(installed_schema_version, '<missing>');
    END IF;

    ALTER TABLE public.schema_metadata
        DROP CONSTRAINT IF EXISTS schema_metadata_schema_version_check;

    UPDATE public.schema_metadata
    SET schema_version = '0.5.1-sql'
    WHERE singleton = TRUE;

    ALTER TABLE public.schema_metadata
        ADD CONSTRAINT schema_metadata_schema_version_check
        CHECK (schema_version = '0.5.1-sql');
END
$schema_version_transition$;

COMMIT;
