ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS gal_visibility TEXT NOT NULL DEFAULT 'tenant',
    ADD COLUMN IF NOT EXISTS directory_kind TEXT NOT NULL DEFAULT 'person';

ALTER TABLE accounts
    DROP CONSTRAINT IF EXISTS accounts_gal_visibility_chk;

ALTER TABLE accounts
    ADD CONSTRAINT accounts_gal_visibility_chk
    CHECK (gal_visibility IN ('tenant', 'hidden'));

ALTER TABLE accounts
    DROP CONSTRAINT IF EXISTS accounts_directory_kind_chk;

ALTER TABLE accounts
    ADD CONSTRAINT accounts_directory_kind_chk
    CHECK (directory_kind IN ('person', 'room', 'equipment'));

ALTER TABLE domains
    ADD COLUMN IF NOT EXISTS default_sieve_script TEXT NOT NULL DEFAULT '';
