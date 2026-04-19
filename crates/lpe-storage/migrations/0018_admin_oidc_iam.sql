ALTER TABLE security_settings
    ADD COLUMN IF NOT EXISTS oidc_login_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS oidc_provider_label TEXT NOT NULL DEFAULT 'Corporate SSO',
    ADD COLUMN IF NOT EXISTS oidc_auto_link_by_email BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE server_administrators
    ADD COLUMN IF NOT EXISTS permissions_json TEXT NOT NULL DEFAULT '[]';

ALTER TABLE admin_sessions
    ADD COLUMN IF NOT EXISTS auth_method TEXT NOT NULL DEFAULT 'password';

CREATE TABLE IF NOT EXISTS admin_oidc_config (
    tenant_id TEXT PRIMARY KEY,
    issuer_url TEXT NOT NULL,
    authorization_endpoint TEXT NOT NULL,
    token_endpoint TEXT NOT NULL,
    userinfo_endpoint TEXT NOT NULL,
    client_id TEXT NOT NULL,
    client_secret TEXT NOT NULL,
    scopes TEXT NOT NULL DEFAULT 'openid profile email',
    claim_email TEXT NOT NULL DEFAULT 'email',
    claim_display_name TEXT NOT NULL DEFAULT 'name',
    claim_subject TEXT NOT NULL DEFAULT 'sub',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admin_oidc_identities (
    tenant_id TEXT NOT NULL,
    issuer_url TEXT NOT NULL,
    subject TEXT NOT NULL,
    admin_email TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, issuer_url, subject)
);

CREATE INDEX IF NOT EXISTS admin_oidc_identities_admin_idx
    ON admin_oidc_identities (tenant_id, admin_email);

CREATE TABLE IF NOT EXISTS admin_auth_factors (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    admin_email TEXT NOT NULL,
    factor_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    secret_ciphertext TEXT,
    recovery_codes_hashes_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    verified_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS admin_auth_factors_admin_idx
    ON admin_auth_factors (tenant_id, admin_email, factor_type);
