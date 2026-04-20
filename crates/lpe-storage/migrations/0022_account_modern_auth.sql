ALTER TABLE security_settings
    ADD COLUMN IF NOT EXISTS mailbox_password_login_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS mailbox_oidc_login_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS mailbox_oidc_provider_label TEXT NOT NULL DEFAULT 'Mailbox SSO',
    ADD COLUMN IF NOT EXISTS mailbox_oidc_auto_link_by_email BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS mailbox_app_passwords_enabled BOOLEAN NOT NULL DEFAULT TRUE;

CREATE TABLE IF NOT EXISTS account_oidc_config (
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

CREATE TABLE IF NOT EXISTS account_oidc_identities (
    tenant_id TEXT NOT NULL,
    issuer_url TEXT NOT NULL,
    subject TEXT NOT NULL,
    account_email TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, issuer_url, subject)
);

CREATE INDEX IF NOT EXISTS account_oidc_identities_account_idx
    ON account_oidc_identities (tenant_id, account_email);

CREATE TABLE IF NOT EXISTS account_auth_factors (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    account_email TEXT NOT NULL,
    factor_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    secret_ciphertext TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    verified_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS account_auth_factors_account_idx
    ON account_auth_factors (tenant_id, account_email, factor_type);

CREATE TABLE IF NOT EXISTS account_app_passwords (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    account_email TEXT NOT NULL,
    label TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS account_app_passwords_account_idx
    ON account_app_passwords (tenant_id, account_email, status);
