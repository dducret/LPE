UPDATE domains
SET tenant_id = lower(name)
WHERE tenant_id = 'default';

UPDATE accounts
SET tenant_id = split_part(lower(primary_email), '@', 2)
WHERE tenant_id = 'default';

UPDATE mailboxes mb
SET tenant_id = a.tenant_id
FROM accounts a
WHERE mb.account_id = a.id
  AND mb.tenant_id = 'default';

UPDATE mailbox_pst_jobs j
SET tenant_id = mb.tenant_id
FROM mailboxes mb
WHERE j.mailbox_id = mb.id
  AND j.tenant_id = 'default';

UPDATE aliases
SET tenant_id = split_part(lower(source), '@', 2)
WHERE tenant_id = 'default';

UPDATE server_administrators sa
SET tenant_id = COALESCE(d.tenant_id, '__platform__')
FROM domains d
WHERE sa.domain_id = d.id
  AND sa.tenant_id = 'default';

UPDATE server_administrators
SET tenant_id = '__platform__'
WHERE tenant_id = 'default'
  AND domain_id IS NULL;

UPDATE admin_credentials ac
SET tenant_id = COALESCE(
    (
        SELECT sa.tenant_id
        FROM server_administrators sa
        WHERE lower(sa.email) = lower(ac.email)
        ORDER BY sa.created_at ASC
        LIMIT 1
    ),
    '__platform__'
)
WHERE ac.tenant_id = 'default';

UPDATE admin_sessions s
SET tenant_id = ac.tenant_id
FROM admin_credentials ac
WHERE lower(s.admin_email) = lower(ac.email)
  AND s.tenant_id = 'default';

UPDATE security_settings
SET tenant_id = '__platform__'
WHERE tenant_id = 'default';

UPDATE local_ai_settings
SET tenant_id = '__platform__'
WHERE tenant_id = 'default';

UPDATE antispam_settings
SET tenant_id = '__platform__'
WHERE tenant_id = 'default';

UPDATE antispam_filter_rules
SET tenant_id = '__platform__'
WHERE tenant_id = 'default';

UPDATE antispam_quarantine
SET tenant_id = '__platform__'
WHERE tenant_id = 'default';

UPDATE admin_oidc_config
SET tenant_id = '__platform__'
WHERE tenant_id = 'default';

UPDATE admin_oidc_identities i
SET tenant_id = COALESCE(
    (
        SELECT ac.tenant_id
        FROM admin_credentials ac
        WHERE lower(ac.email) = lower(i.admin_email)
        ORDER BY ac.created_at ASC
        LIMIT 1
    ),
    '__platform__'
)
WHERE i.tenant_id = 'default';

UPDATE admin_auth_factors
SET tenant_id = '__platform__'
WHERE tenant_id = 'default';

UPDATE account_credentials ac
SET tenant_id = a.tenant_id
FROM accounts a
WHERE lower(ac.account_email) = lower(a.primary_email)
  AND ac.tenant_id = 'default';

UPDATE account_sessions s
SET tenant_id = ac.tenant_id
FROM account_credentials ac
WHERE lower(s.account_email) = lower(ac.account_email)
  AND s.tenant_id = 'default';

UPDATE contacts c
SET tenant_id = a.tenant_id
FROM accounts a
WHERE c.account_id = a.id
  AND c.tenant_id = 'default';

UPDATE calendar_events e
SET tenant_id = a.tenant_id
FROM accounts a
WHERE e.account_id = a.id
  AND e.tenant_id = 'default';

UPDATE tasks t
SET tenant_id = a.tenant_id
FROM accounts a
WHERE t.account_id = a.id
  AND t.tenant_id = 'default';

UPDATE jmap_upload_blobs b
SET tenant_id = a.tenant_id
FROM accounts a
WHERE b.account_id = a.id
  AND b.tenant_id = 'default';

UPDATE activesync_sync_states s
SET tenant_id = a.tenant_id
FROM accounts a
WHERE s.account_id = a.id
  AND s.tenant_id = 'default';

UPDATE messages m
SET tenant_id = a.tenant_id
FROM accounts a
WHERE m.account_id = a.id
  AND m.tenant_id = 'default';

UPDATE message_recipients r
SET tenant_id = m.tenant_id
FROM messages m
WHERE r.message_id = m.id
  AND r.tenant_id = 'default';

UPDATE message_bcc_recipients r
SET tenant_id = m.tenant_id
FROM messages m
WHERE r.message_id = m.id
  AND r.tenant_id = 'default';

UPDATE outbound_message_queue q
SET tenant_id = m.tenant_id
FROM messages m
WHERE q.message_id = m.id
  AND q.tenant_id = 'default';

UPDATE attachments a
SET tenant_id = m.tenant_id
FROM messages m
WHERE a.message_id = m.id
  AND a.tenant_id = 'default';

UPDATE attachment_blobs
SET tenant_id = lower(domain_name)
WHERE tenant_id = 'default';

UPDATE audit_events
SET tenant_id = '__platform__'
WHERE tenant_id = 'default';

ALTER TABLE admin_sessions DROP CONSTRAINT IF EXISTS admin_sessions_admin_email_fkey;
ALTER TABLE admin_credentials DROP CONSTRAINT IF EXISTS admin_credentials_pkey;
ALTER TABLE admin_credentials ADD PRIMARY KEY (tenant_id, email);
ALTER TABLE admin_sessions
    ADD CONSTRAINT admin_sessions_admin_email_fkey
    FOREIGN KEY (tenant_id, admin_email)
    REFERENCES admin_credentials (tenant_id, email)
    ON DELETE CASCADE;

ALTER TABLE account_sessions DROP CONSTRAINT IF EXISTS account_sessions_account_email_fkey;
ALTER TABLE account_credentials DROP CONSTRAINT IF EXISTS account_credentials_pkey;
ALTER TABLE account_credentials ADD PRIMARY KEY (tenant_id, account_email);
ALTER TABLE account_sessions
    ADD CONSTRAINT account_sessions_account_email_fkey
    FOREIGN KEY (tenant_id, account_email)
    REFERENCES account_credentials (tenant_id, account_email)
    ON DELETE CASCADE;
