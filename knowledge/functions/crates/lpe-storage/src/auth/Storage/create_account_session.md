---
type: Rust Method
title: create_account_session
resource: crates/lpe-storage/src/auth.rs#L1040-L1062
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_login
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback
---

# Signature

`pub async fn create_account_session( &self, token: &str, tenant_id: Uuid, account_email: &str, session_timeout_minutes: u32, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [client_login](../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_login.md)
- [client_oidc_callback](../../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback.md)