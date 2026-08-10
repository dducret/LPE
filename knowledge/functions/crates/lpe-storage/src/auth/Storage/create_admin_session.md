---
type: Rust Method
title: create_admin_session
resource: crates/lpe-storage/src/auth.rs#L963-L987
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/util/normalize_admin_session_auth_method
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/login
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_callback
---

# Signature

`pub async fn create_admin_session( &self, token: &str, tenant_id: Uuid, email: &str, session_timeout_minutes: u32, auth_method: &str, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [normalize_admin_session_auth_method](../../../../../../functions/crates/lpe-storage/src/util/normalize_admin_session_auth_method.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [login](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/login.md)
- [oidc_callback](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)