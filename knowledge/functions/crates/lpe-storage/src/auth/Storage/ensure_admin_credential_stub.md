---
type: Rust Method
title: ensure_admin_credential_stub
resource: crates/lpe-storage/src/auth.rs#L170-L192
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_callback
---

# Signature

`pub async fn ensure_admin_credential_stub(&self, email: &str) -> Result<()>`

# Calls

- [tenant_id_for_admin_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_admin_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [oidc_callback](../../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)