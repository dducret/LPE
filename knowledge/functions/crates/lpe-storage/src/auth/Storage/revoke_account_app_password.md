---
type: Rust Method
title: revoke_account_app_password
resource: crates/lpe-storage/src/auth.rs#L800-L824
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`pub async fn revoke_account_app_password( &self, account_email: &str, app_password_id: Uuid, ) -> Result<bool>`

# Calls

- [tenant_id_for_account_email](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_email.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)