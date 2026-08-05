---
type: Rust Function
title: bootstrap_admin
resource: crates/lpe-admin-api/src/bootstrap.rs#L43-L93
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/bootstrap/validate_bootstrap_admin_request
  - functions/crates/lpe-storage/src/auth/Storage/has_admin_bootstrap_state
  - functions/crates/lpe-storage/src/auth/Storage/upsert_admin_credential
  called_by:
  - functions/crates/lpe-cli/src/auto_bootstrap_admin_if_missing
  - functions/crates/lpe-cli/src/run_bootstrap_admin_command
---

# Signature

`pub async fn bootstrap_admin( storage: &Storage, request: BootstrapAdminRequest, ) -> anyhow::Result<BootstrapAdminResponse>`

# Calls

- [validate_bootstrap_admin_request](../../../../../functions/crates/lpe-admin-api/src/bootstrap/validate_bootstrap_admin_request.md)
- [has_admin_bootstrap_state](../../../../../functions/crates/lpe-storage/src/auth/Storage/has_admin_bootstrap_state.md)
- [upsert_admin_credential](../../../../../functions/crates/lpe-storage/src/auth/Storage/upsert_admin_credential.md)

# Called by

- [auto_bootstrap_admin_if_missing](../../../../../functions/crates/lpe-cli/src/auto_bootstrap_admin_if_missing.md)
- [run_bootstrap_admin_command](../../../../../functions/crates/lpe-cli/src/run_bootstrap_admin_command.md)