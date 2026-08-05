---
type: Rust Function
title: bootstrap_admin_request_from_env_or_defaults
resource: crates/lpe-admin-api/src/bootstrap.rs#L26-L41
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/bootstrap/validate_bootstrap_admin_request
  called_by:
  - functions/crates/lpe-admin-api/src/app/bootstrap_auto_request_requires_explicit_bootstrap_credentials
  - functions/crates/lpe-cli/src/auto_bootstrap_admin_if_missing
---

# Signature

`pub fn bootstrap_admin_request_from_env_or_defaults() -> anyhow::Result<BootstrapAdminRequest>`

# Calls

- [validate_bootstrap_admin_request](../../../../../functions/crates/lpe-admin-api/src/bootstrap/validate_bootstrap_admin_request.md)

# Called by

- [bootstrap_auto_request_requires_explicit_bootstrap_credentials](../../../../../functions/crates/lpe-admin-api/src/app/bootstrap_auto_request_requires_explicit_bootstrap_credentials.md)
- [auto_bootstrap_admin_if_missing](../../../../../functions/crates/lpe-cli/src/auto_bootstrap_admin_if_missing.md)