---
type: Rust Function
title: bootstrap_admin_request_from_env
resource: crates/lpe-admin-api/src/bootstrap.rs#L9-L24
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/bootstrap/validate_bootstrap_admin_request
  called_by:
  - functions/crates/lpe-admin-api/src/app/bootstrap_request_requires_explicit_strong_password
  - functions/crates/lpe-cli/src/run_bootstrap_admin_command
---

# Signature

`pub fn bootstrap_admin_request_from_env() -> anyhow::Result<BootstrapAdminRequest>`

# Calls

- [validate_bootstrap_admin_request](../../../../../functions/crates/lpe-admin-api/src/bootstrap/validate_bootstrap_admin_request.md)

# Called by

- [bootstrap_request_requires_explicit_strong_password](../../../../../functions/crates/lpe-admin-api/src/app/bootstrap_request_requires_explicit_strong_password.md)
- [run_bootstrap_admin_command](../../../../../functions/crates/lpe-cli/src/run_bootstrap_admin_command.md)