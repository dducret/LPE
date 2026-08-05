---
type: Rust Function
title: validate_bootstrap_admin_request
resource: crates/lpe-admin-api/src/bootstrap.rs#L112-L125
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/bootstrap/validate_admin_password
  called_by:
  - functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env
  - functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env_or_defaults
  - functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin
---

# Signature

`fn validate_bootstrap_admin_request( email: &str, display_name: &str, password: &str, ) -> anyhow::Result<()>`

# Calls

- [validate_admin_password](../../../../../functions/crates/lpe-admin-api/src/bootstrap/validate_admin_password.md)

# Called by

- [bootstrap_admin_request_from_env](../../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env.md)
- [bootstrap_admin_request_from_env_or_defaults](../../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin_request_from_env_or_defaults.md)
- [bootstrap_admin](../../../../../functions/crates/lpe-admin-api/src/bootstrap/bootstrap_admin.md)