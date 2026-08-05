---
type: Rust Function
title: validate_admin_password
resource: crates/lpe-admin-api/src/bootstrap.rs#L127-L138
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/bootstrap/validate_bootstrap_admin_request
---

# Signature

`fn validate_admin_password(password: &str) -> anyhow::Result<()>`

# Called by

- [validate_bootstrap_admin_request](../../../../../functions/crates/lpe-admin-api/src/bootstrap/validate_bootstrap_admin_request.md)