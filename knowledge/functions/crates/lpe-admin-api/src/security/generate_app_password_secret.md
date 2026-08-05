---
type: Rust Function
title: generate_app_password_secret
resource: crates/lpe-admin-api/src/security.rs#L9-L15
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/create_account_app_password
---

# Signature

`pub(crate) fn generate_app_password_secret() -> String`

# Called by

- [create_account_app_password](../../../../../functions/crates/lpe-admin-api/src/client_auth/create_account_app_password.md)