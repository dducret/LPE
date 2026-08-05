---
type: Rust Function
title: admin_session_minutes
resource: crates/lpe-admin-api/src/security.rs#L17-L22
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/login
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_callback
---

# Signature

`pub(crate) fn admin_session_minutes() -> u32`

# Called by

- [login](../../../../../functions/crates/lpe-admin-api/src/admin_auth/login.md)
- [oidc_callback](../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)