---
type: Rust Function
title: client_session_minutes
resource: crates/lpe-admin-api/src/security.rs#L24-L29
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_login
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback
---

# Signature

`pub(crate) fn client_session_minutes() -> u32`

# Called by

- [client_login](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_login.md)
- [client_oidc_callback](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback.md)