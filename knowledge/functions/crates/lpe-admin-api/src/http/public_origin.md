---
type: Rust Function
title: public_origin
resource: crates/lpe-admin-api/src/http.rs#L21-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/http/forwarded_header
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_start
  - functions/crates/lpe-admin-api/src/admin_auth/oidc_callback
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_start
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback
---

# Signature

`pub(crate) fn public_origin(headers: &HeaderMap) -> String`

# Calls

- [forwarded_header](../../../../../functions/crates/lpe-admin-api/src/http/forwarded_header.md)

# Called by

- [oidc_start](../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_start.md)
- [oidc_callback](../../../../../functions/crates/lpe-admin-api/src/admin_auth/oidc_callback.md)
- [client_oidc_start](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_start.md)
- [client_oidc_callback](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback.md)