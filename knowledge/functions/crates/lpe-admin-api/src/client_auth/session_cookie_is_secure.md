---
type: Rust Function
title: session_cookie_is_secure
resource: crates/lpe-admin-api/src/client_auth.rs#L555-L561
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_login
  - functions/crates/lpe-admin-api/src/client_auth/client_logout
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback
---

# Signature

`fn session_cookie_is_secure(headers: &HeaderMap) -> bool`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [client_login](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_login.md)
- [client_logout](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_logout.md)
- [client_oidc_callback](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback.md)