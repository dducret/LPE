---
type: Rust Function
title: mail_session_headers
resource: crates/lpe-admin-api/src/client_auth.rs#L522-L532
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_login
  - functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback
---

# Signature

`fn mail_session_headers(token: &str) -> HeaderMap`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [client_login](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_login.md)
- [client_oidc_callback](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_oidc_callback.md)