---
type: Rust Function
title: cleared_mail_session_headers
resource: crates/lpe-admin-api/src/client_auth.rs#L542-L553
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-admin-api/src/client_auth/client_logout
---

# Signature

`fn cleared_mail_session_headers(secure: bool) -> HeaderMap`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [client_logout](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_logout.md)