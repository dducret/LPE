---
type: Rust Function
title: generate_secret
resource: crates/lpe-admin-api/src/totp.rs#L8-L10
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/totp/encode_base32
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/enroll_totp
  - functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp
  - functions/crates/lpe-admin-api/src/totp/generated_secret_is_base32
---

# Signature

`pub fn generate_secret() -> String`

# Calls

- [encode_base32](../../../../../functions/crates/lpe-admin-api/src/totp/encode_base32.md)

# Called by

- [enroll_totp](../../../../../functions/crates/lpe-admin-api/src/admin_auth/enroll_totp.md)
- [enroll_account_totp](../../../../../functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp.md)
- [generated_secret_is_base32](../../../../../functions/crates/lpe-admin-api/src/totp/generated_secret_is_base32.md)