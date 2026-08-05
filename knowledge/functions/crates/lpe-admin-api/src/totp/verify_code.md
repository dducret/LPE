---
type: Rust Function
title: verify_code
resource: crates/lpe-admin-api/src/totp.rs#L19-L35
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/totp/generate_code
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/login
  - functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor
  - functions/crates/lpe-admin-api/src/client_auth/client_login
  - functions/crates/lpe-admin-api/src/client_auth/verify_account_totp_factor
---

# Signature

`pub fn verify_code(secret: &str, code: &str, unix_time: u64) -> bool`

# Calls

- [generate_code](../../../../../functions/crates/lpe-admin-api/src/totp/generate_code.md)

# Called by

- [login](../../../../../functions/crates/lpe-admin-api/src/admin_auth/login.md)
- [verify_totp_factor](../../../../../functions/crates/lpe-admin-api/src/admin_auth/verify_totp_factor.md)
- [client_login](../../../../../functions/crates/lpe-admin-api/src/client_auth/client_login.md)
- [verify_account_totp_factor](../../../../../functions/crates/lpe-admin-api/src/client_auth/verify_account_totp_factor.md)