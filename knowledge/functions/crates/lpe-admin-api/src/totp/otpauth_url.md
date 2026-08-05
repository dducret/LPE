---
type: Rust Function
title: otpauth_url
resource: crates/lpe-admin-api/src/totp.rs#L37-L47
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/totp/url_encode
  called_by:
  - functions/crates/lpe-admin-api/src/admin_auth/enroll_totp
  - functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp
  - functions/crates/lpe-admin-api/src/totp/otp_url_contains_expected_parameters
---

# Signature

`pub fn otpauth_url(hostname: &str, email: &str, label: &str, secret: &str) -> String`

# Calls

- [url_encode](../../../../../functions/crates/lpe-admin-api/src/totp/url_encode.md)

# Called by

- [enroll_totp](../../../../../functions/crates/lpe-admin-api/src/admin_auth/enroll_totp.md)
- [enroll_account_totp](../../../../../functions/crates/lpe-admin-api/src/client_auth/enroll_account_totp.md)
- [otp_url_contains_expected_parameters](../../../../../functions/crates/lpe-admin-api/src/totp/otp_url_contains_expected_parameters.md)