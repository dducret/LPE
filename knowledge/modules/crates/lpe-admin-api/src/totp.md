---
type: Rust Module
title: totp
resource: crates/lpe-admin-api/src/totp.rs#L1-L144
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/lpe-domain-crypto-hmac-sha256
  - external/uuid-uuid
  - external/super
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [generate_secret](../../../../functions/crates/lpe-admin-api/src/totp/generate_secret.md)
- [unix_time](../../../../functions/crates/lpe-admin-api/src/totp/unix_time.md)
- [verify_code](../../../../functions/crates/lpe-admin-api/src/totp/verify_code.md)
- [otpauth_url](../../../../functions/crates/lpe-admin-api/src/totp/otpauth_url.md)
- [generate_code](../../../../functions/crates/lpe-admin-api/src/totp/generate_code.md)
- [encode_base32](../../../../functions/crates/lpe-admin-api/src/totp/encode_base32.md)
- [decode_base32](../../../../functions/crates/lpe-admin-api/src/totp/decode_base32.md)
- [url_encode](../../../../functions/crates/lpe-admin-api/src/totp/url_encode.md)
- [generated_secret_is_base32](../../../../functions/crates/lpe-admin-api/src/totp/generated_secret_is_base32.md)
- [current_code_verifies](../../../../functions/crates/lpe-admin-api/src/totp/current_code_verifies.md)
- [otp_url_contains_expected_parameters](../../../../functions/crates/lpe-admin-api/src/totp/otp_url_contains_expected_parameters.md)

# Imports

- `lpe_domain::crypto::hmac_sha256`
- `uuid::Uuid`
- `super::*`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)