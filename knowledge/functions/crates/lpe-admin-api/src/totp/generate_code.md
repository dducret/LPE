---
type: Rust Function
title: generate_code
resource: crates/lpe-admin-api/src/totp.rs#L49-L61
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/totp/decode_base32
  - functions/crates/lpe-domain/src/crypto/hmac_sha256
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-admin-api/src/totp/verify_code
  - functions/crates/lpe-admin-api/src/totp/current_code_verifies
---

# Signature

`fn generate_code(secret: &str, unix_time: u64) -> Option<String>`

# Calls

- [decode_base32](../../../../../functions/crates/lpe-admin-api/src/totp/decode_base32.md)
- [hmac_sha256](../../../../../functions/crates/lpe-domain/src/crypto/hmac_sha256.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [verify_code](../../../../../functions/crates/lpe-admin-api/src/totp/verify_code.md)
- [current_code_verifies](../../../../../functions/crates/lpe-admin-api/src/totp/current_code_verifies.md)