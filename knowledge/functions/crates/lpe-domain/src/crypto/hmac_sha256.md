---
type: Rust Function
title: hmac_sha256
resource: crates/lpe-domain/src/crypto.rs#L25-L29
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-admin-api/src/totp/generate_code
  - functions/crates/lpe-domain/src/crypto/hmac_sha256_hex
  - functions/crates/lpe-storage/src/storage_backend/s3_signing_key
---

# Signature

`pub fn hmac_sha256(key: &[u8], payload: &[u8]) -> Vec<u8>`

# Calls

- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [generate_code](../../../../../functions/crates/lpe-admin-api/src/totp/generate_code.md)
- [hmac_sha256_hex](../../../../../functions/crates/lpe-domain/src/crypto/hmac_sha256_hex.md)
- [s3_signing_key](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_signing_key.md)