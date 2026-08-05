---
type: Rust Function
title: hmac_sha256_hex
resource: crates/lpe-domain/src/crypto.rs#L31-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/crypto/hex_lower
  - functions/crates/lpe-domain/src/crypto/hmac_sha256
  called_by:
  - functions/crates/lpe-domain/src/bridge_auth/sign_components
  - functions/crates/lpe-storage/src/storage_backend/signed_s3_request
---

# Signature

`pub fn hmac_sha256_hex(key: &[u8], payload: &[u8]) -> String`

# Calls

- [hex_lower](../../../../../functions/crates/lpe-domain/src/crypto/hex_lower.md)
- [hmac_sha256](../../../../../functions/crates/lpe-domain/src/crypto/hmac_sha256.md)

# Called by

- [sign_components](../../../../../functions/crates/lpe-domain/src/bridge_auth/sign_components.md)
- [signed_s3_request](../../../../../functions/crates/lpe-storage/src/storage_backend/signed_s3_request.md)