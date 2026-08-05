---
type: Rust Function
title: sign_components
resource: crates/lpe-domain/src/bridge_auth.rs#L165-L183
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-domain/src/crypto/hmac_sha256_hex
  called_by:
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_bytes
---

# Signature

`fn sign_components( shared_secret: &str, method: &str, path: &str, timestamp: &str, nonce: &str, payload: &[u8], ) -> String`

# Calls

- [sha256_hex](../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [hmac_sha256_hex](../../../../../functions/crates/lpe-domain/src/crypto/hmac_sha256_hex.md)

# Called by

- [sign_with_timestamp_and_nonce](../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign_with_timestamp_and_nonce.md)
- [validate_bytes](../../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/validate_bytes.md)