---
type: Rust Function
title: s3_signing_key
resource: crates/lpe-storage/src/storage_backend.rs#L715-L727
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/crypto/hmac_sha256
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/signed_s3_request
---

# Signature

`fn s3_signing_key( secret_access_key: &str, date_stamp: &str, config: &S3CompatiblePoolConfig, ) -> Result<Vec<u8>>`

# Calls

- [hmac_sha256](../../../../../functions/crates/lpe-domain/src/crypto/hmac_sha256.md)

# Called by

- [signed_s3_request](../../../../../functions/crates/lpe-storage/src/storage_backend/signed_s3_request.md)