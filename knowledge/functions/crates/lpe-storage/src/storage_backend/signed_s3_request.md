---
type: Rust Function
title: signed_s3_request
resource: crates/lpe-storage/src/storage_backend.rs#L590-L664
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/s3_timestamp
  - functions/crates/lpe-storage/src/storage_backend/canonical_host
  - functions/crates/lpe-storage/src/storage_backend/s3_signing_key
  - functions/crates/lpe-domain/src/crypto/hmac_sha256_hex
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  - functions/crates/lpe-storage/src/storage_backend/s3_read_object
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
  - functions/crates/lpe-storage/src/storage_backend/s3_probe_pool
---

# Signature

`fn signed_s3_request( method: Method, url: &Url, config: &S3CompatiblePoolConfig, credentials: &S3Credentials, payload_sha256: &str, extra_headers: BTreeMap<String, String>, now: SystemTime, ) -> Result<HeaderMap>`

# Calls

- [s3_timestamp](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_timestamp.md)
- [canonical_host](../../../../../functions/crates/lpe-storage/src/storage_backend/canonical_host.md)
- [s3_signing_key](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_signing_key.md)
- [hmac_sha256_hex](../../../../../functions/crates/lpe-domain/src/crypto/hmac_sha256_hex.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [s3_put_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [s3_read_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_read_object.md)
- [s3_stat_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)
- [s3_probe_pool](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_probe_pool.md)