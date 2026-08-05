---
type: Rust Function
title: s3_signing_timestamp_uses_utc_amz_format
resource: crates/lpe-storage/src/storage_backend.rs#L941-L945
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/s3_timestamp
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn s3_signing_timestamp_uses_utc_amz_format()`

# Calls

- [s3_timestamp](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_timestamp.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)