---
type: Rust Function
title: s3_timestamp
resource: crates/lpe-storage/src/storage_backend.rs#L774-L785
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/civil_time/utc_from_unix_seconds
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/signed_s3_request
  - functions/crates/lpe-storage/src/storage_backend/s3_signing_timestamp_uses_utc_amz_format
---

# Signature

`fn s3_timestamp(now: SystemTime) -> Result<(String, String)>`

# Calls

- [utc_from_unix_seconds](../../../../../functions/crates/lpe-domain/src/civil_time/utc_from_unix_seconds.md)

# Called by

- [signed_s3_request](../../../../../functions/crates/lpe-storage/src/storage_backend/signed_s3_request.md)
- [s3_signing_timestamp_uses_utc_amz_format](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_signing_timestamp_uses_utc_amz_format.md)