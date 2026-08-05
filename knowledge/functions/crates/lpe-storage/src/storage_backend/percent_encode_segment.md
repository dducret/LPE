---
type: Rust Function
title: percent_encode_segment
resource: crates/lpe-storage/src/storage_backend.rs#L761-L772
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-domain/src/crypto/hex_lower
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/s3_bucket_url
---

# Signature

`fn percent_encode_segment(value: &str) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [hex_lower](../../../../../functions/crates/lpe-domain/src/crypto/hex_lower.md)

# Called by

- [s3_bucket_url](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_bucket_url.md)