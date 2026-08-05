---
type: Rust Function
title: percent_encode_path
resource: crates/lpe-storage/src/storage_backend.rs#L753-L759
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/s3_object_url
---

# Signature

`fn percent_encode_path(value: &str) -> String`

# Called by

- [s3_object_url](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_url.md)