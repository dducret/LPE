---
type: Rust Function
title: canonical_host
resource: crates/lpe-storage/src/storage_backend.rs#L729-L738
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/signed_s3_request
---

# Signature

`fn canonical_host(url: &Url) -> Result<String>`

# Called by

- [signed_s3_request](../../../../../functions/crates/lpe-storage/src/storage_backend/signed_s3_request.md)