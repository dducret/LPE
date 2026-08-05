---
type: Rust Function
title: env_secret
resource: crates/lpe-storage/src/storage_backend.rs#L507-L519
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials
---

# Signature

`fn env_secret(prefix: &str, suffix: &str) -> Result<String>`

# Called by

- [resolve_s3_credentials](../../../../../functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials.md)