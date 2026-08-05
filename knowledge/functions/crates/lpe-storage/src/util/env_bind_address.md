---
type: Rust Function
title: env_bind_address
resource: crates/lpe-storage/src/util.rs#L238-L244
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/fetch_server_settings
---

# Signature

`pub(crate) fn env_bind_address(name: &str, fallback: &str) -> String`

# Called by

- [fetch_server_settings](../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_server_settings.md)