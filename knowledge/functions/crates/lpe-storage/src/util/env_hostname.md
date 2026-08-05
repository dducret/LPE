---
type: Rust Function
title: env_hostname
resource: crates/lpe-storage/src/util.rs#L231-L236
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/fetch_server_settings
---

# Signature

`pub(crate) fn env_hostname(name: &str) -> Option<String>`

# Called by

- [fetch_server_settings](../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_server_settings.md)