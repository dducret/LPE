---
type: Rust Function
title: normalize_public_base_path
resource: crates/lpe-jmap/src/session.rs#L120-L131
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/session/public_base_path
  - functions/crates/lpe-jmap/src/session/normalize_public_base_url
---

# Signature

`fn normalize_public_base_path(value: Option<&str>) -> String`

# Called by

- [public_base_path](../../../../../functions/crates/lpe-jmap/src/session/public_base_path.md)
- [normalize_public_base_url](../../../../../functions/crates/lpe-jmap/src/session/normalize_public_base_url.md)