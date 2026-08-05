---
type: Rust Function
title: normalize_public_base_url
resource: crates/lpe-jmap/src/session.rs#L133-L141
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/normalize_public_base_path
  called_by:
  - functions/crates/lpe-jmap/src/session/JmapService/session_document
---

# Signature

`fn normalize_public_base_url(value: Option<&str>) -> String`

# Calls

- [normalize_public_base_path](../../../../../functions/crates/lpe-jmap/src/session/normalize_public_base_path.md)

# Called by

- [session_document](../../../../../functions/crates/lpe-jmap/src/session/JmapService/session_document.md)