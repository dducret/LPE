---
type: Rust Function
title: default_retry_after_seconds
resource: crates/lpe-storage/src/outbound.rs#L68-L71
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/outbound/normalize_handoff_response
---

# Signature

`fn default_retry_after_seconds(attempts: i32) -> i32`

# Called by

- [normalize_handoff_response](../../../../../functions/crates/lpe-storage/src/outbound/normalize_handoff_response.md)