---
type: Rust Function
title: has_ascii_boundary_whitespace
resource: crates/lpe-domain/src/mailbox_name.rs#L378-L384
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-domain/src/mailbox_name/validate_raw_segment
---

# Signature

`fn has_ascii_boundary_whitespace(value: &str) -> bool`

# Called by

- [validate_raw_segment](../../../../../functions/crates/lpe-domain/src/mailbox_name/validate_raw_segment.md)