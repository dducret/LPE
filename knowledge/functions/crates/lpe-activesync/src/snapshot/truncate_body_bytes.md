---
type: Rust Function
title: truncate_body_bytes
resource: crates/lpe-activesync/src/snapshot.rs#L178-L186
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/email_body_value
---

# Signature

`fn truncate_body_bytes(bytes: &[u8], truncation_size: Option<usize>) -> (Vec<u8>, bool)`

# Called by

- [email_body_value](../../../../../functions/crates/lpe-activesync/src/snapshot/email_body_value.md)