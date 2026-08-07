---
type: Rust Function
title: json_text_matches
resource: crates/lpe-storage/src/workspace.rs#L1258-L1269
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-storage/src/workspace/event_update_is_unchanged
---

# Signature

`fn json_text_matches(existing: &str, candidate: &str) -> bool`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [event_update_is_unchanged](../../../../../functions/crates/lpe-storage/src/workspace/event_update_is_unchanged.md)