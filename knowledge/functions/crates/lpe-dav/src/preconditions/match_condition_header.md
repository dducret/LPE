---
type: Rust Function
title: match_condition_header
resource: crates/lpe-dav/src/preconditions.rs#L50-L58
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/preconditions/precondition_not_modified
  - functions/crates/lpe-dav/src/preconditions/check_write_preconditions
  - functions/crates/lpe-dav/src/preconditions/check_delete_preconditions
---

# Signature

`fn match_condition_header(header_value: Option<&HeaderValue>, current_etag: &str) -> bool`

# Called by

- [precondition_not_modified](../../../../../functions/crates/lpe-dav/src/preconditions/precondition_not_modified.md)
- [check_write_preconditions](../../../../../functions/crates/lpe-dav/src/preconditions/check_write_preconditions.md)
- [check_delete_preconditions](../../../../../functions/crates/lpe-dav/src/preconditions/check_delete_preconditions.md)