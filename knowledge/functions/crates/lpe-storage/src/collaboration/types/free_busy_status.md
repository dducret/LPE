---
type: Rust Function
title: free_busy_status
resource: crates/lpe-storage/src/collaboration/types.rs#L458-L469
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/collaboration/types/merge_free_busy_rows
---

# Signature

`fn free_busy_status(status: &str, can_read_details: bool) -> String`

# Called by

- [merge_free_busy_rows](../../../../../../functions/crates/lpe-storage/src/collaboration/types/merge_free_busy_rows.md)