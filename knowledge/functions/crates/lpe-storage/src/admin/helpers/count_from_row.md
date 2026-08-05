---
type: Rust Function
title: count_from_row
resource: crates/lpe-storage/src/admin/helpers.rs#L226-L228
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/fetch_outlook_profile_state
---

# Signature

`pub(super) fn count_from_row(row: &sqlx::postgres::PgRow, column: &str) -> Result<u64>`

# Called by

- [fetch_outlook_profile_state](../../../../../../functions/crates/lpe-storage/src/admin/Storage/fetch_outlook_profile_state.md)