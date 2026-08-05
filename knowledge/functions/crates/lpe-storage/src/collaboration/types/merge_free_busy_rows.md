---
type: Rust Function
title: merge_free_busy_rows
resource: crates/lpe-storage/src/collaboration/types.rs#L427-L456
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/types/free_busy_status
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_free_busy_blocks
  - functions/crates/lpe-storage/src/collaboration/types/free_busy_rows_merge_adjacent_matching_states
  - functions/crates/lpe-storage/src/collaboration/types/free_busy_without_calendar_access_hides_tentative_detail
  - functions/crates/lpe-storage/src/collaboration/types/free_busy_cancelled_rows_stay_free_without_calendar_access
---

# Signature

`pub(super) fn merge_free_busy_rows( rows: Vec<crate::FreeBusyEventRow>, owner_account_id: Uuid, owner_email: String, can_read_details: bool, ) -> Vec<FreeBusyBlock>`

# Calls

- [free_busy_status](../../../../../../functions/crates/lpe-storage/src/collaboration/types/free_busy_status.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [fetch_free_busy_blocks](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_free_busy_blocks.md)
- [free_busy_rows_merge_adjacent_matching_states](../../../../../../functions/crates/lpe-storage/src/collaboration/types/free_busy_rows_merge_adjacent_matching_states.md)
- [free_busy_without_calendar_access_hides_tentative_detail](../../../../../../functions/crates/lpe-storage/src/collaboration/types/free_busy_without_calendar_access_hides_tentative_detail.md)
- [free_busy_cancelled_rows_stay_free_without_calendar_access](../../../../../../functions/crates/lpe-storage/src/collaboration/types/free_busy_cancelled_rows_stay_free_without_calendar_access.md)