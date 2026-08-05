---
type: Rust Function
title: delegate_freebusy_message_objects
resource: crates/lpe-storage/src/collaboration/types.rs#L536-L566
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projections
  - functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projection_updated_at
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages
  - functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_message_objects_use_interval_commit_time_without_store_state
---

# Signature

`pub(super) fn delegate_freebusy_message_objects( principal_account_id: Uuid, owner_account_id: Uuid, delegate: Option<&DelegateAccessObject>, free_busy: Vec<FreeBusyBlock>, ) -> Result<Vec<DelegateFreeBusyMessageObject>>`

# Calls

- [delegate_freebusy_projections](../../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projections.md)
- [delegate_freebusy_projection_updated_at](../../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projection_updated_at.md)

# Called by

- [compute_delegate_freebusy_messages](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages.md)
- [delegate_freebusy_message_objects_use_interval_commit_time_without_store_state](../../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_message_objects_use_interval_commit_time_without_store_state.md)