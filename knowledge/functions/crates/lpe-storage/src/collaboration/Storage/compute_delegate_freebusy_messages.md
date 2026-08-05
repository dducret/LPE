---
type: Rust Method
title: compute_delegate_freebusy_messages
resource: crates/lpe-storage/src/collaboration.rs#L207-L262
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_delegate_access_objects
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_free_busy_blocks
  - functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_message_objects
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/project_delegate_freebusy_messages
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_delegate_freebusy_messages
---

# Signature

`async fn compute_delegate_freebusy_messages( &self, principal_account_id: Uuid, owner_account_id: Option<Uuid>, starts_before: &str, ends_after: &str, ) -> Result<Vec<DelegateFreeBusyMessageObject>>`

# Calls

- [fetch_delegate_access_objects](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_delegate_access_objects.md)
- [fetch_free_busy_blocks](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_free_busy_blocks.md)
- [delegate_freebusy_message_objects](../../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_message_objects.md)

# Called by

- [project_delegate_freebusy_messages](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/project_delegate_freebusy_messages.md)
- [fetch_delegate_freebusy_messages](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_delegate_freebusy_messages.md)