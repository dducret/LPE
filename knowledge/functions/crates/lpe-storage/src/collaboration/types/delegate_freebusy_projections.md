---
type: Rust Function
title: delegate_freebusy_projections
resource: crates/lpe-storage/src/collaboration/types.rs#L482-L534
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/collaboration/types/stable_delegate_freebusy_id
  called_by:
  - functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_message_objects
  - functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projection_does_not_create_empty_placeholder
  - functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projection_uses_only_canonical_delegate_and_blocks
---

# Signature

`fn delegate_freebusy_projections( principal_account_id: Uuid, owner_account_id: Uuid, delegate: Option<&DelegateAccessObject>, free_busy: Vec<FreeBusyBlock>, ) -> Result<Vec<DelegateFreeBusyProjection>>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [stable_delegate_freebusy_id](../../../../../../functions/crates/lpe-storage/src/collaboration/types/stable_delegate_freebusy_id.md)

# Called by

- [delegate_freebusy_message_objects](../../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_message_objects.md)
- [delegate_freebusy_projection_does_not_create_empty_placeholder](../../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projection_does_not_create_empty_placeholder.md)
- [delegate_freebusy_projection_uses_only_canonical_delegate_and_blocks](../../../../../../functions/crates/lpe-storage/src/collaboration/types/delegate_freebusy_projection_uses_only_canonical_delegate_and_blocks.md)