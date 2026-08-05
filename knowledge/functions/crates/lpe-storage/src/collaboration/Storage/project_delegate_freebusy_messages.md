---
type: Rust Method
title: project_delegate_freebusy_messages
resource: crates/lpe-storage/src/collaboration.rs#L177-L191
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages
  called_by:
  - functions/crates/lpe-admin-api/src/delegation/get_free_busy
---

# Signature

`pub async fn project_delegate_freebusy_messages( &self, principal_account_id: Uuid, owner_account_id: Uuid, starts_before: &str, ends_after: &str, ) -> Result<Vec<DelegateFreeBusyMessageObject>>`

# Calls

- [compute_delegate_freebusy_messages](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages.md)

# Called by

- [get_free_busy](../../../../../../functions/crates/lpe-admin-api/src/delegation/get_free_busy.md)