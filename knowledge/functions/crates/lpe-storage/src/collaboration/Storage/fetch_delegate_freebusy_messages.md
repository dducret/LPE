---
type: Rust Method
title: fetch_delegate_freebusy_messages
resource: crates/lpe-storage/src/collaboration.rs#L193-L205
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages
---

# Signature

`pub async fn fetch_delegate_freebusy_messages( &self, principal_account_id: Uuid, owner_account_id: Option<Uuid>, ) -> Result<Vec<DelegateFreeBusyMessageObject>>`

# Calls

- [compute_delegate_freebusy_messages](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages.md)