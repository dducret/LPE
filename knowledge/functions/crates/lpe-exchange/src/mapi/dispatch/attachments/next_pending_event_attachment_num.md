---
type: Rust Function
title: next_pending_event_attachment_num
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1113-L1150
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
---

# Signature

`fn next_pending_event_attachment_num( session: &MapiSession, parent_handle: u32, folder_id: u64, message_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> u32`

# Calls

- [event_attachments_for_parent_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_create_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)