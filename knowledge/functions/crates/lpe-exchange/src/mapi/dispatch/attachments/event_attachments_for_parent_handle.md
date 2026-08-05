---
type: Rust Function
title: event_attachments_for_parent_handle
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L977-L1016
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/apply_event_attachment_overlay_property
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/next_pending_event_attachment_num
---

# Signature

`pub(super) fn event_attachments_for_parent_handle( session: &MapiSession, parent_handle: u32, folder_id: u64, message_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> Vec<crate::mapi_store::MapiAttachment>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_get_valid_attachments_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response.md)
- [append_get_attachment_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response.md)
- [append_open_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response.md)
- [append_delete_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)
- [apply_event_attachment_overlay_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/apply_event_attachment_overlay_property.md)
- [next_pending_event_attachment_num](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/next_pending_event_attachment_num.md)