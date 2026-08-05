---
type: Rust Function
title: pending_event_attachment_object
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1018-L1054
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response
---

# Signature

`fn pending_event_attachment_object( folder_id: u64, message_id: u64, upsert: &MapiEventAttachmentUpsert, ) -> MapiObject`

# Called by

- [append_open_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response.md)