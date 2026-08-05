---
type: Rust Function
title: apply_event_attachment_overlay_property
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1056-L1097
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object
---

# Signature

`pub(super) fn apply_event_attachment_overlay_property( session: &MapiSession, parent_handle: Option<u32>, snapshot: &MapiMailStoreSnapshot, object: &mut MapiObject, )`

# Calls

- [event_attachments_for_parent_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/event_attachments_for_parent_handle.md)

# Called by

- [attachment_overlay_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object.md)