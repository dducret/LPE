---
type: Rust Function
title: serialize_event_row_with_attachments
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L115-L130
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row_with_reminder_and_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row
---

# Signature

`pub(in crate::mapi) fn serialize_event_row_with_attachments( event: &AccessibleEvent, item_id: u64, folder_id: u64, has_attachments: bool, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_event_row_with_reminder_and_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row_with_reminder_and_attachments.md)

# Called by

- [serialize_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row.md)