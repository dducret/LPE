---
type: Rust Function
title: serialize_event_row
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L105-L112
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row_with_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row
---

# Signature

`pub(in crate::mapi) fn serialize_event_row( event: &AccessibleEvent, item_id: u64, folder_id: u64, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_event_row_with_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row_with_attachments.md)

# Called by

- [format_calendar_event_query_position_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
- [serialize_pending_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row.md)