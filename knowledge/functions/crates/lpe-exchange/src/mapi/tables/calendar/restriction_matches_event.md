---
type: Rust Function
title: restriction_matches_event
resource: crates/lpe-exchange/src/mapi/tables/calendar.rs#L13-L20
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_matches
---

# Signature

`pub(super) fn restriction_matches_event( restriction: Option<&MapiRestriction>, event: &crate::mapi_store::MapiEvent, ) -> bool`

# Calls

- [restriction_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [versioned_event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [calendar_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows.md)
- [deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows.md)
- [deleted_items_content_row_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_matches.md)