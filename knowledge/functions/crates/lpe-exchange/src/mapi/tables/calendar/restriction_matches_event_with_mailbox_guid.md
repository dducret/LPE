---
type: Rust Function
title: restriction_matches_event_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/calendar.rs#L24-L45
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_matches
---

# Signature

`pub(super) fn restriction_matches_event_with_mailbox_guid( restriction: Option<&MapiRestriction>, event: &crate::mapi_store::MapiEvent, mailbox_guid: Uuid, ) -> bool`

# Calls

- [restriction_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [versioned_event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/versioned_event_property_value_with_reminder.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [calendar_content_rows_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows_with_mailbox_guid.md)
- [deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows.md)
- [deleted_items_content_row_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_matches.md)