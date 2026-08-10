---
type: Rust Function
title: deleted_items_content_row_matches
resource: crates/lpe-exchange/src/mapi/tables/deleted_items.rs#L35-L49
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/restriction_matches_event_with_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
---

# Signature

`pub(super) fn deleted_items_content_row_matches( row: &DeletedItemsContentRow<'_>, restriction: Option<&MapiRestriction>, snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> bool`

# Calls

- [restriction_matches_email_in_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/filters/restriction_matches_email_in_snapshot.md)
- [restriction_matches_event_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/restriction_matches_event_with_mailbox_guid.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)