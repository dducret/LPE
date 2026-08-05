---
type: Rust Function
title: restriction_matches_mailbox_with_context_for_account
resource: crates/lpe-exchange/src/mapi/properties.rs#L180-L194
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches
---

# Signature

`pub(in crate::mapi) fn restriction_matches_mailbox_with_context_for_account( restriction: Option<&MapiRestriction>, mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], mailbox_guid: Uuid, ) -> bool`

# Calls

- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [mailbox_property_value_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)

# Called by

- [hierarchy_rows_excluding_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted.md)
- [hierarchy_row_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches.md)