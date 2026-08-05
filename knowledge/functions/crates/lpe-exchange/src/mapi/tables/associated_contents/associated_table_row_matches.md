---
type: Rust Function
title: associated_table_row_matches
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L360-L373
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
---

# Signature

`pub(super) fn associated_table_row_matches( message: &AssociatedTableRow, restriction: Option<&MapiRestriction>, mailbox_guid: Uuid, ) -> bool`

# Calls

- [restriction_matches_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config.md)
- [restriction_matches_common_view_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)