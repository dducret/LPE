---
type: Rust Function
title: restriction_matches_common_view_named_view
resource: crates/lpe-exchange/src/mapi/properties.rs#L388-L396
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message
---

# Signature

`pub(in crate::mapi) fn restriction_matches_common_view_named_view( restriction: Option<&MapiRestriction>, message: &MapiCommonViewNamedViewMessage, account_id: Uuid, ) -> bool`

# Calls

- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [common_view_named_view_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)

# Called by

- [debug_associated_table_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [rop_find_row_response](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [restriction_matches_common_views_message](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message.md)