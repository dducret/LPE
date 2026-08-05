---
type: Rust Function
title: restriction_matches_common_views_message
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L391-L416
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn restriction_matches_common_views_message( restriction: Option<&MapiRestriction>, message: &MapiCommonViewsMessage, mailbox_guid: Uuid, ) -> bool`

# Calls

- [restriction_matches_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_navigation_shortcut.md)
- [restriction_matches_common_view_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view.md)
- [restriction_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [search_folder_definition_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [restriction_matches_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config.md)

# Called by

- [format_common_views_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [restricted_associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)