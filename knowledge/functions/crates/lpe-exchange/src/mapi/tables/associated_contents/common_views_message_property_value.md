---
type: Rust Function
title: common_views_message_property_value
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L418-L437
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_views_property_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns
---

# Signature

`pub(super) fn common_views_message_property_value( message: &MapiCommonViewsMessage, mailbox_guid: Uuid, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [navigation_shortcut_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [search_folder_definition_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)

# Called by

- [serialize_common_views_property_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_views_property_row_with_mailbox_guid.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [sort_common_views_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages.md)
- [captured_common_views_query_rows_flags_heterogeneous_missing_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns.md)