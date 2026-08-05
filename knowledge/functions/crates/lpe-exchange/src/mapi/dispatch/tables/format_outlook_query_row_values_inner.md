---
type: Rust Function
title: format_outlook_query_row_values_inner
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L503-L635
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_for_principal
---

# Signature

`fn format_outlook_query_row_values_inner( account_id: Uuid, principal: Option<&AccountPrincipal>, folder_id: u64, associated: bool, position: usize, forward_read: bool, row_count: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, columns: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [restriction_matches_common_views_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message.md)
- [sort_common_views_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_common_views_messages.md)
- [select_query_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)
- [navigation_shortcut_property_value_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal.md)
- [navigation_shortcut_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value.md)
- [format_debug_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_mapi_value.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [search_folder_definition_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [sort_debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows.md)
- [debug_associated_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_property_value.md)

# Called by

- [format_outlook_query_row_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values.md)
- [format_outlook_query_row_values_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_for_principal.md)