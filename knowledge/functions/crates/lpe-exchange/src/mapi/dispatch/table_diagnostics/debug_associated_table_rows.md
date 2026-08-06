---
type: Rust Function
title: debug_associated_table_rows
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L461-L490
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/append_exact_virtual_inbox_debug_associated_config
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_visible_in_table
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_config_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_fai_inventory
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_folder_local_default_view_fai_visibility_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_contract_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_fai_handoff_visibility_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_prefix_find_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_inbox_associated_query_row_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
---

# Signature

`pub(super) fn debug_associated_table_rows( folder_id: u64, snapshot: &MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, mailbox_guid: Uuid, ) -> Vec<DebugAssociatedTableRow>`

# Calls

- [associated_config_messages_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder.md)
- [append_exact_virtual_inbox_debug_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/append_exact_virtual_inbox_debug_associated_config.md)
- [restriction_matches_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_associated_config.md)
- [associated_config_visible_in_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_visible_in_table.md)
- [debug_default_folder_associated_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view.md)
- [restriction_matches_common_view_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [format_inbox_associated_wire_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary.md)
- [format_inbox_associated_config_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_config_summary.md)
- [format_calendar_fai_inventory](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_fai_inventory.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_folder_local_default_view_fai_visibility_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_folder_local_default_view_fai_visibility_contract.md)
- [format_ipm_configuration_contract_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_contract_summary.md)
- [format_inbox_fai_handoff_visibility_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_fai_handoff_visibility_context.md)
- [format_inbox_associated_prefix_find_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_prefix_find_summary.md)
- [format_inbox_associated_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_inbox_associated_query_row_window.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)