---
type: Rust Function
title: debug_advertised_default_named_view
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L582-L598
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_fai_handoff_visibility_context
---

# Signature

`pub(super) fn debug_advertised_default_named_view( snapshot: &MapiMailStoreSnapshot, folder_id: u64, ) -> Option<crate::mapi_store::MapiCommonViewNamedViewMessage>`

# Calls

- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [collaboration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class.md)
- [advertised_special_folder_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [default_common_views_named_view_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id.md)
- [common_view_named_view_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id.md)
- [debug_default_folder_associated_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)
- [format_outlook_view_descriptor_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context.md)
- [outlook_view_descriptor_visible_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags.md)
- [format_inbox_view_descriptor_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [format_inbox_view_descriptor_set_columns_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract.md)
- [format_default_view_table_compatibility_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_default_view_table_compatibility_contract.md)
- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [format_inbox_fai_handoff_visibility_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_fai_handoff_visibility_context.md)