---
type: Rust Function
title: debug_default_folder_associated_named_view
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L562-L580
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
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_folder_local_default_view_fai_visibility_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
---

# Signature

`pub(super) fn debug_default_folder_associated_named_view( snapshot: &MapiMailStoreSnapshot, folder_id: u64, ) -> Option<crate::mapi_store::MapiCommonViewNamedViewMessage>`

# Calls

- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [collaboration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class.md)
- [advertised_special_folder_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [default_common_views_named_view_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id.md)
- [common_view_named_view_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id.md)
- [default_folder_named_view_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message.md)
- [outlook_default_folder_named_view_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id.md)

# Called by

- [log_outlook_view_handoff](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/log_outlook_view_handoff.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_folder_local_default_view_fai_visibility_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_folder_local_default_view_fai_visibility_contract.md)
- [debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [debug_advertised_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)