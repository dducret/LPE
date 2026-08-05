---
type: Rust Method
title: default_folder_named_view_message
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1328-L1344
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/common_view_named_view_message_for_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id
  - functions/crates/lpe-exchange/src/mapi_store/tests/inbox_default_named_view_is_materialized_for_the_advertised_entry_id
---

# Signature

`pub(crate) fn default_folder_named_view_message( &self, folder_id: u64, item_id: u64, ) -> Option<MapiCommonViewNamedViewMessage>`

# Calls

- [outlook_default_folder_named_view_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id.md)
- [outlook_default_folder_named_view_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_name.md)

# Called by

- [common_view_named_view_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/common_view_named_view_message_for_open.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [folder_local_default_named_view_is_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported.md)
- [debug_default_folder_associated_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view.md)
- [log_common_view_descriptor_getprops_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)
- [default_folder_associated_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/default_folder_associated_named_view.md)
- [named_view_message_for_folder_and_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id.md)
- [inbox_default_named_view_is_materialized_for_the_advertised_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/inbox_default_named_view_is_materialized_for_the_advertised_entry_id.md)