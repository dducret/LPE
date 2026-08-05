---
type: Rust Method
title: common_view_named_view_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1302-L1307
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/common_view_named_view_message_for_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id
---

# Signature

`pub(crate) fn common_view_named_view_message_for_id( &self, _item_id: u64, ) -> Option<MapiCommonViewNamedViewMessage>`

# Called by

- [common_view_named_view_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/common_view_named_view_message_for_open.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [debug_default_folder_associated_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view.md)
- [debug_advertised_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)
- [log_common_view_descriptor_getprops_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)
- [named_view_message_for_folder_and_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id.md)