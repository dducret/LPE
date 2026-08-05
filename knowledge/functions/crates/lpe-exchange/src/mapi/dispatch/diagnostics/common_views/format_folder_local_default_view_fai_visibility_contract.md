---
type: Rust Function
title: format_folder_local_default_view_fai_visibility_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L620-L647
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_open
---

# Signature

`pub(in crate::mapi::dispatch) fn format_folder_local_default_view_fai_visibility_contract( folder_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> Option<String>`

# Calls

- [debug_default_folder_associated_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view.md)
- [default_common_views_named_view_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id.md)
- [advertised_special_folder_container_class](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class.md)
- [debug_associated_table_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [debug_associated_row_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_id.md)

# Called by

- [log_outlook_contents_table_open](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_open.md)