---
type: Rust Function
title: advertised_special_folder_container_class
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1031-L1046
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_folder_local_default_view_fai_visibility_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
---

# Signature

`pub(super) fn advertised_special_folder_container_class(folder_id: u64) -> Option<&'static str>`

# Calls

- [role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)

# Called by

- [format_outlook_view_handoff_table_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_folder_local_default_view_fai_visibility_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_folder_local_default_view_fai_visibility_contract.md)
- [folder_local_default_named_view_is_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_local_default_named_view_is_supported.md)
- [debug_default_folder_associated_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view.md)
- [debug_advertised_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)