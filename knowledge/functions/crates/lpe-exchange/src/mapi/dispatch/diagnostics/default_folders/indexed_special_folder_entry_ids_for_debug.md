---
type: Rust Function
title: indexed_special_folder_entry_ids_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L456-L491
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/format_indexed_special_folder_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug
---

# Signature

`fn indexed_special_folder_entry_ids_for_debug( storage_tag: u32, property_name: &'static str, value: &MapiValue, expected_folder_ids: &[u64], ) -> String`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [format_indexed_special_folder_entry_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/format_indexed_special_folder_entry_id.md)

# Called by

- [default_folder_entry_id_values_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_folder_entry_id_values_for_debug.md)