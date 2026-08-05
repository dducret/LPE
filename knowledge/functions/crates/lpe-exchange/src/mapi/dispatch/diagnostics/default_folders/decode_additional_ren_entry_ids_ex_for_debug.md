---
type: Rust Function
title: decode_additional_ren_entry_ids_ex_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L349-L422
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/additional_ren_entry_ids_ex_expected_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/additional_ren_entry_ids_ex_for_debug
---

# Signature

`fn decode_additional_ren_entry_ids_ex_for_debug(bytes: &[u8]) -> Result<Vec<String>>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [additional_ren_entry_ids_ex_expected_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/additional_ren_entry_ids_ex_expected_folder_id.md)

# Called by

- [additional_ren_entry_ids_ex_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/additional_ren_entry_ids_ex_for_debug.md)