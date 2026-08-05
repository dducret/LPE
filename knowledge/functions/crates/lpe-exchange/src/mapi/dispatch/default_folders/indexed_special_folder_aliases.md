---
type: Rust Function
title: indexed_special_folder_aliases
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L335-L355
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/special_folder_alias
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases
---

# Signature

`fn indexed_special_folder_aliases( value: &MapiValue, expected_folder_ids: &[u64], ) -> Vec<MapiSpecialFolderAlias>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/special_folder_alias.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [default_folder_entry_id_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases.md)